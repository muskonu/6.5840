package kvraft

import (
	"6.5840/labrpc"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader      int
	clientId    int64
	sequenceNum int64
	mu          sync.Mutex
}

func (ck *Clerk) changeLeader() {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leader++
	if ck.leader >= len(ck.servers) {
		ck.leader = 0
	}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	ck.clientId = nrand()

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	sequenceNum := atomic.LoadInt64(&ck.sequenceNum)
	defer func() {
		DPrintf("Clerk complete Get Op,SeqNum:%d\n", sequenceNum)
	}()
	args := GetArgs{Key: key, ClientId: ck.clientId, SequenceNum: sequenceNum}
	for {
		reply := GetReply{}
		if ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply); ok {
			if reply.Err == OK || reply.Err == ErrNoKey {
				atomic.AddInt64(&ck.sequenceNum, 1)
				return reply.Value
			}
			if reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
				ck.changeLeader()
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		ck.changeLeader()
		time.Sleep(10 * time.Millisecond)
		continue
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	sequenceNum := atomic.LoadInt64(&ck.sequenceNum)
	defer func() {
		DPrintf("Clerk complete %s Op,SeqNum:%d\n", op, sequenceNum)
	}()
	args := PutAppendArgs{Key: key, Value: value, ClientId: ck.clientId, SequenceNum: sequenceNum}
	for {
		reply := PutAppendReply{}
		if ok := ck.servers[ck.leader].Call("KVServer."+op, &args, &reply); ok {
			if reply.Err == OK {
				atomic.AddInt64(&ck.sequenceNum, 1)
				return
			}
			if reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
				ck.changeLeader()
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		ck.changeLeader()
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
