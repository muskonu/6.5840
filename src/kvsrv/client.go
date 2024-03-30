package kvsrv

import (
	"6.5840/labrpc"
	"math"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	receiveNonce int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.receiveNonce = math.MaxInt
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}
	reply := GetReply{}
	for {
		if ok := ck.server.Call("KVServer.Get", &args, &reply); ok {
			break
		}
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Nonce: nrand()}
	reply := PutAppendReply{}
	for {
		if ok := ck.server.Call("KVServer."+op, &args, &reply); ok {
			break
		}
	}

	// 删除
	if op == "Append" {
		ck.receiveNonce = args.Nonce
	}

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	res := ck.PutAppend(key, value, "Append")
	for {
		if ok := ck.server.Call("KVServer.Received", &ReceivedArgs{Nonce: ck.receiveNonce}, &ReceivedReply{}); ok {
			break
		}
	}
	return res
}
