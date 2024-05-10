package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

type Operator string

const (
	GetOp    Operator = "Get"
	PutOp             = "Put"
	AppendOp          = "Append"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operator    Operator
	Key         string
	Value       string
	ClientId    int64
	SequenceNum int64
}

type Entry struct {
	Key   string
	Value string
}

type Notice struct {
	Err         Err
	Value       string
	ClientId    int64
	SequenceNum int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store     map[string]string
	completed map[int64]int64
	listeners map[int]chan Notice
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	index, _, isLeader := kv.rf.Start(Op{
		Operator:    GetOp,
		Key:         args.Key,
		Value:       "",
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("KVServer%d Get,args:%+v\n", kv.me, args)
	listener := make(chan Notice)
	kv.mu.Lock()
	kv.listeners[index] = listener
	kv.mu.Unlock()
	select {
	case notice := <-listener:
		if notice.ClientId != args.ClientId || notice.SequenceNum != args.SequenceNum {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = notice.Err
		reply.Value = notice.Value
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	kv.mu.Lock()
	delete(kv.listeners, index)
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	index, _, isLeader := kv.rf.Start(Op{
		Operator:    PutOp,
		Key:         args.Key,
		Value:       args.Value,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("KVServer%d Put,args:%+v\n", kv.me, args)
	listener := make(chan Notice)
	kv.mu.Lock()
	kv.listeners[index] = listener
	kv.mu.Unlock()
	select {
	case notice := <-listener:
		if notice.ClientId != args.ClientId || notice.SequenceNum != args.SequenceNum {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = notice.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	kv.mu.Lock()
	delete(kv.listeners, index)
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	index, _, isLeader := kv.rf.Start(Op{
		Operator:    AppendOp,
		Key:         args.Key,
		Value:       args.Value,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("KVServer%d Append,%+v\n", kv.me, args)
	listener := make(chan Notice)
	kv.mu.Lock()
	kv.listeners[index] = listener
	kv.mu.Unlock()
	select {
	case notice := <-listener:
		if notice.ClientId != args.ClientId || notice.SequenceNum != args.SequenceNum {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = notice.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	kv.mu.Lock()
	delete(kv.listeners, index)
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.store = map[string]string{}
	kv.listeners = map[int]chan Notice{}
	kv.completed = map[int64]int64{}

	// kv.applyCh的容量不能为0，两边select都有default分支，日志apply很难对上
	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	go kv.applier()

	return kv
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		kv.mu.Lock()
		size, index := kv.rf.RaftState()
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.SnapshotValid {
				kv.decodeSnapshot(applyMsg.Snapshot)
				break
			}

			op := applyMsg.Command.(Op)

			v, ok := kv.completed[op.ClientId]
			if !ok {
				kv.completed[op.ClientId] = -1
				v = -1
			}
			if op.SequenceNum <= v && op.Operator != GetOp {
				if ch, ok := kv.listeners[applyMsg.CommandIndex]; ok {
					DPrintf("KVServer%d give up old log %d,Op:%+v\n", kv.me, applyMsg.CommandIndex, op)
					ch <- Notice{Err: OK, ClientId: op.ClientId, SequenceNum: op.SequenceNum}
				}
				break
			}

			DPrintf("KVServer%d apply log %d,Op:%+v\n", kv.me, applyMsg.CommandIndex, op)

			kv.completed[op.ClientId] = max(op.SequenceNum, v)

			notice := kv.executeLog(op)

			if kv.maxraftstate != -1 && size >= kv.maxraftstate {
				DPrintf("KVServer%d compact log from index:%d,size:%d,maxraftstate:%d\n", kv.me, index, size, kv.maxraftstate)
				entries := kv.encodeSnapshot()
				kv.rf.Snapshot(applyMsg.CommandIndex, entries)
			}

			if ch, ok := kv.listeners[applyMsg.CommandIndex]; ok {
				ch <- notice
			}
		default:
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *KVServer) executeLog(op Op) (notice Notice) {
	notice.ClientId = op.ClientId
	notice.SequenceNum = op.SequenceNum
	switch op.Operator {
	case GetOp:
		v, ok := kv.store[op.Key]
		if ok {
			notice.Err = OK
			notice.Value = v
		} else {
			notice.Err = ErrNoKey
		}
	case PutOp:
		notice.Err = OK
		kv.store[op.Key] = op.Value
	case AppendOp:
		notice.Err = OK
		kv.store[op.Key] = kv.store[op.Key] + op.Value
	}
	return
}

func (kv *KVServer) encodeSnapshot() []byte {
	es := []Entry{}
	for k, v := range kv.store {
		es = append(es, Entry{
			Key:   k,
			Value: v,
		})
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(es)
	for i, v := range kv.completed {
		e.Encode(i)
		e.Encode(v)
	}
	return w.Bytes()
}

func (kv *KVServer) decodeSnapshot(entries []byte) {
	var es []Entry
	r := bytes.NewBuffer(entries)
	d := labgob.NewDecoder(r)
	d.Decode(&es)
	DPrintf("KVServer%d recover log entries count:%d\n", kv.me, len(es))
	for _, entry := range es {
		kv.store[entry.Key] = entry.Value
	}
	for true {
		var clientId int64
		var completed int64
		err := d.Decode(&clientId)
		if err != nil {
			break
		}
		d.Decode(&completed)
		kv.completed[clientId] = completed
	}
	return
}
