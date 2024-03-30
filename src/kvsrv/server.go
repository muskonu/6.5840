package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	rw sync.RWMutex

	// Your definitions here.
	store map[string]string
	once  map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.rw.RLock()
	defer kv.rw.RUnlock()
	reply.Value = kv.store[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.rw.Lock()
	defer kv.rw.Unlock()
	kv.store[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.rw.Lock()
	defer kv.rw.Unlock()
	if v, ok := kv.once[args.Nonce]; ok {
		reply.Value = v
		return
	}
	reply.Value = kv.store[args.Key]
	kv.store[args.Key] = reply.Value + args.Value
	kv.once[args.Nonce] = reply.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.once = make(map[int64]string)

	return kv
}

func (kv *KVServer) Received(args *ReceivedArgs, reply *ReceivedReply) {
	kv.rw.Lock()
	defer kv.rw.Unlock()
	delete(kv.once, args.Nonce)
}
