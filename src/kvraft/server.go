package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

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
	Key       string
	Value     string
	OpType    string
	LeaderID  int
	CommandID int
	ClientID  int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap    map[string]string
	waitChan map[int]chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{
		Key:       args.Key,
		CommandID: args.CommandID,
		ClientID:  args.ClientID,
		OpType:    "Get",
		LeaderID:  kv.me,
	}
	index, _, ifLeader := kv.rf.Start(command)
	if ifLeader == false {
		DPrintf("Server %d receive get, client %d, commandID %d, but not leader\n", kv.me, args.ClientID, args.CommandID)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Leader Server %d receive Get, index %d, key %s, client %d, commandID %d\n", kv.me, index, args.Key, args.ClientID, args.CommandID)
	waitCh := kv.getWaitCh(index)
	_ = <-waitCh
	DPrintf("Leader Server %d receives Get from waitChan, index %d\n", kv.me, index)
	kv.mu.Lock()
	val, ok := kv.kvMap[args.Key]
	kv.mu.Unlock()
	if ok == false {
		reply.Err = ErrNoKey
		return
	}
	reply.Value = val
	reply.Err = OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	command := Op{
		Key:       args.Key,
		CommandID: args.CommandID,
		ClientID:  args.ClientID,
		OpType:    args.Op,
		Value:     args.Value,
		LeaderID:  kv.me,
	}
	index, _, ifLeader := kv.rf.Start(command)
	if ifLeader == false {
		DPrintf("Server %d receive %s, client %d, commandID %d, but not leader\n", kv.me, args.Op, args.ClientID, args.CommandID)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Leader Server %d receive %s, index %d, key %s, val %s, client %d, commandID %d\n", kv.me, args.Op, index, args.Key, args.Value, args.ClientID, args.CommandID)
	waitCh := kv.getWaitCh(index)
	_ = <-waitCh
	DPrintf("Leader Server %d receives %s command from waitChan, index %d\n", kv.me, args.Op, index)
	kv.mu.Lock()
	_, ok := kv.kvMap[args.Key]
	kv.mu.Unlock()
	if ok == false && args.Op == "Append" {
		reply.Err = ErrNoKey
		return
	}
	reply.Err = OK
}

func (kv *KVServer) applyMsgChecker() {
	for kv.killed() == false {
		msg := <-kv.applyCh
		DPrintf("Server %d find ApplyMsg index %d\n", kv.me, msg.CommandIndex)
		index := msg.CommandIndex
		op := msg.Command.(Op)
		if kv.me == op.LeaderID {
			//only the corresponding leader needs to be notified
			waitCh := kv.getWaitCh(index)
			waitCh <- op
			DPrintf("Server %d finds index %d command finished, back to waitChan\n", kv.me, msg.CommandIndex)
		}
		kv.mu.Lock()
		if op.OpType == "Put" {
			kv.kvMap[op.Key] = op.Value
		}
		if op.OpType == "Append" {
			kv.kvMap[op.Key] += op.Value
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res, exist := kv.waitChan[index]
	if !exist {
		kv.waitChan[index] = make(chan Op)
		res = kv.waitChan[index]
	}
	return res
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.waitChan = make(map[int]chan Op)

	go kv.applyMsgChecker()
	return kv
}
