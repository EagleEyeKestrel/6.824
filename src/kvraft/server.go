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

const RPCTIMEOUT = 500

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
	KVMap         map[string]string
	WaitChan      map[int]chan Op      // command index -> wait channel
	LastCommandID map[int64]int        // to a server, clientID -> last commandID
	ReplyMap      map[Pair]interface{} // command -> reply
	Waiting       map[Pair]int         // command -> if server is waiting to reply
	persister     *raft.Persister
	LastApplied   int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("Server %d receive Get, key %s, client %d, commandID %d\n", kv.me, args.Key, args.ClientID, args.CommandID)
	command := Op{
		Key:       args.Key,
		CommandID: args.CommandID,
		ClientID:  args.ClientID,
		OpType:    "Get",
		LeaderID:  kv.me,
	}
	kv.mu.Lock()
	key := Pair{
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	}
	//if args.CommandID <= kv.LastCommandID[args.ClientID] {
	//	lastReply := kv.ReplyMap[key].(GetReply)
	//	reply.Err = lastReply.Err
	//	reply.Value = lastReply.Value
	//	kv.mu.Unlock()
	//	return
	//}
	kv.Waiting[key] = 1
	kv.mu.Unlock()

	index, _, ifLeader := kv.rf.Start(command)
	if ifLeader == false {
		DPrintf("Server %d receive get, client %d, commandID %d, but not leader\n", kv.me, args.ClientID, args.CommandID)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Leader Server %d receive Get, index %d, key %s, client %d, commandID %d\n", kv.me, index, args.Key, args.ClientID, args.CommandID)
	waitCh := kv.getWaitCh(index)
	select {
	case _ = <-waitCh:
		DPrintf("Leader Server %d receives Get from waitChan, command %v, acquire lock\n", kv.me, command)
		key := Pair{
			ClientID:  args.ClientID,
			CommandID: args.CommandID,
		}
		kv.mu.Lock()
		res := kv.ReplyMap[key].(GetReply)
		reply.Err = res.Err
		reply.Value = res.Value
		kv.mu.Unlock()
		DPrintf("Leader Server %d finish command index %d, release lock\n", kv.me, index)
	case <-time.After(RPCTIMEOUT * time.Millisecond):
		DPrintf("Leader Server %d cannot be applied, command %v\n", kv.me, command)
		reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.Waiting, key)
	delete(kv.WaitChan, index)
	kv.mu.Unlock()
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
	DPrintf("Server %d receive %s, key %s, client %d, command %v\n", kv.me, args.Op, args.Key, args.ClientID, command)
	kv.mu.Lock()
	key := Pair{
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	}
	//if args.CommandID <= kv.LastCommandID[args.ClientID] {
	//	lastReply := kv.ReplyMap[key].(PutAppendReply)
	//	reply.Err = lastReply.Err
	//	kv.mu.Unlock()
	//	return
	//}
	kv.Waiting[key] = 1
	kv.mu.Unlock()

	index, _, ifLeader := kv.rf.Start(command)
	if ifLeader == false {
		DPrintf("Server %d receive %s, client %d, commandID %d, but not leader\n", kv.me, args.Op, args.ClientID, args.CommandID)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Leader Server %d receive %s, index %d, key %s, val %s, client %d, commandID %d\n", kv.me, args.Op, index, args.Key, args.Value, args.ClientID, args.CommandID)
	waitCh := kv.getWaitCh(index)
	select {
	case _ = <-waitCh:
		DPrintf("Leader Server %d receives %s from waitChan, command %v, acquire lock\n", kv.me, args.Op, command)
		key := Pair{
			ClientID:  args.ClientID,
			CommandID: args.CommandID,
		}
		kv.mu.Lock()
		res := kv.ReplyMap[key].(PutAppendReply)
		reply.Err = res.Err
		kv.mu.Unlock()
		DPrintf("Leader Server %d finish command index %d, release lock\n", kv.me, index)
	case <-time.After(RPCTIMEOUT * time.Millisecond):
		DPrintf("Leader Server %d cannot be applied, command %v\n", kv.me, command)
		reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.Waiting, key)
	delete(kv.WaitChan, index)
	kv.mu.Unlock()
}

func (kv *KVServer) getWaitCh(index int) chan Op {
	DPrintf("Server %d command index %d getWaitCh, acquire lock\n", kv.me, index)
	kv.mu.Lock()
	res, exist := kv.WaitChan[index]
	if !exist {
		kv.WaitChan[index] = make(chan Op, 1)
		res = kv.WaitChan[index]
	}
	kv.mu.Unlock()
	DPrintf("Server %d command index %d getWaitCh, release lock\n", kv.me, index)
	return res
}

func (kv *KVServer) processOutdatedCommand(op Op) {
	// outdated command, generate replyMap in case it's a request before crashing
	// hold lock before entering
	key := Pair{
		CommandID: op.CommandID,
		ClientID:  op.ClientID,
	}
	if _, ifProcessed := kv.ReplyMap[key]; ifProcessed {
		return
	}
	if op.OpType == "Put" || op.OpType == "Append" {
		reply := PutAppendReply{
			Err: OK,
		}
		kv.ReplyMap[key] = reply
	}
	if op.OpType == "Get" {
		reply := GetReply{}
		val, hasKey := kv.KVMap[op.Key]
		if hasKey {
			reply.Err = OK
			reply.Value = val
		} else {
			reply.Err = ErrNoKey
		}
		kv.ReplyMap[key] = reply
	}

}

func (kv *KVServer) installCommand(op Op) {
	// new command, generate replyMap and modify KVMap
	// hold lock before entering
	kv.LastCommandID[op.ClientID] = op.CommandID
	key := Pair{
		CommandID: op.CommandID,
		ClientID:  op.ClientID,
	}
	if op.OpType == "Put" {
		kv.KVMap[op.Key] = op.Value
		reply := PutAppendReply{
			Err: OK,
		}
		kv.ReplyMap[key] = reply
	}
	if op.OpType == "Append" {
		val, exist := kv.KVMap[op.Key]
		if exist {
			kv.KVMap[op.Key] = val + op.Value
		} else {
			kv.KVMap[op.Key] = op.Value
		}
		reply := PutAppendReply{
			Err: OK,
		}
		kv.ReplyMap[key] = reply
	}
	if op.OpType == "Get" {
		reply := GetReply{
			Err: OK,
		}
		val, exist := kv.KVMap[op.Key]
		if exist {
			reply.Value = val
		} else {
			reply.Err = ErrNoKey
		}
		kv.ReplyMap[key] = reply
	}
	DPrintf("Server %d, install command %v, reply %v", kv.me, op, kv.ReplyMap[key])
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
	//labgob.Register(Pair{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetReply{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.KVMap = make(map[string]string)
	kv.WaitChan = make(map[int]chan Op)
	kv.LastCommandID = make(map[int64]int)
	kv.ReplyMap = make(map[Pair]interface{})
	kv.Waiting = make(map[Pair]int)
	kv.LastApplied = 0
	kv.persister = persister

	kv.readPersist(persister.ReadSnapshot())

	go kv.applyMsgChecker()
	go kv.raftSizeChecker()
	return kv
}
