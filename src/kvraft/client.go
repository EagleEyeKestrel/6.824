package kvraft

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID   int
	commandNum int
	clientID   int64
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
	ck.leaderID = 0
	ck.commandNum = 0
	ck.clientID = nrand()
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.commandNum++
	for true {
		receiver := ck.leaderID
		args := GetArgs{
			Key:       key,
			CommandID: ck.commandNum,
			ClientID:  ck.clientID,
		}
		reply := GetReply{}
		DPrintf("Client %d send Server %d Get, commandID %d, key %s\n", ck.clientID, receiver, ck.commandNum, key)
		ok := ck.sendGet(receiver, &args, &reply)
		if !ok {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == ErrWrongLeader {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrNoKey {
			return ""
		}
		return reply.Value
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.commandNum++
	for true {
		receiver := ck.leaderID
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			CommandID: ck.commandNum,
			ClientID:  ck.clientID,
		}
		reply := PutAppendReply{}
		DPrintf("Client %d send Server %d %s, commandID %d, key %s, value %s\n", ck.clientID, receiver, op, ck.commandNum, key, value)
		ok := ck.sendPutAppend(receiver, &args, &reply)
		if !ok {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == ErrWrongLeader {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) sendGet(serverID int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[serverID].Call("KVServer.Get", args, reply)
	return ok
}

func (ck *Clerk) sendPutAppend(serverID int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[serverID].Call("KVServer.PutAppend", args, reply)
	return ok
}
