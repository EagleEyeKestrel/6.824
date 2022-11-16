package kvraft

import "time"

func (kv *KVServer) raftSizeChecker() {
	for kv.killed() == false {
		kv.mu.Lock()
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
			snapshot := kv.generatePersistData()
			DPrintf("Server %d install snapshot, snapshotindex %d, %v\n", kv.me, kv.LastApplied, snapshot)
			kv.rf.Snapshot(kv.LastApplied, snapshot)
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *KVServer) applyMsgChecker() {
	for kv.killed() == false {
		msg := <-kv.applyCh
		//DPrintf("Server %d find ApplyMsg index %d, command %v\n", kv.me, msg.CommandIndex, msg.Command)
		if msg.CommandValid {
			index := msg.CommandIndex
			op := msg.Command.(Op)
			DPrintf("Server %d receive CommandApplyMsg %d, acquire lock in applyMsgChecker, command %v\n", kv.me, index, op)
			kv.mu.Lock()
			if op.CommandID > kv.LastCommandID[op.ClientID] {
				kv.installCommand(op)
			} else {
				kv.processOutdatedCommand(op)
			}
			key := Pair{
				ClientID:  op.ClientID,
				CommandID: op.CommandID,
			}
			_, ifWaiting := kv.Waiting[key]
			kv.mu.Unlock()
			DPrintf("Server %d receive CommandApplyMsg %d, release lock in applyMsgChecker\n", kv.me, index)
			if kv.me == op.LeaderID && ifWaiting {
				//only the corresponding leader needs to be notified
				waitCh := kv.getWaitCh(index)
				DPrintf("Server %d waits op %v back to waitChan\n", kv.me, op)
				waitCh <- op
				DPrintf("Server %d finds index %d command finished, back to waitChan\n", kv.me, msg.CommandIndex)
			}
			kv.mu.Lock()
			if index > kv.LastApplied {
				kv.LastApplied = index
			}
			kv.mu.Unlock()
		}
		if msg.SnapshotValid {
			DPrintf("Server %d receive Snapshot %d, %v\n", kv.me, msg.SnapshotIndex, msg.Snapshot)
			kv.mu.Lock()
			kv.readPersist(msg.Snapshot)
			kv.mu.Unlock()
		}

	}
}
