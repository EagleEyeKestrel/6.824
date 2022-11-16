package raft

import (
	log2 "log"
	"time"
)

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	go func() {
		PrettyDebug(dTest, "S%d, Snapshot acquire lock\n", rf.me)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		PrettyDebug(dSnap, "S%d in Term %d, call Snapshot, index %d, lastincludeindex %d, lastapplied %d\n", rf.me, rf.currentTerm, index, rf.lastIncludedIndex, rf.lastApplied)
		if index > rf.lastApplied {
			// Cannot reach here
			log2.Fatalf("Snapshot Index greater than LastApplied\n")
		}

		if index <= rf.lastIncludedIndex {
			PrettyDebug(dTest, "S%d, Snapshot release lock\n", rf.me)
			return
		}

		newLog := make([]LogEntry, 0)
		newLog = append(newLog, LogEntry{Term: 0, Command: 0})
		for i := index + 1; i < rf.realIndex(len(rf.log)); i++ {
			newLog = append(newLog, rf.log[rf.logIndex(i)])
		}
		rf.lastIncludedTerm = rf.log[rf.logIndex(index)].Term
		rf.lastIncludedIndex = index
		rf.log = newLog
		rf.persister.SaveStateAndSnapshot(rf.generatePersistData(), snapshot)
		PrettyDebug(dTest, "S%d, Snapshot release lock, lastinclude index changes to %d\n", rf.me, rf.lastIncludedIndex)
	}()

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) installSnapshotCaller(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.sendInstallSnapshot(server, args, reply)
	PrettyDebug(dTest, "S%d, installSnapshotCaller acquire lock\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.currentTerm != args.Term {
		PrettyDebug(dTest, "S%d, installSnapshotCaller release lock\n", rf.me)
		return false
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.persist()
	}
	PrettyDebug(dTest, "S%d, installSnapshotCaller release lock\n", rf.me)
	return true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	PrettyDebug(dTest, "S%d, InstallSnapshot acquire lock\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	PrettyDebug(dSnap, "S%d in Term %d, receive InstallSnapshot from S%d, lastIncludedIndex %d, to snapshot %d\n", rf.me, rf.currentTerm, args.LeaderID, rf.lastIncludedIndex, args.LastIncludedIndex)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		PrettyDebug(dTest, "S%d, InstallSnapshot release lock, local term bigger\n", rf.me)
		return
	}
	reply.Term = rf.currentTerm
	if rf.lastApplied >= args.LastIncludedIndex {
		PrettyDebug(dTest, "S%d, InstallSnapshot release lock, has been included\n", rf.me)
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.status = Follower
	rf.lastTime = time.Now()

	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Term: 0, Command: 0})
	for i := args.LastIncludedIndex + 1; i < rf.realIndex(len(rf.log)); i++ {
		newLog = append(newLog, rf.log[rf.logIndex(i)])
	}
	rf.log = newLog
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.generatePersistData(), args.Data)

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
		Snapshot:      args.Data,
	}

	rf.applyChan <- applyMsg

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}
	PrettyDebug(dTest, "S%d, InstallSnapshot release lock\n", rf.me)
}
