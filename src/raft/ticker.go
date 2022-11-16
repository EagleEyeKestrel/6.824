package raft

import (
	"math/rand"
	"time"
)

// periodly check if there is new log, if exists, then call
// AppendEntries
func (rf *Raft) appendChecker() {
	for rf.killed() == false {
		PrettyDebug(dTest, "S%d, appendChecker acquire lock\n", rf.me)
		rf.mu.Lock()
		if rf.status != Leader {
			rf.mu.Unlock()
			PrettyDebug(dTest, "S%d, appendChecker release lock\n", rf.me)
			return
		}

		sendAppendEntriesToServer := func(serverID int) {
			PrettyDebug(dTest, "S%d, sendAppendEntriesToServer %d acquire lock\n", rf.me, serverID)
			rf.mu.Lock()
			if rf.status != Leader || rf.nextIndex[serverID] <= rf.lastIncludedIndex {
				PrettyDebug(dTest, "S%d, sendAppendEntriesToServer %d release lock\n", rf.me, serverID)
				rf.mu.Unlock()
				return
			}
			copiedEntries := make([]LogEntry, rf.realIndex(len(rf.log))-rf.nextIndex[serverID])
			for i := 0; i < rf.realIndex(len(rf.log))-rf.nextIndex[serverID]; i++ {
				//PrettyDebug(dAppend, "S%d in Term %d, copiedEntries length %d, use log index %d\n", rf.me, rf.currentTerm, rf.realIndex(len(rf.log))-rf.nextIndex[serverID], rf.logIndex(rf.nextIndex[serverID]+i))
				copiedEntries[i] = rf.log[rf.logIndex(rf.nextIndex[serverID]+i)]
			}
			PrettyDebug(dAppend, "S%d in Term %d, send AppendEntries to S%d to update log, status %v, index %d to %d\n", rf.me, rf.currentTerm, serverID, rf.status, rf.nextIndex[serverID], rf.realIndex(len(rf.log)-1))
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.nextIndex[serverID] - 1,
				PrevLogTerm:  rf.getTerm(rf.nextIndex[serverID] - 1),
				Entries:      copiedEntries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			PrettyDebug(dTest, "S%d, sendAppendEntriesToServer %d release lock\n", rf.me, serverID)
			reply := AppendEntriesReply{}
			rf.appendEntriesCaller(serverID, &args, &reply)
			PrettyDebug(dTest, "S%d, sendAppendEntriesToServer %d acquire lock\n", rf.me, serverID)
			rf.mu.Lock()
			if reply.Status == LogUnmatch {
				if reply.XTerm == -1 {
					rf.nextIndex[serverID] = reply.XLen
				} else {
					followerTermLastIndex := -1
					for i := len(rf.log) - 1; i >= 0; i-- {
						if rf.log[i].Term == reply.XTerm {
							followerTermLastIndex = rf.realIndex(i)
							break
						}
					}
					if followerTermLastIndex == -1 {
						rf.nextIndex[serverID] = reply.XIndex
					} else {
						rf.nextIndex[serverID] = followerTermLastIndex + 1
					}
				}
			} else if reply.Status == Success {
				oldnxt := rf.nextIndex[serverID]
				if args.PrevLogIndex+len(args.Entries) > rf.matchIndex[serverID] {
					rf.matchIndex[serverID] = args.PrevLogIndex + len(args.Entries)
				}
				if args.PrevLogIndex+len(args.Entries)+1 > rf.nextIndex[serverID] {
					rf.nextIndex[serverID] = args.PrevLogIndex + len(args.Entries) + 1
				}
				PrettyDebug(dAppend, "S%d in Term %d, send AppendEntries to S%d success, nextIndex changes from %d to %d\n", rf.me, rf.currentTerm, serverID, oldnxt, rf.nextIndex[serverID])
			}
			rf.mu.Unlock()
			PrettyDebug(dTest, "S%d, sendAppendEntriesToServer %d release lock\n", rf.me, serverID)
		}

		sendInstallSnapshotToServer := func(serverID int) {
			PrettyDebug(dTest, "S%d, sendInstallSnapshotToServer %d acquire lock\n", rf.me, serverID)
			rf.mu.Lock()
			if rf.status != Leader {
				rf.mu.Unlock()
				PrettyDebug(dTest, "S%d, sendInstallSnapshotToServer %d release lock\n", rf.me, serverID)
				return
			}
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderID:          rf.me,
				LastIncludedTerm:  rf.lastIncludedTerm,
				LastIncludedIndex: rf.lastIncludedIndex,
				Data:              rf.persister.ReadSnapshot(),
			}
			reply := InstallSnapshotReply{}
			PrettyDebug(dSnap, "S%d in Term %d, send Installsnapshot to S%d, lastincludedindex %d\n", rf.me, rf.currentTerm, serverID, args.LastIncludedIndex)
			rf.mu.Unlock()
			PrettyDebug(dTest, "S%d, sendInstallSnapshotToServer %d release lock\n", rf.me, serverID)
			ok := rf.installSnapshotCaller(serverID, &args, &reply)
			PrettyDebug(dTest, "S%d, sendInstallSnapshotToServer %d acquire lock\n", rf.me, serverID)
			rf.mu.Lock()
			if ok && rf.lastIncludedIndex+1 > rf.nextIndex[serverID] {
				rf.nextIndex[serverID] = rf.lastIncludedIndex + 1
			}
			rf.mu.Unlock()
			PrettyDebug(dTest, "S%d, sendInstallSnapshotToServer %d release lock\n", rf.me, serverID)
		}

		for serverID := 0; serverID < len(rf.peers); serverID++ {
			PrettyDebug(dAppend, "S%d in Term %d, check server %d, nextIndex %d, log len %d\n", rf.me, rf.currentTerm, serverID, rf.nextIndex[serverID], rf.realIndex(len(rf.log)))
			if serverID == rf.me || rf.nextIndex[serverID] >= rf.realIndex(len(rf.log)) {
				continue
			}
			if rf.nextIndex[serverID] <= rf.lastIncludedIndex {
				go sendInstallSnapshotToServer(serverID)
			} else {
				go sendAppendEntriesToServer(serverID)
			}
		}
		rf.mu.Unlock()
		PrettyDebug(dTest, "S%d, appendChecker release lock\n", rf.me)
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) leaderHeartbeat() {
	for rf.killed() == false {
		PrettyDebug(dTest, "S%d, leaderHeartbeat acquire lock\n", rf.me)
		rf.mu.Lock()
		if rf.status != Leader {
			rf.mu.Unlock()
			PrettyDebug(dTest, "S%d, leaderHeartbeat release lock\n", rf.me)
			return
		}
		for serverID, _ := range rf.peers {
			if serverID == rf.me {
				continue
			}
			PrettyDebug(dTimer, "S%d in Term %d, send heartbeat to S%d, status %v, log length %d, previndex %d, prevterm %d\n", rf.me, rf.currentTerm, serverID, rf.status, len(rf.log), rf.realIndex(len(rf.log)-1), rf.getTerm(rf.realIndex(len(rf.log)-1)))
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.realIndex(len(rf.log) - 1),
				PrevLogTerm:  rf.getTerm(rf.realIndex(len(rf.log) - 1)),
				Entries:      make([]LogEntry, 0),
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			go rf.appendEntriesCaller(serverID, &args, &reply)
		}
		rf.mu.Unlock()
		PrettyDebug(dTest, "S%d, leaderHeartbeat release lock\n", rf.me)
		time.Sleep(120 * time.Millisecond)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		PrettyDebug(dTest, "S%d, ticker acquire lock\n", rf.me)
		rf.mu.Lock()
		if rf.status != Leader && time.Since(rf.lastTime) > time.Duration(300+rand.Intn(150))*time.Millisecond {
			go rf.startElection()
		}
		rf.mu.Unlock()
		PrettyDebug(dTest, "S%d, ticker release lock\n", rf.me)
		time.Sleep(10 * time.Millisecond)
	}
}

// leader checks if there exists a log that can be commited
func (rf *Raft) commitChecker() {
	for rf.killed() == false {
		PrettyDebug(dTest, "S%d, commitChecker acquire lock\n", rf.me)
		rf.mu.Lock()
		if rf.status != Leader {
			rf.mu.Unlock()
			PrettyDebug(dTest, "S%d, commitChecker release lock\n", rf.me)
			return
		}
		if rf.commitIndex < rf.realIndex(len(rf.log)-1) {
			for i := rf.commitIndex + 1; i < rf.realIndex(len(rf.log)); i++ {
				copied := 1
				for j := 0; j < len(rf.peers); j++ {
					if j != rf.me && rf.matchIndex[j] >= i {
						copied++
					}
				}
				if copied*2 > len(rf.peers) {
					rf.commitIndex++
					PrettyDebug(dLog, "S%d in Term %d, commit index changes to %d\n", rf.me, rf.currentTerm, rf.commitIndex)
				} else {
					break
				}
			}
		}
		rf.mu.Unlock()
		PrettyDebug(dTest, "S%d, commitChecker release lock\n", rf.me)
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) applyChecker() {
	for rf.killed() == false {
		PrettyDebug(dTest, "S%d, applyChecker acquire lock\n", rf.me)
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				if i <= rf.lastIncludedIndex {
					continue
				}
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.logIndex(i)].Command,
					CommandIndex: i,
				}
				rf.lastApplied = i
				rf.applyChan <- msg
				PrettyDebug(dLog, "S%d in Term %d, apply index changes to %d, this command index %d\n", rf.me, rf.currentTerm, rf.lastApplied, i)
			}
		}
		rf.mu.Unlock()
		PrettyDebug(dTest, "S%d, applyChecker release lock\n", rf.me)
		time.Sleep(10 * time.Millisecond)
	}
}
