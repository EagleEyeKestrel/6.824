package raft

import "time"

// AppendEntries RPC Handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	PrettyDebug(dTest, "S%d, AppendEntries acquire lock\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	PrettyDebug(dAppend, "S%d in Term %d, receive AppendEntries from S%d, log length %d, prevlogindex %d, lastincluded %d, leader commitindex %d\n", rf.me, rf.currentTerm, args.LeaderID, len(args.Entries), args.PrevLogIndex, rf.lastIncludedIndex, args.LeaderCommit)
	if args.Term < rf.currentTerm {
		reply.Status = FallBehind
		reply.Term = rf.currentTerm
		PrettyDebug(dTest, "S%d, AppendEntries release lock, Fallbehind\n", rf.me)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.persist()
	}
	rf.lastTime = time.Now()
	reply.Term = rf.currentTerm

	if args.PrevLogIndex >= rf.realIndex(len(rf.log)) {
		reply.Status = LogUnmatch
		reply.XTerm = -1
		reply.XLen = rf.realIndex(len(rf.log))
		PrettyDebug(dTest, "S%d, AppendEntries release lock, LogUnmatch\n", rf.me)
		return
	}
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Status = NoReply
		PrettyDebug(dTest, "S%d, AppendEntries release lock, NoReply\n", rf.me)
		return
	}
	PrettyDebug(dAppend, "S%d in Term %d, receive AppendEntries from S%d, local term %d, args prevlogterm %d\n", rf.me, rf.currentTerm, args.LeaderID, rf.getTerm(args.PrevLogIndex), args.PrevLogTerm)
	if rf.getTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Status = LogUnmatch
		reply.Term = rf.currentTerm
		reply.XTerm = rf.log[rf.logIndex(args.PrevLogIndex)].Term
		for i := 1; i < len(rf.log); i++ {
			if rf.log[i].Term == reply.XTerm {
				reply.XIndex = i + rf.lastIncludedIndex
				break
			}
		}
		PrettyDebug(dTest, "S%d, AppendEntries release lock, LogUnmatch\n", rf.me)
		return
	}

	conflict := -1
	for i := args.PrevLogIndex + 1; i < rf.realIndex(len(rf.log)) && i <= args.PrevLogIndex+len(args.Entries); i++ {
		if rf.log[rf.logIndex(i)] != args.Entries[i-args.PrevLogIndex-1] {
			conflict = i
			break
		}
	}
	if conflict != -1 {
		rf.log = rf.log[:rf.logIndex(conflict)]
		for i := conflict - args.PrevLogIndex - 1; i < len(args.Entries); i++ {
			rf.log = append(rf.log, args.Entries[i])
		}
	} else if len(args.Entries) > rf.realIndex(len(rf.log)-1)-args.PrevLogIndex {
		for i := rf.realIndex(len(rf.log)); i-args.PrevLogIndex <= len(args.Entries); i++ {
			rf.log = append(rf.log, args.Entries[i-args.PrevLogIndex-1])
		}
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit <= rf.realIndex(len(rf.log)-1) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.realIndex(len(rf.log) - 1)
		}
		PrettyDebug(dLog, "S%d in Term %d, commit index changes to %d\n", rf.me, rf.currentTerm, rf.commitIndex)
	}
	reply.Term = rf.currentTerm
	reply.Status = Success
	PrettyDebug(dTest, "S%d, AppendEntries release lock, Success\n", rf.me)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// call sendAppendEntries here
// return false if fails or doesn't get response, and reply is false
func (rf *Raft) appendEntriesCaller(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.sendAppendEntries(server, args, reply)
	PrettyDebug(dTest, "S%d, appendEntriesCaller acquire lock\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.currentTerm != args.Term {
		reply.Status = NoReply
		PrettyDebug(dTest, "S%d, appendEntriesCaller release lock\n", rf.me)
		return
	}
	PrettyDebug(dAppend, "S%d in Term %d, AppendEntries to S%d success, reply term is %d\n", rf.me, rf.currentTerm, server, reply.Term)
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.persist()
	}
	PrettyDebug(dTest, "S%d, appendEntriesCaller release lock\n", rf.me)
}
