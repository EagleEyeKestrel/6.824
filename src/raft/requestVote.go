package raft

import (
	"sync"
	"time"
)

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	PrettyDebug(dTest, "S%d, RequestVote acquire lock\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	PrettyDebug(dVote, "S%d in Term %d, receive RequestVote from S%d\n", rf.me, rf.currentTerm, args.CandidateID)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		PrettyDebug(dTest, "S%d, RequestVote release lock\n", rf.me)
		return
	}
	if args.Term > rf.currentTerm {
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	//当前term，如果有多个，那么votedFor可能是别的
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		lastLogTerm := rf.getTerm(rf.realIndex(len(rf.log) - 1))
		PrettyDebug(dVote, "S%d in Term %d, in requestvote handler, args lastlogterm %d, local lastlogterm %d, args lastlogindex %d, local lastlogindex %d\n", rf.me, rf.currentTerm, args.LastLogTerm, lastLogTerm, args.LastLogIndex, rf.realIndex(len(rf.log)-1))
		if args.LastLogTerm > lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.realIndex(len(rf.log)-1) {
			rf.votedFor = args.CandidateID
			rf.lastTime = time.Now()
			rf.persist()
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			PrettyDebug(dVote, "S%d in Term %d, vote for S%d, lastlogterm %d, args last term %d, loglen %d\n", rf.me, rf.currentTerm, args.CandidateID, lastLogTerm, args.LastLogTerm, rf.realIndex(len(rf.log)))
			PrettyDebug(dTest, "S%d, RequestVote release lock\n", rf.me)
			return
		}
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	PrettyDebug(dTest, "S%d, RequestVote release lock\n", rf.me)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	PrettyDebug(dTest, "S%d, startElection acquire lock\n", rf.me)
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastTime = time.Now()
	rf.status = Candidate
	rf.persist()
	term := rf.currentTerm
	candidateID := rf.me
	lastLogIndex := rf.realIndex(len(rf.log) - 1)
	lastLogTerm := rf.getTerm(lastLogIndex)
	PrettyDebug(dElec, "S%d in Term %d, starts election\n", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	PrettyDebug(dTest, "S%d, startElection release lock\n", rf.me)

	getVotes := 1
	getVotesMutex := sync.Mutex{}
	sendRequestVoteInCandidate := func(serverID int) {
		args := RequestVoteArgs{
			Term:         term,
			CandidateID:  candidateID,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply := RequestVoteReply{}
		ok := rf.requestVoteCaller(serverID, &args, &reply)
		if !ok {
			return
		}
		getVotesMutex.Lock()
		getVotes++
		PrettyDebug(dTest, "S%d, startElection acquire lock\n", rf.me)
		rf.mu.Lock()
		PrettyDebug(dElec, "S%d in Term %d, get vote from %d, %d servers total\n", rf.me, rf.currentTerm, serverID, len(rf.peers))
		if getVotes*2 > len(rf.peers) && rf.status == Candidate {
			rf.status = Leader
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.realIndex(len(rf.log))
				rf.matchIndex[i] = 0
			}
			PrettyDebug(dElec, "S%d in Term %d, wins election, %d servers total\n", rf.me, rf.currentTerm, len(rf.peers))
			go rf.leaderHeartbeat()
			go rf.appendChecker()
			go rf.commitChecker()
		}
		rf.mu.Unlock()
		PrettyDebug(dTest, "S%d, startElection release lock\n", rf.me)
		getVotesMutex.Unlock()
	}
	for serverID, _ := range rf.peers {
		if serverID == rf.me {
			continue
		}
		go sendRequestVoteInCandidate(serverID)
	}
}

func (rf *Raft) requestVoteCaller(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.sendRequestVote(server, args, reply)
	PrettyDebug(dTest, "S%d, requestVoteCaller acquire lock\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.currentTerm != args.Term {
		reply.VoteGranted = false
		PrettyDebug(dTest, "S%d, requestVoteCaller release lock\n", rf.me)
		return false
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.persist()
	}
	PrettyDebug(dTest, "S%d, requestVoteCaller release lock\n", rf.me)
	return reply.VoteGranted
}
