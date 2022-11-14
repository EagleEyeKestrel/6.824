package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type Status int

const (
	Follower Status = iota
	Candidate
	Leader
)

type AppendEntriesReplyStatus int

const (
	NoReply    AppendEntriesReplyStatus = iota // call failure
	FallBehind                                 // wrong leader, need to upd
	LogUnmatch                                 // log not match, causing append failure
	Success                                    // append success
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int // candidate who receives his vote
	log         []LogEntry
	// volatile state on servers
	commitIndex int // index of the highest log entry committed
	lastApplied int // index of the highest log entry applied to state machine
	// volatile state on leaders
	nextIndex  []int // for each server, index of next log entry to send
	matchIndex []int // for each server, index of the highest log entry matched

	lastTime  time.Time // last time resetting timer
	status    Status
	applyChan chan ApplyMsg
	//chanMu    sync.Mutex

	lastIncludedIndex int // last index written into snapshot
	lastIncludedTerm  int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term   int
	Status AppendEntriesReplyStatus
	XTerm  int
	XIndex int
	XLen   int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	PrettyDebug(dTest, "S%d, GetState acquire lock\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	PrettyDebug(dTest, "S%d, GetState release lock\n", rf.me)
	return rf.currentTerm, rf.status == Leader
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	PrettyDebug(dTest, "S%d, Start acquire lock\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.status != Leader {
		PrettyDebug(dTest, "S%d, Start release lock\n", rf.me)
		return 0, 0, false
	}
	index := rf.realIndex(len(rf.log))
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.persist()
	PrettyDebug(dLog, "S%d in Term %d, index %d add command %v\n", rf.me, rf.currentTerm, index, command)
	PrettyDebug(dTest, "S%d, Start release lock\n", rf.me)
	return index, term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	atomic.StoreInt32(&rf.dead, 0)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0, Command: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastTime = time.Now()
	rf.status = Follower
	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastIncludedIndex > 0 {
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyChecker()
	//go rf.debuger()

	return rf
}

func (rf *Raft) debuger() {
	for rf.killed() == false {
		PrettyDebug(dTest, "S%d, debuger acquire lock\n", rf.me)
		rf.mu.Lock()
		list := "[ "
		for i := 0; i < len(rf.log); i++ {
			list = list + fmt.Sprintf("%v", rf.log[i].Command) + "-" + fmt.Sprintf("%v", rf.log[i].Term) + " "
		}
		list += "]"
		PrettyDebug(dTrace, "S%d in Term %d, lastincludedindex: %d, log: %s\n", rf.me, rf.currentTerm, rf.lastIncludedIndex, list)
		rf.mu.Unlock()
		PrettyDebug(dTest, "S%d, debuger release lock\n", rf.me)
		time.Sleep(100 * time.Millisecond)
	}
}
