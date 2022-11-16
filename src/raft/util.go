package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) realIndex(i int) int {
	return rf.lastIncludedIndex + i
}

func (rf *Raft) logIndex(i int) int {
	return i - rf.lastIncludedIndex
}

func (rf *Raft) getTerm(i int) int {
	if i < rf.lastIncludedIndex {
		log.Fatal("Index less than lastIncludedIndex")
	}
	if i == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.log[rf.logIndex(i)].Term
}
