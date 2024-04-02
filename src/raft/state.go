package raft

import (
	"math"
	"time"
)

func (rf *Raft) BecomeFollower(term int) {
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.resetTicker(500)
	DPrintf("%d become follower,From %s Term %d\n", rf.me, stateString(rf.state), rf.currentTerm)
}

func (rf *Raft) BecomeCandidate() {
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.resetTicker(500)
	rf.votedFor = rf.me
	DPrintf("%d become candidate, Term %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) BecomeLeader() {
	rf.state = LEADER
	rf.resetTicker(math.MaxInt / int(time.Millisecond) / 2)
	DPrintf("%d become leader, Term %d\n", rf.me, rf.currentTerm)
}

func stateString(i role) string {
	switch i {
	case 0:
		return "FOLLOWER"
	case 1:
		return "CANDIDATE"
	case 2:
		return "LEADER"
	}
	return ""
}
