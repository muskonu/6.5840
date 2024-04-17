package raft

func (rf *Raft) BecomeFollower(term int) {
	DPrintf("%d become follower,From %s Term %d\n", rf.me, stateString(rf.state), rf.currentTerm)
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
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
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.lastIncludedIndex + 1 + len(rf.log)
	}
	rf.resetHeartbeat()

	DPrintf("%d become leader, Term %d\n", rf.me, rf.currentTerm)
}

func stateString(i role) string {
	switch i {
	case 0:
		return "FOLLOWER"
	case 1:
		return "LEADER"
	case 2:
		return "CANDIDATE"
	}
	return ""
}
