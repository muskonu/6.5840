package raft

import (
	"time"
)

func (rf *Raft) followerLoop() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		if rf.state == FOLLOWER {
			select {
			case <-rf.timeoutTicker.C:
				rf.BecomeCandidate()
			default:
			}
		}

		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Intn(50))
		//time.Sleep(time.Duration(ms) * time.Millisecond)
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) candidateLoop() {
	for rf.killed() == false {

		select {
		case <-rf.timeoutTicker.C:
			rf.mu.Lock()
			// 成为candidate
			rf.BecomeCandidate()

			args := &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}

			rf.mu.Unlock()
			// 投票给自己
			var voteCount int64 = 1
			// 发送voteRequest
			for i := 0; i < len(rf.peers); i++ {
				// 排除自身
				if i == rf.me {
					continue
				}

				go func(i int) {
					reply := &RequestVoteReply{}
					DPrintf("%d send requestVote to %d, Term %d\n", rf.me, i, rf.currentTerm)
					ok := rf.sendRequestVote(i, args, reply)
					if ok {
						DPrintf("%d receive requestVote response %d %v, Term %d\n", rf.me, i, reply, rf.currentTerm)
						//投票超过一半，变成领导者
						if reply.VoteGranted {
							rf.mu.Lock()
							voteCount++
							if voteCount >= int64(len(rf.peers)/2+1) {
								voteCount = 0
								rf.BecomeLeader()
							}
							rf.mu.Unlock()
						}
						//发现更高任期，be follower
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.BecomeFollower(reply.Term)
						}
						rf.mu.Unlock()
					}
				}(i)

			}
		default:
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Intn(50))
		//time.Sleep(time.Duration(ms) * time.Millisecond)
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) leaderLoop() {
	for rf.killed() == false {

		rf.mu.Lock()

		if rf.state == LEADER {

			args := &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}

			rf.mu.Unlock()

			for i := 0; i < len(rf.peers); i++ {

				// 排除自身
				if i == rf.me {
					continue
				}

				go func(i int) {
					reply := &AppendEntriesReply{}
					DPrintf("leader%d send heartbeat to %d, Term %d\n", rf.me, i, rf.currentTerm)
					ok := rf.sendAppendEntries(i, args, reply)
					if ok {
						DPrintf("leader%d received heartbeat response from %d, %v\n", rf.me, i, reply)
						//发现更高任期，be follower
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.BecomeFollower(reply.Term)
						}
						rf.mu.Unlock()
					}
				}(i)
			}
			//间隔时间发送心跳
			time.Sleep(90 * time.Millisecond)
		} else {
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Intn(50))
		//time.Sleep(time.Duration(ms) * time.Millisecond)
		time.Sleep(10 * time.Millisecond)
	}
}
