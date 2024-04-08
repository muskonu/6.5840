package raft

import (
	"time"
)

func (rf *Raft) tickerLoop() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		if rf.state == FOLLOWER || rf.state == CANDIDATE {
			select {
			case <-rf.timeoutTicker.C:
				rf.BecomeCandidate()

				args := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.log[len(rf.log)-1].Index,
					LastLogTerm:  rf.log[len(rf.log)-1].Term,
				}

				// 投票给自己
				var voteCount int64 = 1
				// 发送voteRequest
				for i := 0; i < len(rf.peers); i++ {
					// 排除自身
					if i == rf.me {
						continue
					}

					DPrintf("%d send requestVote to %d, Term %d\n", rf.me, i, rf.currentTerm)

					go func(i int) {
						reply := &RequestVoteReply{}
						ok := rf.sendRequestVote(i, args, reply)
						if ok {
							rf.mu.Lock()
							if args.Term != rf.currentTerm {
								rf.mu.Unlock()
								return
							}
							DPrintf("%d receive requestVote response %d %v, Term %d\n", rf.me, i, reply, rf.currentTerm)
							//投票超过一半，变成领导者
							if reply.VoteGranted {
								voteCount++
								if voteCount >= int64(len(rf.peers)/2+1) {
									voteCount = 0
									rf.BecomeLeader()
								}
							}
							//发现更高任期，be follower
							if reply.Term > rf.currentTerm {
								rf.BecomeFollower(reply.Term)
							}
							rf.mu.Unlock()
						}
					}(i)

				}
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

func (rf *Raft) leaderLoop() {
	for rf.killed() == false {

		rf.mu.Lock()

		if rf.state == LEADER {

			select {
			case <-rf.timeoutTicker.C:
				// 发送心跳
				for i := 0; i < len(rf.peers); i++ {
					// 排除自身
					if rf.me == i {
						continue
					}
					rf.sendAppendEntries(i)
				}
				rf.resetHeartbeat()
			default:
			}
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Intn(50))
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntries(server int) {

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.log[rf.nextIndex[server]-1].Index,
		PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
		Entries:      rf.log[rf.nextIndex[server]:],
		LeaderCommit: rf.commitIndex,
	}

	DPrintf("leader%d send entry to %d, Term %d, prevLogIndex:%d , prevLogIndex:%d ,Entries len:%d\n", rf.me,
		server, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))

	go func() {
		reply := &AppendEntriesReply{}
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			rf.mu.Lock()
			//收到了旧任期的回复，直接返回
			if args.Term != rf.currentTerm {
				rf.mu.Unlock()
				return
			}
			// 发现更高任期，be follower
			if reply.Term > rf.currentTerm {
				rf.BecomeFollower(reply.Term)
				rf.resetTicker(500)
				rf.mu.Unlock()
				return
			}
			if reply.Success {
				// 复制成功，更新nextIndex和matchIndex
				rf.nextIndex[server] = max(rf.nextIndex[server], args.PrevLogIndex+len(args.Entries)+1) //max 防止第二条日志的回复比第一条日志的回复先返回
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				DPrintf("leader%d received entry response from %d, true,nextIndex:%d\n", rf.me, server, rf.nextIndex[server])
				count := 1
				newMatchIndex := rf.matchIndex[server]
				// 查看是否需要更新leader commitIndex
				if newMatchIndex > rf.commitIndex && rf.log[newMatchIndex].Term == rf.currentTerm {
					for i := 0; i < len(rf.peers); i++ {
						if rf.matchIndex[i] >= newMatchIndex {
							count++
							if count >= len(rf.peers)/2+1 {
								rf.commitIndex = newMatchIndex
								break
							}
						}
					}
				}
			} else {
				DPrintf("leader%d received entry response from %d, false : %+v\n", rf.me, server, reply)
				// 复制失败，减少nextIndex并重试(快速恢复)
				if reply.XTerm != -1 { // server nextIndex 有日志
					if rf.log[reply.XIndex].Term != reply.Term {
						rf.nextIndex[server] = reply.XIndex
					} else {
						for rf.log[reply.XIndex].Term == reply.Term {
							reply.XIndex++
						}
						rf.nextIndex[server] = reply.XIndex
					}
				} else { // server nextIndex 无日志
					rf.nextIndex[server] = args.PrevLogIndex + 1 - reply.XLen
				}
				rf.sendAppendEntries(server)
			}
			rf.mu.Unlock()
		}
	}()

}
