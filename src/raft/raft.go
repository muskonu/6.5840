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
	"6.5840/labgob"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type role int

const (
	FOLLOWER role = iota
	LEADER
	CANDIDATE
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Entry 日志
type Entry struct {
	Index int
	Term  int
	Cmd   interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state         role //服务器状态
	currentTerm   int  //服务器知道的最近任期，当服务器启动时初始化为0，单调递增
	votedFor      int  //当前任期中，该服务器给投过票的candidateId，如果没有则为null
	timeoutTicker *time.Timer
	log           []Entry //日志条目；每一条包含了状态机指令以及该条目被leader收到时的任期号

	commitIndex int //已知被提交的最高日志条目索引号，一开始是0，单调递增
	lastApplied int //应用到状态机的最高日志条目索引号，一开始为0，单调递增

	nextIndex  []int // leader保持对于其他所有server下次应该发送的日志的index
	matchIndex []int // leader保持对于其他所有server已知最大的已复制的index

	//3D
	snapshot          []byte
	lastIncludedIndex int // 快照中最后一个日志的索引
	lastIncludedTerm  int // 快照中最后一个日志的任期
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	rf.mu.Unlock()

	// Your code here (3A).
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var voteFor int
	var log []Entry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
	DPrintf("%dcrash recover,lastIncludedIndex:%d,lastIncludedTerm:%d\n", rf.me,
		lastIncludedIndex, lastIncludedTerm)
	rf.snapshot = rf.persister.ReadSnapshot()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.snapshot = snapshot
	rf.lastIncludedTerm = rf.log[index-rf.lastIncludedIndex-1].Term
	rf.log = rf.log[index-rf.lastIncludedIndex:]
	rf.lastIncludedIndex = index
	DPrintf("%s%d create Snapshot, lastIncludeIndex:%d,len Entries:%d\n", stateString(rf.state), rf.me, rf.lastIncludedIndex, len(rf.log))
}

type RequestVoteArgs struct {
	Term        int
	CandidateId int

	LastLogIndex int //安全性，选举限制
	LastLogTerm  int //安全性，选举限制
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	//任期已过期，直接返回
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}

	//若收到了任期更高的请求，则将自身的term更新成request的term，不是follower的变为follower
	if rf.currentTerm < args.Term {
		rf.BecomeFollower(args.Term)
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		var lastLog Entry
		//若所有日志都以快照方式存储
		if len(rf.log) == 0 {
			lastLog.Term = rf.lastIncludedTerm
			lastLog.Index = rf.lastIncludedIndex
		} else {
			lastLog = rf.log[len(rf.log)-1]
		}
		// 安全性，选举限制
		if args.LastLogTerm < lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex < lastLog.Index) {
			reply.VoteGranted = false
			return
		}
		rf.resetTicker(500)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		return
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int // leader的任期号
	LeaderId int // 用来让follower把客户端请求定向到leader

	PrevLogIndex int     // 紧接新日志条目之前的日志条目索引
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // 要存储的日志条目（心跳时为空；为提高效率，可发送多个日志条目）
	LeaderCommit int     // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// 快速恢复
	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	//任期已过期，直接返回
	if rf.currentTerm > args.Term {
		return
	}

	//若收到了任期更高的请求，则将自身的term更新成request的term，不是follower的变为follower
	if rf.currentTerm < args.Term || rf.state == CANDIDATE {
		rf.BecomeFollower(args.Term)
	}

	rf.resetTicker(500)

	targetIndex := len(rf.log) + rf.lastIncludedIndex
	//如果 prevLog 冲突则返回false
	if args.PrevLogIndex > targetIndex { // prevLog处没有日志
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - targetIndex
		reply.Success = false
		return
	}

	if args.PrevLogIndex-rf.lastIncludedIndex-1 >= 0 && rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term != args.PrevLogTerm {
		term := rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term
		prevLogIndex := args.PrevLogIndex - rf.lastIncludedIndex - 2
		// 因为应用程序只能感知到已经提交过的日志，所以快照中存储的日志都不会被覆盖。
		for prevLogIndex >= 0 && rf.log[prevLogIndex].Term == term {
			prevLogIndex--
		}
		prevLogIndex++
		reply.XTerm = term
		reply.XIndex = prevLogIndex
		reply.Success = false
		return
	}

	reply.Success = true

	//If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it
	for _, entry := range args.Entries {
		if entry.Index > rf.lastIncludedIndex+len(rf.log) {
			rf.log = append(rf.log, entry)
		} else if rf.log[entry.Index-rf.lastIncludedIndex-1].Term != entry.Term {
			rf.log = rf.log[:entry.Index-rf.lastIncludedIndex]
		}
		rf.log[entry.Index-rf.lastIncludedIndex-1] = entry
	}

	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		var serverIndex int
		if len(rf.log) == 0 {
			serverIndex = rf.lastIncludedIndex
		} else {
			serverIndex = rf.log[len(rf.log)-1].Index
		}
		rf.commitIndex = min(args.LeaderCommit, serverIndex)
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%s%d: Receive InstallSnapshot:lastIncludedIndex:%d\n", stateString(rf.state), rf.me, args.LastIncludedIndex)

	reply.Term = rf.currentTerm

	//任期已过期，直接返回
	if rf.currentTerm > args.Term {
		return
	}

	//收到了过期的快照，直接返回
	if args.LastIncludedIndex < rf.commitIndex {
		return
	}

	if args.LastIncludedIndex >= rf.lastIncludedIndex+len(rf.log) || //所有日志都被覆盖，server的日志置为nil
		rf.log[args.LastIncludedIndex-rf.lastIncludedIndex-1].Term != args.LastIncludedTerm {
		rf.log = nil
	} else { //保留覆盖之后的日志
		rf.log = rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]
	}
	rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
}

// 将日志提交到本地状态机
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.lastApplied < rf.lastIncludedIndex {
			msg := ApplyMsg{
				CommandValid:  false,
				Command:       nil,
				CommandIndex:  -1,
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			select {
			case rf.applyCh <- msg:
				DPrintf("%d apply install snapshot, lastLogIndex:%d\n", rf.me, msg.SnapshotIndex)
				rf.commitIndex = max(rf.lastIncludedIndex, rf.commitIndex)
				rf.lastApplied = rf.lastIncludedIndex
				goto out
			default:
				goto out
			}
		}
		for rf.lastApplied < rf.commitIndex {
			msg := ApplyMsg{
				CommandValid:  true,
				Command:       rf.log[rf.lastApplied-rf.lastIncludedIndex].Cmd,
				CommandIndex:  rf.lastApplied + 1,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
			select {
			case rf.applyCh <- msg:
				rf.lastApplied++
				DPrintf("%d apply Log: %+v\n", rf.me, msg.CommandIndex)
			default:
				goto out
			}
		}
	out:
		rf.mu.Unlock()

		//睡眠
		time.Sleep(8 * time.Millisecond)
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.lastIncludedIndex + 1 + len(rf.log)
	term := rf.currentTerm
	DPrintf("%s%d start command:%v, Term:%d, Index%d\n", stateString(rf.state), rf.me, command, term, index)
	isLeader := rf.state == LEADER
	if isLeader {
		entry := Entry{
			Index: index,
			Term:  term,
			Cmd:   command,
		}
		rf.log = append(rf.log, entry)
		rf.resetImmediately()
	}

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Intn(50))
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	// 3A
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))

	ms := 500 + (rand.Intn(500))
	rf.timeoutTicker = time.NewTimer(time.Duration(ms) * time.Millisecond)

	rf.log = []Entry{{
		Index: 0,
		Term:  0,
		Cmd:   nil,
	}}

	rf.lastIncludedTerm = -1
	rf.lastIncludedIndex = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// state machine translate state loop
	go rf.tickerLoop()

	go rf.leaderLoop()

	go rf.applier()

	return rf
}

// 重置计时器
func (rf *Raft) resetTicker(t int) {
	ms := t + (rand.Intn(t))
	if !rf.timeoutTicker.Stop() {
		select {
		case <-rf.timeoutTicker.C: // try to drain the channel
		default:
		}
	}
	rf.timeoutTicker.Reset(time.Duration(ms) * time.Millisecond)
}

// leader重置心跳计时器
func (rf *Raft) resetHeartbeat() {
	if !rf.timeoutTicker.Stop() {
		select {
		case <-rf.timeoutTicker.C: // try to drain the channel
		default:
		}
	}
	rf.timeoutTicker.Reset(100 * time.Millisecond)
}

// leader start立即发送
func (rf *Raft) resetImmediately() {
	if !rf.timeoutTicker.Stop() {
		select {
		case <-rf.timeoutTicker.C: // try to drain the channel
		default:
		}
	}
	rf.timeoutTicker.Reset(0)
}
