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

import "sync"
import "labrpc"
import "math/rand"
import "time"
import "sync/atomic"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type RaftState uint32

const (
	Follower RaftState = iota
	Candidate
	Leader
	Shutdown
)

const VOTENULL = -1

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	case Shutdown:
		return "shutdown"
	default:
		return "unknown"
	}
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type AppendEntries struct {
	Term     int "leader's term"
	LeaderId int "leader id"
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state            RaftState
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration
	electionTimer    *time.Timer
	heartbeatTimer   *time.Timer

	currentTerm int // all servers persistent
	votedFor    int // all servers persistent
	log         []*LogEntry

	commitIndex int // all servers volatile
	lastApplied int // all servers volatile

	nextIndex  []int //only on leaders volatile
	matchIndex []int //only on leaders volatile
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int "candidate's term"
	CandidateId int "candidate requestiong vote"
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  "currentTerm, for candidate to update itself"
	VoteGranted bool "true means candidate receive vote"
}

func (rf *Raft) resetTimer(timer *time.Timer, timeout time.Duration) {
	rf.disableTimer(timer)
	timer.Reset(timeout)
}

func (rf *Raft) disableTimer(timer *time.Timer) {
	if !timer.Stop() {
		// timer的正确用法，必须用select，否则可能会卡住，详见
		// http://tonybai.com/2016/12/21/how-to-use-timer-reset-in-golang-correctly/.
		select {
		case <-timer.C:
		default:
		}
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// all servers
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term) // 这里不能return，因为还没设置好回复
	}

	// 1
	if args.Term < rf.currentTerm {
		DPrintf("%v not vote %v my term:%d, vote term:%d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	//2
	if rf.votedFor != VOTENULL && rf.votedFor != args.CandidateId {
		DPrintf("%v not voteFor %v my term:%d, vote term:%d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	rf.votedFor = args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.resetTimer(rf.electionTimer, rf.electionTimeout)
	DPrintf("%v vote %v my term:%d, vote term:%d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("server(%v) handle appendentry, myTerm:%v, argsTerm:%v", rf.me, rf.currentTerm, args.Term)
	// all servers
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	// 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 2
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.resetTimer(rf.electionTimer, rf.electionTimeout)
	return
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	if rf.electionTimer != nil {
		rf.disableTimer(rf.electionTimer)
	}

	if rf.heartbeatTimer != nil {
		rf.disableTimer(rf.heartbeatTimer)
	}
}

func getRandomElectionTimeout() int {
	return 300 + rand.Intn(100)
}

func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Convert server(%v) state(%v=>candidate) term(%v)", rf.me,
		rf.state.String(), rf.currentTerm)
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.resetTimer(rf.electionTimer, rf.electionTimeout)
}

func (rf *Raft) leaderElection() {
	for {
		<-rf.electionTimer.C
		DPrintf("Server(%v) election timeout expire, term:%v", rf.me, rf.currentTerm)

		rf.convertToCandidate()

		// 5.2 Candidate, do not disableTimer now.

		var numVoted int32 = 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			go func(rf *Raft, idx int) {
				args := &RequestVoteArgs{
					rf.currentTerm,
					rf.me,
				}
				reply := &RequestVoteReply{}
				ret := rf.sendRequestVote(idx, args, reply)
				if ret {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.state != Candidate {
						return
					}

					if reply.Term > rf.currentTerm {
						rf.convertToFollower(reply.Term)
						return
					}

					if reply.VoteGranted {
						atomic.AddInt32(&numVoted, 1)
					}

					if atomic.LoadInt32(&numVoted) > int32(len(rf.peers)/2) {
						rf.convertToLeader()
					}
				}
			}(rf, i)
		}
	}
}

func (rf *Raft) convertToFollower(term int) {
	DPrintf("Convert server(%v) state(%v=>follower) term(%v => %v)", rf.me,
		rf.state.String(), rf.currentTerm, term)
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = VOTENULL
	rf.resetTimer(rf.electionTimer, rf.electionTimeout)
}

func (rf *Raft) convertToLeader() {
	DPrintf("Convert server(%v) state(%v=>leader) term %v", rf.me,
		rf.state.String(), rf.currentTerm)
	rf.disableTimer(rf.electionTimer) // 成为leader之后，关闭electionTimeout
	rf.state = Leader
	rf.votedFor = rf.me

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.startHeartbeart()
}

func (rf *Raft) startHeartbeart() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(rf *Raft, idx int) {
			rf.heartbeatTimer = time.NewTimer(rf.heartbeatTimeout)
			for {
				<-rf.heartbeatTimer.C
				reply := &AppendEntriesReply{}
				retChan := make(chan bool)
				go func(reply *AppendEntriesReply, retChan chan bool) {
					args := &AppendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderId: rf.me,
					}
					startTime := time.Now()
					ret := rf.sendAppendEntries(idx, args, reply)
					elapsed := time.Since(startTime)
					DPrintf("sendAppendEntries(%v => %v) took %s", rf.me, idx, elapsed)
					retChan <- ret
				}(reply, retChan)

				appendEntryTimeout := false
				select {
				case <-retChan:
				case <-time.After(150 * time.Millisecond): //这里要注意，如果不加一个超时，网络延时的情况下会导致选举多个leader。
					appendEntryTimeout = true
				}

				rf.mu.Lock()
				if reply.Success {
					DPrintf("AppendEntries(%v => %v) success, leaderTerm:%v, replyTerm:%v", rf.me, idx, rf.currentTerm, reply.Term)
				} else {
					if appendEntryTimeout {
						DPrintf("AppendEntries(%v => %v) timeout, leaderTerm:%v, replyTerm:%v", rf.me, idx, rf.currentTerm, reply.Term)
					} else {
						DPrintf("AppendEntries(%v => %v) failed, leaderTerm:%v, replyTerm:%v", rf.me, idx, rf.currentTerm, reply.Term)
					}
					if reply.Term > rf.currentTerm {
						rf.convertToFollower(reply.Term) // 对所有服务器，只要收到Term大于currentTerm，直接转换成Follower。
						rf.disableTimer(rf.heartbeatTimer)
					}
				}

				if rf.state == Leader {
					rf.resetTimer(rf.heartbeatTimer, rf.heartbeatTimeout)
				}
				rf.mu.Unlock()
			}
		}(rf, i)
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = VOTENULL
	rf.state = Follower

	rf.heartbeatTimeout = time.Duration(100) * time.Millisecond
	electionTimeout := getRandomElectionTimeout()
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.electionTimer = time.NewTimer(rf.electionTimeout)
	DPrintf("server(%v) electionTimeout:%v ms, state:%v, term:%v, votedFor:%v", me, electionTimeout, rf.state, rf.currentTerm, rf.votedFor)

	go rf.leaderElection()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
