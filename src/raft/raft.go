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
	"log"
	"math/rand"
	"strconv"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type State int32

const (
	Follower State = iota
	Candidate
	Leader
)

type MsgType int

const (
	HeartBeat MsgType = iota
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
	peerId      string
	currentTerm int    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    string // peerId that received vote in current term (or null if none)
	state       *State  // the leader peer index into peers

	log []Log

	heartBeatCh       chan bool
	heartBeatInterval time.Duration
	grantCh           chan bool
	electAsLeaderCh   chan bool
	electionTimeout   time.Duration // for election

	commitIndex int
	lastApplied int
}

type Log struct {
	Index int
	Term  int
}

func (rf *Raft) getState() State {
	return State(atomic.LoadInt32((*int32)(rf.state)))
}

func (rf *Raft) setState(state State) {
	atomic.StoreInt32((*int32)(rf.state), int32(state))
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
	isleader = rf.getState() == Leader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int    // candidate’s term
	CandidateId  string // candidate requesting vote
	LastLogIndex int    // index of candidate’s last log entry
	LastLogTerm  int    // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
	VoterId     string
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoterId = rf.peerId
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}
	lastLog := rf.getLastLog()
	if (rf.votedFor == "" || rf.votedFor == args.CandidateId) && (lastLog.Index <= args.LastLogIndex && lastLog.Term <= args.LastLogTerm) {
		reply.Term = rf.currentTerm
		rf.grantCh <- true
		reply.VoteGranted = true
		// set voteFor
		rf.votedFor = args.CandidateId
		log.Printf("peer %v elect peer %v as leader\n", rf.peerId, args.CandidateId)
	}
	return
}

func (rf *Raft) stepDownToFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = ""
	rf.setState(Follower)
	log.Printf("peer %v step down to follower", rf.peerId)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteCount *int32) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	log.Printf("peer %v request vote to peer %v result %v", rf.peerId, reply.VoterId, reply)
	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.getState() != Candidate || args.Term != rf.currentTerm {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(reply.Term)
	}
	if reply.VoteGranted {
		atomic.AddInt32(voteCount, 1)
	}
	if int(atomic.LoadInt32(voteCount)) > len(rf.peers)/2 {
		rf.setState(Leader)
		rf.electAsLeaderCh <- true
	}
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	// TODO dump info
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	lastLog := rf.getLastLog()
	req := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.peerId,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	rf.mu.Unlock()
	replyCh := make(chan bool, len(rf.peers))
	var votedCount int32 = 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(index int) {
			ok := rf.sendRequestVote(index, req, &RequestVoteReply{}, &votedCount)
			replyCh <- ok
		}(i)
	}
}

func (rf *Raft) randomElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration(rand.Intn(150) + 150) + rf.electionTimeout
}

func (rf *Raft) getLastLog() *Log {
	lastLog := Log{}
	if len(rf.log) > 0 {
		lastLog = rf.log[len(rf.log)-1]
	}
	return &lastLog
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	req := &AppendEntriesArgs{
		Type:         HeartBeat,
		Term:         rf.currentTerm,
		LeaderId:     rf.peerId,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i, req, &AppendEntriesReply{})
	}
}

type AppendEntriesArgs struct {
	Type         MsgType
	Term         int    // leader's term
	LeaderId     string // so follower can redirect clients
	PrevLogIndex int    // index of log entry immediately preceding new ones
	PrevLogTerm  int    // term of prevLogIndex entry
	//Entries      []Entries
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.getState() != Leader || args.Term != rf.currentTerm {
		return ok
	}

	if rf.currentTerm < reply.Term {
		rf.stepDownToFollower(reply.Term)
	}

	return ok
}

// AppendEntries TODO(2B): currently only work for leader send heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}
	rf.heartBeatCh <- true
	reply.Term = rf.currentTerm
	reply.Success = true
	return
}

func (rf *Raft) runServer() {
	for !rf.killed() {
		switch rf.getState() {
		case Leader:
			rf.sendHeartbeat()
			time.Sleep(rf.heartBeatInterval)
		case Follower:
			select {
			case <-rf.heartBeatCh:
			case <-rf.grantCh:
			case <-time.After(rf.randomElectionTimeout()):
				log.Printf("peer %v step to candiate", rf.peerId)
				rf.setState(Candidate)
			}
		case Candidate:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.peerId
			rf.persist()
			rf.mu.Unlock()
			go rf.leaderElection()
			select {
			case <- time.After(rf.randomElectionTimeout()):
				log.Printf("peer %v elect leader timeout", rf.peerId)
			case <-rf.heartBeatCh:
			case <-rf.electAsLeaderCh:
				rf.setState(Leader)
				log.Printf("peer %v step to leader", rf.peerId)
			}
		}
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = sync.Mutex{}

	// Your initialization code here (2A, 2B, 2C).
	state := Follower
	rf.state = &state
	rf.currentTerm = 0
	rf.votedFor = ""
	rf.log = make([]Log, 0)
	rf.heartBeatCh = make(chan bool)
	rf.grantCh = make(chan bool)
	rf.heartBeatInterval = time.Millisecond * 150
	rf.electionTimeout = 2 * rf.heartBeatInterval
	rf.electAsLeaderCh = make(chan bool)
	rf.peerId = strconv.Itoa(rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.runServer()

	return rf
}
