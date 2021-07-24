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
	Entries
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
	state       *State // the leader peer index into peers

	logEntries []LogEntry

	heartBeatCh       chan bool
	heartBeatInterval time.Duration
	grantCh           chan bool
	electAsLeaderCh   chan bool
	electionTimeout   time.Duration // for election

	commitIndex int
	lastApplied int

	LeaderStatus LeaderStatus

	applyCh chan ApplyMsg
}

type LeaderStatus struct {
	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIndex []int
	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIndex []int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
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
	if (rf.votedFor == "" || rf.votedFor == args.CandidateId) && (lastLog.Term < args.LastLogTerm || (lastLog.Index <= args.LastLogIndex && lastLog.Term == args.LastLogTerm)) {
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
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	if !rf.killed() && rf.getState() == Leader {
		isLeader = true
		lastEntry := rf.getLastLog()
		index = lastEntry.Index + 1
		term = rf.currentTerm
		newEntry := LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		}
		//DPrintf("peer=%v start command=%+v", rf.me, newEntry)
		rf.logEntries = append(rf.logEntries, newEntry)
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

// sendEntries this is a loop
// when the state is leader compare the last entry index with the leaderStatus.nextIndex[i]
// if the entry be receive > 1/2 * peers update the commitIndex and notify the Start() method
func (rf *Raft) sendEntries() {
	rf.mu.Lock()
	lastLog := rf.getLastLog()
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		matchIndex := rf.LeaderStatus.matchIndex[i]
		nextIndex := rf.LeaderStatus.nextIndex[i]
		//DPrintf("send entry peer=%v matchIndex=%v lastIndex=%v nextIndex=%v", i, matchIndex, lastLog.Index, nextIndex)
		var req *AppendEntriesArgs
		// TODO: whether delete ???
		if matchIndex >= lastLog.Index {
			req = &AppendEntriesArgs{
				Type:         HeartBeat,
				Term:         rf.currentTerm,
				LeaderId:     rf.peerId,
				LeaderCommit: rf.commitIndex,
			}
			DPrintf("peer=%v send heartbeat to peer=%v", rf.me, i)
		} else {
			// TODO: if the logEntries be cutoff after make snapshot, we should shift the start index
			logEntries := rf.logEntries[matchIndex+1 : min(nextIndex+1, len(rf.logEntries))]
			prevLog := rf.logEntries[matchIndex]
			req = &AppendEntriesArgs{
				Type:         Entries,
				Term:         rf.currentTerm,
				LeaderId:     rf.peerId,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				LogEntries:   logEntries, // TODO: refine to control each time send message count (case 2B)
				LeaderCommit: rf.commitIndex,
			}
			//DPrintf("peer=%v send entry=%v to=%v next=%v logEntrySize=%d", rf.me, rf.logEntries[matchIndex+1 : nextIndex+1], i, nextIndex, len(logEntries))
		}
		rf.mu.Unlock()
		go rf.sendAppendEntries(i, req, &AppendEntriesReply{})
	}
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
	rf.currentTerm++
	rf.votedFor = rf.peerId
	rf.persist()
	lastLog := rf.getLastLog()
	req := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.peerId,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	rf.mu.Unlock()
	go func() {
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
	}()
}

func (rf *Raft) randomElectionTimeout() time.Duration {
	return time.Millisecond*time.Duration(rand.Intn(150)+150) + rf.electionTimeout
}

func (rf *Raft) getLastLog() *LogEntry {
	if len(rf.logEntries) > 0 {
		return &rf.logEntries[len(rf.logEntries)-1]
	}
	return &LogEntry{}
}

type AppendEntriesArgs struct {
	Type         MsgType
	Term         int    // leader's term
	LeaderId     string // so follower can redirect clients
	PrevLogIndex int    // index of log entry immediately preceding new ones
	PrevLogTerm  int    // term of prevLogIndex entry
	LogEntries   []LogEntry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term           int      // currentTerm, for leader to update itself
	Success        bool     // true if follower contained entry matching prevLogIndex and prevLogTerm
	LatestLogEntry LogEntry // when the success is false the follower will response the the latest logEntry
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
	if len(args.LogEntries) == 0 {
		return ok
	}
	// check the reply
	if reply.Success {
		matchIndex := args.LogEntries[len(args.LogEntries)-1].Index
		rf.LeaderStatus.matchIndex[server] = matchIndex
		rf.LeaderStatus.nextIndex[server] = matchIndex + 1
		if matchIndex > rf.commitIndex {
			matchCount := 1
			for i, peerMatchIndex := range rf.LeaderStatus.matchIndex {
				if i != rf.me && peerMatchIndex >= matchIndex {
					matchCount++
				}
			}
			if matchCount >= len(rf.peers)/2+1 {
				rf.commitIndex = matchIndex
				IPrintf("leader peer=%v commitIndex=%v", rf.me, rf.commitIndex)
			}
		}
	} else {
		rf.LeaderStatus.nextIndex[server] = reply.LatestLogEntry.Index
	}
	return ok
}

// AppendEntries receive the leader appendEntries request
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
	// TODO: check, this is a new approach to use goroutine
	go func() {
		rf.heartBeatCh <- true
	}()
	reply.Term = rf.currentTerm
	reply.Success = true

	if len(args.LogEntries) > 0 {
		// validate the log, remove duplicate
		reply.Success, reply.LatestLogEntry = rf.appendEntries(args)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLog().Index)
	}
	return
}

func (rf *Raft) appendEntries(args *AppendEntriesArgs) (bool, LogEntry) {
	// 1. get latest log
	lastEntry := rf.getLastLog()
	// 2. Reply false if the log does not contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > lastEntry.Index {
		DPrintf("peer=%v reject entry: index too large, prevLogIndex=%v peerIndex=%v", rf.peerId, args.PrevLogIndex, lastEntry.Index)
		return false, *lastEntry
	}
	// 3. When the latest entry index >= PrevLogIndex find the entry index == PrevLogIndex
	//    If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	for i := len(rf.logEntries) - 1; i >= 0; i-- {
		if rf.logEntries[i].Index == args.PrevLogIndex && rf.logEntries[i].Term != args.PrevLogTerm {
			if i == 0 {
				rf.logEntries = make([]LogEntry, 0)
			} else {
				rf.logEntries = rf.logEntries[0 : i-1]
			}
			DPrintf("peer=%v reject entry: term is not match", rf.peerId)
			return false, *rf.getLastLog()
		} else if rf.logEntries[i].Index == args.PrevLogIndex && rf.logEntries[i].Term == args.PrevLogTerm {
			//DPrintf("peer=%v prevLogIndex=%v prevLogTerm=%v originEntries=%v appendEntries %v", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.logEntries[0:i+1], args.LogEntries)
			// 4. Append any new entries not already in the log
			rf.logEntries = append(rf.logEntries[0:i+1], args.LogEntries...)
			break
		}
	}
	return true, LogEntry{}
}

// applyEntries
func (rf *Raft) applyEntries() {
	for !rf.killed() {
		//DPrintf("peer=%v lastApplied=%v commitIndex=%v", rf.me, rf.lastApplied, rf.commitIndex)
		rf.mu.Lock()
		appliedEntries := make([]LogEntry, 0)
		if rf.lastApplied < rf.commitIndex {
			appliedEntries = copyEntries(rf.logEntries, rf.lastApplied+1, rf.commitIndex+1)
		}
		rf.mu.Unlock()
		if len(appliedEntries) > 0 {
			for _, entry := range appliedEntries {
				rf.applyCh <- ApplyMsg{
					CommandValid:  true,
					Command:       entry.Command,
					CommandIndex:  entry.Index,
					SnapshotValid: false,
					Snapshot:      nil,
					SnapshotTerm:  0,
					SnapshotIndex: 0,
				}
			}
			//DPrintf("apply entry=%v", entry)
			rf.mu.Lock()
			rf.lastApplied = appliedEntries[len(appliedEntries)-1].Index
			rf.mu.Unlock()
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func copyEntries(entries []LogEntry, start, end int) []LogEntry {
	result := make([]LogEntry, end - start)
	for i := start; i < end; i++ {
		result[i-start] = entries[i]
	}
	return result
}

func (rf *Raft) runServer() {
	for !rf.killed() {
		switch rf.getState() {
		case Leader:
			rf.sendEntries()
			time.Sleep(rf.heartBeatInterval)
		case Follower:
			select {
			case <-rf.heartBeatCh:
			case <-rf.grantCh:
			case <-time.After(rf.randomElectionTimeout()):
				rf.stepToCandidate()
			}
		case Candidate:
			rf.leaderElection()
			select {
			case <-time.After(rf.randomElectionTimeout()):
				log.Printf("peer %v elect leader timeout", rf.peerId)
			case <-rf.heartBeatCh:
			case <-rf.electAsLeaderCh:
				rf.stepToLeader()
			}
		}
	}
}

func (rf *Raft) stepToCandidate() {
	log.Printf("peer %v step to candiate", rf.peerId)
	rf.setState(Candidate)
}

func (rf *Raft) stepToLeader() {
	rf.mu.Lock()
	nextIndex := make([]int, len(rf.peers))
	matchIndex := make([]int, len(rf.peers))
	lastLog := rf.getLastLog()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		nextIndex[i] = lastLog.Index + 1
		matchIndex[i] = 0
	}
	rf.mu.Unlock()
	rf.LeaderStatus = LeaderStatus{
		nextIndex:  nextIndex,
		matchIndex: matchIndex,
	}
	rf.setState(Leader)
	log.Printf("peer %v step to leader", rf.peerId)
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
	// init the log entries
	rf.logEntries = []LogEntry{
		{
			Index: 0,
			Term:  0,
		},
	}
	rf.heartBeatCh = make(chan bool)
	rf.grantCh = make(chan bool)
	rf.heartBeatInterval = time.Millisecond * 150
	rf.electionTimeout = 2 * rf.heartBeatInterval
	rf.electAsLeaderCh = make(chan bool)
	rf.peerId = strconv.Itoa(rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.applyCh = applyCh

	go rf.applyEntries()

	// start ticker goroutine to start elections
	go rf.runServer()

	return rf
}
