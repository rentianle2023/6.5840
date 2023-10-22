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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	time "time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type State int

const (
	Leader State = iota
	Follower
	Candidate
)

// A Go object implementing a single Raft peer.
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
	votedFor    interface{}
	log         []LogEntry
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state             State
	lastHeartBeatTime time.Time

	votes   chan RequestVoteReply
	applyCh chan ApplyMsg

	startIndex    int
	snapshot      []byte
	snapshotTerm  int
	snapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.lastHeartBeatTime = time.Now().Add(-1 * time.Second)
	rf.log = []LogEntry{{
		Term: 0,
	}}
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	////fmt.Println(rf.me, "recover from crash", "snapshot term & index", rf.snapshotTerm, rf.snapshotIndex)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotTerm)
	e.Encode(rf.snapshotIndex)
	raftstate := w.Bytes()
	////fmt.Println(rf.me, "persist snapshot term and index", rf.snapshotTerm, rf.snapshotIndex)
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	//Your code here (2C).
	//Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var snapshotTerm int
	var snapshotIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotTerm) != nil ||
		d.Decode(&snapshotIndex) != nil {
		//fmt.Println("fail to decode")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}

	rf.snapshot = rf.persister.ReadSnapshot()
	if len(rf.snapshot) > 0 {
		rf.snapshotTerm = snapshotTerm
		rf.snapshotIndex = snapshotIndex
		rf.startIndex = snapshotIndex + 1
		////fmt.Println("startIndex", rf.startIndex)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	//////fmt.Println(rf.me, "snapshot at index", index, snapshot)
	// Your code here (2D).
	rf.snapshot = snapshot
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.log[index-rf.startIndex].Term
	rf.log = rf.log[index-rf.startIndex+1:]
	rf.startIndex = index + 1
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}
	////////fmt.Println("current term", rf.currentTerm, "request term", args.Term)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())) {
		////////fmt.Println(rf.me, "vote for", args.CandidateId, "at term", args.Term)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeartBeatTime = time.Now()
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, votes chan RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		votes <- *reply
	}
	return ok
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
	// Your code here (2B).
	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if isLeader {
		//////fmt.Println(rf.me, command, "at index", len(rf.log)+rf.startIndex)
		rf.log = append(rf.log, LogEntry{
			Term:    term,
			Command: command,
		})
	}
	rf.persist()
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
		// Your code here (2A)
		// Check if a leader election should be started.
		if rf.state == Candidate || (rf.state == Follower && time.Now().Sub(rf.lastHeartBeatTime) > time.Second) {
			rf.lastHeartBeatTime = time.Now()
			rf.state = Candidate
			rf.votedFor = rf.me
			rf.currentTerm++
			////////fmt.Println(rf.me, "start ask to vote for term", rf.currentTerm)
			newTerm := rf.currentTerm
			rf.votes = make(chan RequestVoteReply)
			voteNeeded := len(rf.peers)/2 + 1
			voteYes := 1
			voteNo := 0
			votes := make(chan RequestVoteReply)
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				args := &RequestVoteArgs{
					Term:         newTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.getLastLogIndex(),
					LastLogTerm:  rf.getLastLogTerm(),
				}
				reply := &RequestVoteReply{}
				go rf.sendRequestVote(i, args, reply, votes)
			}
			timer := time.After(time.Second)
		outer:
			for {
				select {
				case <-timer:
					break outer
				case vote := <-votes:
					if vote.VoteGranted {
						voteYes++
						if voteYes == voteNeeded {
							if rf.state == Candidate {
								rf.convertToLeader()
							}
							break outer
						}
					} else {
						endElection := false
						voteNo++
						if vote.Term > rf.currentTerm {
							rf.currentTerm = vote.Term
							rf.state = Follower
							endElection = true
						}
						if voteNo == voteNeeded {
							rf.state = Follower
							endElection = true
						}
						if endElection {
							break outer
						}
					}
				}
			}
			////////fmt.Println(rf.me, "state", rf.state)
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) convertToLeader() {
	////////fmt.Println(rf.me, "become leader")
	rf.state = Leader
	var nextIndex []int
	var matchIndex []int
	for i := 0; i < len(rf.peers); i++ {
		nextIndex = append(nextIndex, rf.getLastLogIndex()+1)
		matchIndex = append(matchIndex, 0)
	}
	rf.nextIndex = nextIndex
	rf.matchIndex = matchIndex
	go rf.startSendingAgreement()
	//go rf.checkCommitIndex()
}

func (rf *Raft) startSendingAgreement() {
	for {
		if rf.state != Leader {
			break
		}
		currentTerm := rf.currentTerm
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.state != Leader {
				break
			}
			if rf.getLastLogIndex() >= rf.nextIndex[i] {
				go rf.sendAgreement(i, rf.nextIndex[i], currentTerm)
			} else {
				args := &AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.getLastLogIndex(),
					PrevLogTerm:  rf.getLastLogTerm(),
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntriesReply{}
				go rf.sendAppendEntries(i, args, reply)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func findFirstIndexOfTerm(logs []LogEntry, term int) int {
	l, r := 0, len(logs)-1
	for l < r {
		mid := (l + r) >> 1
		if logs[mid].Term >= term {
			r = mid
		} else {
			l = mid + 1
		}
	}
	if logs[l].Term == term {
		return l
	}
	return -1
}

func findLastIndexOfTerm(logs []LogEntry, term int) int {
	l, r := 0, len(logs)-1
	for l < r {
		mid := (l + r + 1) >> 1
		if logs[mid].Term <= term {
			l = mid
		} else {
			r = mid - 1
		}
	}
	if logs[l].Term == term {
		return l
	}
	return -1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if (rf.state == Candidate && args.Term == rf.currentTerm) || args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.lastHeartBeatTime = time.Now()
	reply.Term = rf.currentTerm

	// need more log
	if args.PrevLogIndex > rf.getLastLogIndex() {
		////fmt.Println("need more log")
		reply.Success = false
		reply.XLen = rf.getLastLogIndex() + 1
		return
	}
	////////fmt.Println("last log index", rf.getLastLogIndex())
	if rf.getActualTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.getActualTerm(args.PrevLogIndex)
		reply.XIndex = findFirstIndexOfTerm(rf.log, reply.XTerm) + rf.startIndex
		reply.XLen = rf.getLastLogIndex() + 1
		return
	}

	// append new logs
	reply.Success = true
	if args.Entries != nil {
		//if len(rf.log) >= args.PrevLogIndex + len(args.Entries)
		for i := 0; i < len(args.Entries); i++ {
			idx := args.PrevLogIndex + i + 1
			if rf.getLastLogIndex() >= idx && rf.getActualIndex(idx) == args.Entries[i].Term {
				continue
			} else {
				rf.log = append(rf.log[:args.PrevLogIndex+1-rf.startIndex], args.Entries...)
				break
			}
		}
	}
	if args.LeaderCommit > rf.commitIndex && rf.getLastLogTerm() == rf.currentTerm {
		//fmt.Println(rf.me, "commit index", rf.commitIndex)
		nextIndex := min(args.LeaderCommit, rf.getLastLogIndex())
		if nextIndex > rf.lastApplied {
			i := rf.commitIndex + 1
			rf.commitIndex = nextIndex
			rf.lastApplied = nextIndex
			for ; i <= nextIndex; i++ {
				////////fmt.Println(rf.commitIndex, i, nextIndex, args.LeaderCommit)
				if i-rf.startIndex < 0 {
					continue
				}
				//fmt.Println("follower", rf.me, "apply", rf.log[i-rf.startIndex].Command, "at", i)
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i-rf.startIndex].Command,
					CommandIndex: i,
				}
			}
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAgreement(i int, index int, term int) {
	//////fmt.Println("start index", rf.startIndex, "send index", index)
	if rf.getActualIndex(index) < 0 {
		rf.prepareInstallSnapshot(i)
		return
	}
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: index - 1,
		PrevLogTerm:  rf.getActualTerm(index - 1),
		Entries:      rf.log[rf.getActualIndex(index):],
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{
		Term:    0,
		Success: false,
	}
	ok := rf.sendAppendEntries(i, args, reply)
	////fmt.Println(i, "reply", reply)
	if !ok {
		return
	}
	//////fmt.Println(reply)

	if !reply.Success && reply.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		return
	}

	if reply.Term != term {
		return
	}

	if reply.Success {
		rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[i] = rf.matchIndex[i] + 1
		////////fmt.Println(rf.me, rf.nextIndex, rf.state)
		idx := findNextCommitIndex(rf.matchIndex, rf.commitIndex, rf.me)
		//////fmt.Println("success", rf.matchIndex, idx, rf.getActualTerm(idx))
		if rf.getActualTerm(idx) != rf.currentTerm {
			return
		}
		//////fmt.Println("current commit", rf.commitIndex, "next commit", idx)
		//////fmt.Println(rf.me, "last applied", rf.lastApplied, "commit index", rf.commitIndex, "start Index", rf.startIndex, "idx", idx)

		if idx > rf.lastApplied {
			rf.mu.Lock()
			i := rf.lastApplied + 1
			rf.commitIndex = idx
			rf.lastApplied = idx
			for ; i <= idx; i++ {
				if i-rf.startIndex < 0 {
					continue
				}
				//fmt.Println("leader", rf.me, "apply", i, rf.log[i-rf.startIndex].Command)
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i-rf.startIndex].Command,
					CommandIndex: i,
				}
			}
			rf.mu.Unlock()
		}
	} else if reply.XLen > 0 {
		if reply.XLen < index {
			rf.nextIndex[i] = reply.XLen
		} else {
			idx := findLastIndexOfTerm(rf.log, reply.XTerm) + rf.startIndex
			if idx != -1 {
				rf.nextIndex[i] = idx
			} else {
				rf.nextIndex[i] = reply.XIndex
			}
		}
	} else {
		// failed and Xlen == 0
		rf.nextIndex[i] = -1
	}
}

func findNextCommitIndex(matchIndex []int, commitIndex int, me int) int {
	mx := 0
	for _, i := range matchIndex {
		mx = max(mx, i)
	}
	l, r := commitIndex, mx
	cntNeed := len(matchIndex)/2 + 1
	for l < r {
		mid := (l + r + 1) >> 1
		cnt := 1
		for i, x := range matchIndex {
			if i == me {
				continue
			}
			if x >= mid {
				cnt++
			}
		}
		if cnt >= cntNeed {
			l = mid
		} else {
			r = mid - 1
		}
	}
	return l
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft) getLastLogIndex() int {
	return rf.startIndex + len(rf.log) - 1
}

func (rf *Raft) getActualIndex(index int) int {
	return index - rf.startIndex
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.snapshotTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getActualTerm(index int) int {
	idx := rf.getActualIndex(index)
	if idx < 0 {
		return rf.snapshotTerm
	}
	if idx >= len(rf.log) {
		return -1
	}
	return rf.log[idx].Term
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	////fmt.Println(args, rf.startIndex)
	idx := rf.getActualIndex(args.LastIncludedIndex)
	rf.snapshot = args.Data
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.startIndex = args.LastIncludedIndex + 1
	//fmt.Println(idx, len(rf.log), rf.getActualTerm(args.LastIncludedIndex), args.LastIncludedTerm)
	if idx >= 0 && idx < len(rf.log) && rf.getActualTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
		rf.log = rf.log[args.LastIncludedIndex-rf.startIndex+1:]
	} else {
		rf.log = make([]LogEntry, 0)
	}

	if rf.snapshotIndex > rf.commitIndex {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.snapshot,
			SnapshotTerm:  rf.snapshotTerm,
			SnapshotIndex: rf.snapshotIndex,
		}
		rf.commitIndex = rf.snapshotIndex
	}

}

func (rf *Raft) prepareInstallSnapshot(server int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
		Data:              rf.snapshot,
	}
	reply := InstallSnapshotReply{}
	rf.sendInstallSnapshot(server, &args, &reply)

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
	} else {
		rf.nextIndex[server] = rf.snapshotIndex + 1
		rf.matchIndex[server] = rf.snapshotIndex
	}
}
