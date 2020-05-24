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
	"bytes"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"
import _ "net/http/pprof"

// import "bytes"
// import "../labgob"

func init() {
	rand.Seed(time.Now().UnixNano())
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
}

func getRandomTimeDuration() time.Duration {
	timeLen := int64(rand.Intn(kElectionMaximalTimeLimit-kElectionMinimalTimeLimit+1) + kElectionMinimalTimeLimit)
	return time.Duration(timeLen * int64(time.Millisecond) / int64(50*time.Millisecond) * int64(50*time.Millisecond))
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Idx     int
	Term    int
	Command interface{}
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

	// Persistent state on all servers
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
	state       string
	votesCount  int
	timeLimit   time.Duration
	startTime   time.Time
	applyCh     chan ApplyMsg
	applyCond   *sync.Cond

	// Channels
	doneChan chan bool
	// timeoutChan <-chan time.Time

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex     []int
	matchIndex    []int
	acceptedCount []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isLeader = rf.state == kServerStateLeader

	return term, isLeader
}

type PersistentState struct {
	CurrentTerm   int
	VotedFor      int
	Log           []LogEntry
	AcceptedCount []int
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	state := PersistentState{
		CurrentTerm: rf.CurrentTerm,
		VotedFor:    rf.VotedFor,
	}
	state.Log = make([]LogEntry, len(rf.Log))
	copy(state.Log, rf.Log)
	state.AcceptedCount = make([]int, len(rf.acceptedCount))
	copy(state.AcceptedCount, rf.acceptedCount)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(state)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	state := PersistentState{}
	if err := d.Decode(&state); err != nil {
		log.Fatal("[readPersist] Decoding error!!", err, rf)
	} else {
		rf.mu.Lock()
		rf.CurrentTerm = state.CurrentTerm
		rf.VotedFor = state.VotedFor
		rf.Log = make([]LogEntry, len(state.Log))
		copy(rf.Log, state.Log)
		rf.acceptedCount = make([]int, len(state.AcceptedCount))
		copy(rf.acceptedCount, state.AcceptedCount)
		rf.persist()
		rf.mu.Unlock()
	}
}

// TODO: MUST lock the mutex before calling!!
func (rf *Raft) getIntoNewTerm(termId int) {
	rf.CurrentTerm = termId
	rf.votesCount = 0
	rf.VotedFor = -1
	rf.state = kServerStateFollower
	rf.persist()
}

// TODO: MUST lock the mutex before calling!!
func (rf *Raft) timeReset() {
	rf.timeLimit = getRandomTimeDuration()
	rf.startTime = time.Now()
}

func (rf *Raft) applyLogEntry(logId int) {
	applyMsg := ApplyMsg{
		CommandValid: true,
		Command:      rf.Log[logId].Command,
		CommandIndex: logId,
	}
	rf.applyCh <- applyMsg
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
	rf.doneChan <- true
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	// Your initialization code here (2A, 2B, 2C).

	// For Lab 2A
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.state = kServerStateFollower
	rf.timeReset()
	rf.doneChan = make(chan bool, 1)
	rf.applyCh = applyCh

	// For Lab 2B
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.Log = make([]LogEntry, 1)
	rf.acceptedCount = make([]int, 1)
	rf.acceptedCount[0] = 3
	rf.applyCond = sync.NewCond(&sync.Mutex{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Start working
	go rf.Work()
	go rf.TimeoutChecker()
	go rf.ApplyWork()
	rf.applyCond.Broadcast()

	return rf
}
