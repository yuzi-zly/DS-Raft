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
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"



// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       	int
	Command     	interface{}
	UseSnapshot 	bool   // ignore for lab2; only used in lab3
	Snapshot    	[]byte // ignore for lab2; only used in lab3
}

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        		sync.Mutex
	peers     		[]*labrpc.ClientEnd
	persister 		*Persister
	me        		int // index into peers[]
	// Your data here.
	// Look at the paper's Figure 2 for a description of what state a Raft server must maintain.

	//persistent
	CurrentTerm 	int
	VoteFor 		int
	Logs    		[]LogEntry
	//volatile
	CommitIndex		int
	LastApplied		int
	//volatile on leaders
	NextIndex		[]int
	MatchIndex		[]int
	//others
	Role			int

	ElectionTimer	*time.Timer
	HeartBeatTicker *time.Ticker

	RVReplyChan 	chan *RequestVoteReply
	AEReplyChan		chan *AppendEntriesReply
	Logger      	*log.Logger
	//TODO()

}

// LogEntry
// struct of LogEntry
type LogEntry struct{
	Term 			int
	Index 			int
	Command			interface{}
}

// some const to be used
const(
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER	  = 2

	ELECTION_TIMEOUT_MAX = 500
	ELECTION_TIMEOUT_MIN = 200
	HEARTBEAT_INTERVAL = 50

	DEBUG = true
)

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.Role == LEADER
}

// persist
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Logs)
	rf.persister.SaveRaftState(w.Bytes())
}

// readPersist
// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VoteFor)
	d.Decode(&rf.Logs)

}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term 			int
	CandidateId 	int
	LastLogIndex 	int
	LastLogTerm 	int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term 			int
	VoteGranted 	bool
}

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if DEBUG {
		rf.Logger.Printf("I receive a RV from %v", args.CandidateId)
	}
	rf.ElectionTimer.Reset(RandomElectionTimeOut())
	//过时请求，不予理睬
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}
	//自身变为FOLLOWER
	if args.Term > rf.CurrentTerm {
		if rf.Role == CANDIDATE {
			rf.Role = FOLLOWER
			rf.VoteFor = -1
			rf.ElectionTimer.Reset(RandomElectionTimeOut())
			rf.persist()
			if DEBUG {
				rf.Logger.Printf("I am deprecated and change from a candidate to a follower\n")
			}
		}

		if rf.Role == LEADER {
			rf.HeartBeatTicker.Stop()
			rf.Role = FOLLOWER
			rf.VoteFor = -1
			rf.ElectionTimer.Reset(RandomElectionTimeOut())
			rf.persist()
			if DEBUG {
				rf.Logger.Printf("I am deprecated and change from the leader to a follower\n")
			}
		}
	}
	//投票
	if (args.Term > rf.CurrentTerm) ||
		(args.Term == rf.CurrentTerm && (args.CandidateId == rf.VoteFor || rf.VoteFor == -1)) {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		rf.VoteFor = args.CandidateId
		if DEBUG {
			rf.Logger.Printf("I vote for %v", args.CandidateId)
		}
	} else {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		if DEBUG {
			rf.Logger.Printf("I don't vote for %v", args.CandidateId)
		}
	}
	rf.CurrentTerm = args.Term
	rf.persist()
}

// sendRequestVote
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	if DEBUG {
		rf.Logger.Printf("I am in sendRV to %v", server)
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesArgs
// example AppendEntries RPC arguments structure.
type AppendEntriesArgs struct{
	Term 			int
	LeaderID		int
	PrevLogIndex	int
	PrevLogTerm 	int
	Entries 		[]LogEntry
	LeaderCommit	int
}

// AppendEntriesReply
// example AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term 			int
	Success			bool
}


func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm <= args.Term {
		if DEBUG {
			rf.Logger.Printf("my Term %v and arg Term %v\n", rf.CurrentTerm, args.Term)
		}
		rf.CurrentTerm = args.Term
		rf.Role = FOLLOWER
		rf.VoteFor = -1
		rf.ElectionTimer.Reset(RandomElectionTimeOut())
		if rf.Role == LEADER {
			rf.HeartBeatTicker.Stop()
		}
		rf.persist()
		if DEBUG {
			rf.Logger.Printf("I receive a HeartBeat from %v and become a follower\n", args.LeaderID)
		}
	}


	reply.Term = rf.CurrentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


// Start
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
	//TODO()
	return index, term, isLeader
}

// Kill
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// Make
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

	// Your initialization code here.
	rand.Seed(time.Now().UnixNano())
	rf.CurrentTerm = 0
	rf.VoteFor = -1
	rf.Logs = []LogEntry{}
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = []int{}
	rf.MatchIndex = []int{}
	rf.Role = FOLLOWER
	rf.ElectionTimer = time.NewTimer(RandomElectionTimeOut())
	rf.Logger = log.New(os.Stdout, fmt.Sprintf("[peer %v] ", rf.me), log.Ldate|log.Lmicroseconds|log.Lshortfile)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//start goroutines
	go rf.DeadLoop()

	return rf
}

// RandomElectionTimeOut
// for randomly set election timeout
func RandomElectionTimeOut() time.Duration{
	return time.Duration(ELECTION_TIMEOUT_MIN +
		rand.Intn(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN)) * time.Millisecond
}

// DeadLoop
// a dead loop ensure always run
func (rf *Raft) DeadLoop() {
	for {
		switch rf.Role {
			case FOLLOWER: rf.FollowerHandler()
			case CANDIDATE: rf.CandidateHandler()
			case LEADER: rf.LeaderHandler()
		}
	}
}

// FollowerHandler
// handle follower in dead loop
func (rf *Raft) FollowerHandler(){
	if DEBUG {
		rf.Logger.Printf("I am in FollowerHandler with Term %v, VoteFor %v",rf.CurrentTerm, rf.VoteFor)
	}
	select {
		case <- rf.ElectionTimer.C:
			rf.CurrentTerm = rf.CurrentTerm + 1
			rf.Role = CANDIDATE
			rf.VoteFor = -1
			rf.ElectionTimer.Reset(RandomElectionTimeOut())
			rf.persist()
			if DEBUG {
				rf.Logger.Printf("I change from a follower to a candidate\n")
			}
	}
}

// CandidateHandler
// handle candidate in dead loop
func (rf *Raft) CandidateHandler(){
	if DEBUG {
		rf.Logger.Printf("I change from a follower to a candidate\n")
	}
	// vote for self
	rf.VoteFor = rf.me
	// send request vote
	state := make(chan bool)
	go rf.sendRequestVotePreprocess(&state)
	select {
		case ok := <-state:
			if ok {
				rf.Role = LEADER
				rf.HeartBeatTicker = time.NewTicker(HEARTBEAT_INTERVAL * time.Millisecond)
				rf.persist()
				if DEBUG {
					rf.Logger.Printf("I change from a follower to a candidate\n")
				}
			}
		case <-rf.ElectionTimer.C:
			rf.ElectionTimer.Reset(RandomElectionTimeOut())
			rf.CurrentTerm = rf.CurrentTerm + 1
			rf.VoteFor = -1
			rf.persist()
	}
}

func (rf *Raft) sendRequestVotePreprocess(state *chan bool) {
	//vote for self
	if DEBUG {
		rf.Logger.Printf("I am in sendRVPreprocess with Term %v", rf.CurrentTerm)
	}
	cnt := 1
	rf.RVReplyChan = make(chan *RequestVoteReply)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			var reply RequestVoteReply
			var args RequestVoteArgs
			args.Term = rf.CurrentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.Logs) - 1
			if args.LastLogIndex >= 0 {
				args.LastLogTerm = rf.Logs[args.LastLogIndex].Term
			}
			ok := rf.sendRequestVote(server, args, &reply)
			if ok {
				rf.RVReplyChan <- &reply
			}
		}(server)
	}
	finish := false
	for {
		replyPointer := <- rf.RVReplyChan
		if finish {
			continue
		}
		if replyPointer.Term > rf.CurrentTerm {
			rf.CurrentTerm = replyPointer.Term
			rf.Role = FOLLOWER
			rf.VoteFor = -1
			rf.ElectionTimer.Reset(RandomElectionTimeOut())
			rf.persist()
			*state <- false
			if DEBUG {
				rf.Logger.Printf("I am deprecated and change from a candidate to a follower\n")
			}
			finish = true
		}
		if replyPointer.VoteGranted {
			cnt++
			if cnt > len(rf.peers) / 2 {
				*state <- true
				finish = true
			}
		}
	}
}

// LeaderHandler
// handle leader in dead loop
func (rf *Raft) LeaderHandler(){
	state := make(chan bool)
	go rf.sendHeartBeatPreprocess(&state)

}

func (rf *Raft) sendHeartBeatPreprocess(state *chan bool) {
	for range rf.HeartBeatTicker.C {
		if rf.Role != LEADER {
			return
		}
		rf.AEReplyChan = make(chan *AppendEntriesReply)
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				var reply AppendEntriesReply
				var args AppendEntriesArgs
				args.Term = rf.CurrentTerm
				args.LeaderID = rf.me
				ok := rf.sendAppendEntries(server, args, &reply)
				if ok {
					rf.AEReplyChan <- &reply
				}
			}(server)
		}
		finish := false
		for {
			replyPointer := <- rf.AEReplyChan
			if finish {
				continue
			}
			if replyPointer.Term > rf.CurrentTerm {
				rf.CurrentTerm = replyPointer.Term
				rf.Role = FOLLOWER
				rf.VoteFor = -1
				rf.ElectionTimer.Reset(RandomElectionTimeOut())
				rf.HeartBeatTicker.Stop()
				rf.persist()
				*state <- false
				if DEBUG {
					rf.Logger.Printf("I am deprecated and change from the leader to a follower\n")
				}
				finish = true
			}
		}
	}
}