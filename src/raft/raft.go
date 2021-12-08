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
	applyChan		*chan ApplyMsg

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

	HBReplyVec		[]int
	HBCheckTicker	*time.Ticker

	RVReplyChan 	chan *RequestVoteReply
	AEChans			[]chan bool
	Logger          *log.Logger
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

	ELECTION_TIMEOUT_MAX = 400
	ELECTION_TIMEOUT_MIN = 200
	HEARTBEAT_INTERVAL = 100
	HEARTBEAT_CHECK = 150

	DEBUG = false
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
		rf.Logger.Printf("I receive a RV from %v with Term %v and My term %v\n", args.CandidateId, args.Term, rf.CurrentTerm)
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
		if DEBUG {
			rf.Logger.Printf("I am deprecated and change from a %v to a follower\n", rf.Role)
		}
		if rf.Role == CANDIDATE {
			rf.becomeFollower()
		}

		if rf.Role == LEADER {
			rf.becomeFollower()
		}
	}
	//投票
	if (args.Term > rf.CurrentTerm) ||
		(args.Term == rf.CurrentTerm && (args.CandidateId == rf.VoteFor || rf.VoteFor == -1)) {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		rf.VoteFor = args.CandidateId
		if DEBUG {
			rf.Logger.Printf("I vote for %v\n", args.CandidateId)
		}
	} else {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		if DEBUG {
			rf.Logger.Printf("I don't vote for %v\n", args.CandidateId)
		}
	}
	rf.CurrentTerm = args.Term
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
	ConflictTerm	int
	MayMatchIndex	int
}


func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		//过期，不予理睬
		reply.Term = rf.CurrentTerm
		reply.Success = false
		reply.ConflictTerm = -1
		reply.MayMatchIndex = -1
	} else {
		rf.CurrentTerm = args.Term
		if DEBUG {
			rf.Logger.Printf("I receive a AE become a follower\n")
		}
		rf.becomeFollower()
		reply.Term = args.Term
		if args.PrevLogIndex > len(rf.Logs) - 1 || (args.PrevLogIndex >= 0 && args.PrevLogTerm != rf.Logs[args.PrevLogIndex].Term) {
			reply.Success = false
			if args.PrevLogIndex > len(rf.Logs) - 1 {
				reply.ConflictTerm = 0
				reply.MayMatchIndex = len(rf.Logs)
			} else {
				reply.ConflictTerm = rf.Logs[args.PrevLogIndex].Term
				tIndex := args.PrevLogIndex
				for ; tIndex >= 0; tIndex-- {
					if rf.Logs[tIndex].Term != reply.ConflictTerm {
						break
					}
				}
				reply.MayMatchIndex = tIndex + 1
			}
		} else {
			reply.Success = true
			if len(args.Entries) > 0{
				index := args.PrevLogIndex + 1
				for ; index < args.PrevLogIndex + 1 + len(args.Entries); index++ {
					if len(rf.Logs) - 1 >= index {
						if rf.Logs[index].Term != args.Entries[index - args.PrevLogIndex - 1].Term {
							rf.Logs = rf.Logs[:index]
							break
						}
					} else {
						break
					}
				}
				for appIndex := index; appIndex < args.PrevLogIndex + 1  + len(args.Entries); appIndex++ {
					entry := args.Entries[appIndex - args.PrevLogIndex - 1]
					rf.Logs = append(rf.Logs, LogEntry{entry.Term, entry.Index, entry.Command})
				}
			}
		}
		if reply.Success && args.LeaderCommit > rf.CommitIndex {
			if args.LeaderCommit > args.PrevLogIndex + len(args.Entries) {
				rf.CommitIndex = args.PrevLogIndex + len(args.Entries)
			} else {
				rf.CommitIndex = args.LeaderCommit
			}
		}
		rf.Apply()
		if DEBUG {
			rf.Logger.Printf("%v LogNum %v, commitIndex %v\n", rf.me, len(rf.Logs), rf.CommitIndex)
		}
	}
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
	if rf.Role != LEADER {
		if DEBUG {
			rf.Logger.Printf("I am not Leader so I cannot Start\n")
		}
		return -1, rf.CurrentTerm, false
	}
	index := len(rf.Logs)
	term := rf.CurrentTerm
	rf.Logs = append(rf.Logs, LogEntry{term, index, command})
	rf.NextIndex[rf.me] = len(rf.Logs)
	rf.MatchIndex[rf.me] = len(rf.Logs) - 1
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		if len(rf.Logs) - 1 >= rf.NextIndex[server] {
			if DEBUG {
				rf.Logger.Printf("prepare %v to %v\n", command, server)
			}
			rf.AEChans[server] <- true
			if DEBUG {
				rf.Logger.Printf("Send %v to %v\n", command, server)
			}
		}
	}

	return index + 1, term, true
}

// Kill
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.ElectionTimer.Stop()
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
	rf.applyChan = &applyCh

	// Your initialization code here.
	rand.Seed(time.Now().UnixNano())
	rf.CurrentTerm = 0
	rf.VoteFor = -1
	rf.Logs = []LogEntry{}
	rf.CommitIndex = -1
	rf.LastApplied = -1
	rf.NextIndex = make([]int, len(rf.peers), len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers), len(rf.peers))
	rf.Role = FOLLOWER
	rf.ElectionTimer = time.NewTimer(RandomElectionTimeOut())
	rf.HBReplyVec = make([]int, len(rf.peers), len(rf.peers))
	rf.AEChans = make([]chan bool, len(rf.peers), len(rf.peers))
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
	select {
		case <- rf.ElectionTimer.C:
			rf.CurrentTerm = rf.CurrentTerm + 1
			rf.becomeCandidate()
			if DEBUG {
				rf.Logger.Printf("I change from a follower to a candidate\n")
			}
	}
}

// CandidateHandler
// handle candidate in dead loop
func (rf *Raft) CandidateHandler(){
	// vote for self
	rf.VoteFor = rf.me
	// send request vote
	state := make(chan bool)
	go rf.sendRequestVoteForAllServers(&state)
	select {
		case ok := <-state:
			if ok {
				if DEBUG {
					rf.Logger.Printf("I change from a candidate to the leader\n")
				}
				rf.becomeLeader()
				for server := range rf.peers{
					if server == rf.me{
						continue
					}
					go rf.sendHeartBeat(server)
					go rf.sendAppendEntriesRoutine(server)
				}
			}
		case <-rf.ElectionTimer.C:
			rf.CurrentTerm = rf.CurrentTerm + 1
			rf.becomeCandidate()
	}
}

func (rf *Raft) sendRequestVoteForAllServers(state *chan bool) {
	if rf.Role != CANDIDATE {
		return
	}
	//vote for self
	cnt := 1
	rf.RVReplyChan = make(chan *RequestVoteReply)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			if rf.Role != CANDIDATE {
				return
			}
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
		if rf.Role != CANDIDATE {
			return
		}
		replyPointer := <- rf.RVReplyChan
		if finish {
			continue
		}
		if replyPointer.Term > rf.CurrentTerm {
			rf.CurrentTerm = replyPointer.Term
			rf.becomeFollower()
			*state <- false
			if DEBUG {
				rf.Logger.Printf("I am deprecated and change from a candidate to a follower\n")
			}
			finish = true
		}
		if replyPointer.VoteGranted {
			cnt++
			if DEBUG {
				rf.Logger.Printf("cnt++\n")
			}
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
	for range rf.HBCheckTicker.C {
		if rf.Role != LEADER {
			rf.HBCheckTicker.Stop()
			return
		}
		rf.mu.Lock()
		cnt := 0
		for server := range rf.peers{
			if rf.HBReplyVec[server] == 1 {
				cnt++
			}
			rf.HBReplyVec[server] = 0
		}
		rf.mu.Unlock()
		if cnt < len(rf.peers) / 2 {
			if DEBUG {
				rf.Logger.Printf("I don't receive many HBReply and become a follower\n")
			}
			rf.becomeFollower()
			rf.HBCheckTicker.Stop()
			return
		}
	}
}

func (rf *Raft) sendHeartBeat(server int) {
	HeartBeatTicker := time.NewTicker(HEARTBEAT_INTERVAL * time.Millisecond)
	for range HeartBeatTicker.C {
		timeout := time.NewTimer(HEARTBEAT_INTERVAL * time.Millisecond)
		if rf.Role != LEADER {
			if DEBUG {
				rf.Logger.Printf("SHB return with %v\n", server)
			}
			HeartBeatTicker.Stop()
			return
		}
		var reply AppendEntriesReply
		args := rf.makeAppendEntriesArg(server, true)
		if DEBUG {
			rf.Logger.Printf("send HB to %v", server)
		}
		okCh := make(chan bool)
		go func() {
			okCh <- rf.sendAppendEntries(server, args, &reply)
		}()
		select {
			case ok := <- okCh:
				if ok {
					if reply.Term > rf.CurrentTerm {
						rf.CurrentTerm = reply.Term
						if DEBUG{
							rf.Logger.Printf("I out of date and become a follower\n")
						}
						rf.becomeFollower()
					} else {
						rf.mu.Lock()
						rf.HBReplyVec[server] = 1
						rf.mu.Unlock()
					}
				}
			case <- timeout.C:
				continue
		}
	}
}

func (rf *Raft) sendAppendEntriesRoutine(server int) {
	for {
		if rf.Role != LEADER {
			if DEBUG {
				rf.Logger.Printf("SAER return with %v\n", server)
			}
			return
		}
		select {
			case <- rf.AEChans[server]:
				if DEBUG {
					rf.Logger.Printf("cmd: %v to %v\n", rf.Logs[len(rf.Logs) - 1].Command, server)
				}
				timeout := time.NewTimer(HEARTBEAT_INTERVAL * time.Millisecond)
				args := rf.makeAppendEntriesArg(server, false)
				var reply AppendEntriesReply
				okCh := make(chan bool)
				go func() {
					okCh <- rf.sendAppendEntries(server, args, &reply)
				}()
				select {
					case ok := <- okCh:
						if ok {
							if reply.Term > rf.CurrentTerm {
								if DEBUG{
									rf.Logger.Printf("I %v out of date and become a follower with %v\n", rf.CurrentTerm, reply.Term)
								}
								rf.CurrentTerm = reply.Term
								rf.becomeFollower()
							} else {
								if reply.Success {
									if DEBUG {
										rf.Logger.Printf("add succeed from %v\n", server)
									}
									rf.NextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
									rf.MatchIndex[server] = rf.NextIndex[server] - 1
								} else {
									if DEBUG {
										rf.Logger.Printf("add failed from %v\n", server)
									}
									if reply.ConflictTerm != -1 {
										rf.NextIndex[server] = reply.MayMatchIndex
									} else {
										rf.NextIndex[server] = args.PrevLogIndex
									}
									go rf.sendAppendEntriesAgain(server)
								}
							}

							for N := len(rf.Logs) - 1; N > rf.CommitIndex; N-- {
								if rf.Logs[N].Term != rf.CurrentTerm {
									continue
								}
								cnt := 0
								for i := range rf.peers {
									if rf.MatchIndex[i] >= N {
										cnt++
									}
								}
								if cnt > len(rf.peers)/2 {
									rf.CommitIndex = N
									break
								}
							}
							rf.Apply()
							if DEBUG {
								rf.Logger.Printf("CommitIndex %v\n", rf.CommitIndex)
							}
						}
					case <- timeout.C:
						continue
				}
		}
	}
}

func (rf *Raft) sendAppendEntriesAgain(server int) {
	if rf.Role != LEADER {
		return
	}
	timeout := time.NewTimer(HEARTBEAT_INTERVAL * time.Millisecond)
	args := rf.makeAppendEntriesArg(server, false)
	var reply AppendEntriesReply
	okCh := make(chan bool)
	go func() {
		okCh <- rf.sendAppendEntries(server, args, &reply)
	}()
	select {
		case ok := <- okCh:
			if ok {
				if reply.Term > rf.CurrentTerm {
					if DEBUG{
						rf.Logger.Printf("I %v out of date and become a follower with %v\n", rf.CurrentTerm, reply.Term)
					}
					rf.CurrentTerm = reply.Term
					rf.becomeFollower()
				} else {
					if reply.Success {
						if DEBUG {
							rf.Logger.Printf("add succeed from %v\n", server)
						}
						rf.NextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
						rf.MatchIndex[server] = rf.NextIndex[server] - 1
					} else {
						if DEBUG {
							rf.Logger.Printf("add failed from %v\n", server)
						}
						if reply.ConflictTerm != -1 {
							rf.NextIndex[server] = reply.MayMatchIndex
						} else {
							rf.NextIndex[server] = args.PrevLogIndex
						}
						rf.sendAppendEntriesAgain(server)
					}
				}

				for N := len(rf.Logs) - 1; N > rf.CommitIndex; N-- {
					if rf.Logs[N].Term != rf.CurrentTerm {
						continue
					}
					cnt := 0
					for i := range rf.peers {
						if rf.MatchIndex[i] >= N {
							cnt++
						}
					}
					if cnt > len(rf.peers)/2 {
						rf.CommitIndex = N
						break
					}
				}
				rf.Apply()
				if DEBUG {
					rf.Logger.Printf("CommitIndex %v\n", rf.CommitIndex)
				}
			}
		case <- timeout.C:
			return
	}
}

func (rf *Raft) becomeFollower(){
	rf.Role = FOLLOWER
	rf.VoteFor = -1
	rf.ElectionTimer.Reset(RandomElectionTimeOut())
}

func (rf *Raft) becomeCandidate(){
	rf.Role = CANDIDATE
	rf.VoteFor = -1
	rf.ElectionTimer.Reset(RandomElectionTimeOut())
}

func (rf *Raft) becomeLeader(){
	rf.Role = LEADER
	for server := range rf.peers{
		rf.HBReplyVec[server] = 0
		rf.NextIndex[server] = len(rf.Logs)
		rf.MatchIndex[server] = -1
		rf.AEChans[server] = make(chan bool)
	}
	rf.HBCheckTicker = time.NewTicker(HEARTBEAT_CHECK * time.Millisecond)
}

func (rf *Raft) makeAppendEntriesArg(server int, isHB bool) AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.CurrentTerm
	args.LeaderID = rf.me
	args.PrevLogIndex = rf.NextIndex[server] - 1
	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term
	}
	if !isHB {
		for i := rf.NextIndex[server]; i < len(rf.Logs) ; i++ {
			args.Entries = append(args.Entries, LogEntry{rf.Logs[i].Term, rf.Logs[i].Index, rf.Logs[i].Command})
		}
	}
	args.LeaderCommit = rf.CommitIndex
	return args
}

func (rf *Raft) Apply(){
	if rf.CommitIndex > rf.LastApplied {
		for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
			applyMsg := ApplyMsg{}
			applyMsg.Index = i + 1
			applyMsg.Command = rf.Logs[i].Command
			*rf.applyChan <- applyMsg
		}
		rf.LastApplied = rf.CommitIndex
	}
}