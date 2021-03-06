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
import "sync/atomic"
import "dslabs/mit-go/src/labrpc"
import "time"
import "bytes"
import "dslabs/mit-go/src/labgob"




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


type ServerState string

// Possible ServerState values
const (
	Follower = "follower"
	Candidate = "candidate"
	Leader = "leader"
)



// Raft ... A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	currentTerm int
	votedFor int
	log []LogEntry
	state ServerState
	timeout time.Time

	applyCh chan ApplyMsg

	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int

	
}

// LogEntry ...
type LogEntry struct {
	Term int
	Command interface{}
}


// BeginElection ...
func (rf *Raft) BeginElection() {

	rf.mu.Lock()
	// DPrintf("[%v] is starting an election", rf.me)
	rf.timeout = resetTimer()
	if (rf.killed() || rf.votedFor != -1 ) {
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()
		return
	}

	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()
	rf.timeout = resetTimer()
	votes := 1
	term := rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := -1
	if (lastLogIndex != -1) {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	rf.mu.Unlock()


	for server := range rf.peers {

		if (server == rf.me) {
			continue
		}
		
		go func(server int, term int, lastLogIndex int, lastLogTerm int) {
			reqVoteArgs := &RequestVoteArgs{
				Term: term,
				CandidateId: rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm: lastLogTerm,
			}
			reqReplyArgs := &RequestVoteReply{}
			rf.sendRequestVote(server, reqVoteArgs, reqReplyArgs)

			rf.mu.Lock()
			if (term < reqReplyArgs.CurrentTerm){
				rf.turnToFollower(reqReplyArgs.CurrentTerm)
				rf.persist()
				rf.mu.Unlock()
				return
			}

			if (!reqReplyArgs.VoteGranted || votes > len(rf.peers)/2 ) {
				rf.timeout = resetTimer()
				rf.mu.Unlock()
				return
			}

			votes++
			currVotes := votes
			rf.mu.Unlock()
			if (currVotes > len(rf.peers)/2 && rf.state != Follower) {
				rf.mu.Lock()
				rf.timeout = resetTimer()
				if rf.currentTerm != term {
					rf.turnToFollower(rf.currentTerm)
					rf.persist()
					rf.mu.Unlock()
					return
				}

				rf.state = Leader
				DPrintf("[%v] has been elected on term %v", rf.me, term)
				rf.mu.Unlock()
				rf.SendHeartBeats()
			}

		}(server, term, lastLogIndex, lastLogTerm)

	}
}


// SendHeartBeats ...
func (rf *Raft) SendHeartBeats() { 

	rf.mu.Lock()
	rf.nextIndex = initializeIndexSlice(len(rf.peers), len(rf.log))
	term := rf.currentTerm
	rf.mu.Unlock()

	for {
		rf.mu.Lock()
		// DPrintf("[%v] is sending heartbeart", rf.me)
		rf.timeout = resetTimer()
		if (rf.killed()) {
			rf.persist()
			break
		}
		rf.mu.Unlock()

		for server := range rf.peers {
			if (server == rf.me) {
				continue
			}
			go func(server int, term int) {	
				rf.SendAppendRPC(server, term)
			}(server, term)
		}

		rf.mu.Lock()
		if (rf.state != Leader) {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		time.Sleep(105 * time.Millisecond)
	}
}

// SendAppendRPC ... Structure and send append RPCs
func (rf *Raft) SendAppendRPC(server int, term int) {
		rf.mu.Lock()
		prevLogIndex := rf.nextIndex[server] - 1
		prevLogTerm := -1
		entries := []LogEntry{}
		savedCurrentTerm := rf.currentTerm
		if (prevLogIndex != -1) {
			prevLogTerm = rf.log[prevLogIndex].Term
			entries = rf.log[prevLogIndex+1:]
		} else if (prevLogIndex == -1 && len(rf.log) > 0) {
			entries = rf.log
		}

		appendArgs := &AppendEntriesArgs{
			Term: term,
			LeaderID: rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm: prevLogTerm,
			LeaderCommit: rf.commitIndex,
			Entries: entries,
		}	
		rf.mu.Unlock()	
		appendReplyArgs := &AppendEntriesReply{}
		rf.sendAppendEntries(server, appendArgs, appendReplyArgs)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if (appendReplyArgs.Term > term) {
			rf.turnToFollower(appendReplyArgs.Term)
			rf.persist()
			return
		}

		if (appendReplyArgs.Success) {
			rf.nextIndex[server] = prevLogIndex + 1 + len(entries)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		
		for i := rf.commitIndex + 1; i < len(rf.log); i++ {
			if (rf.log[i].Term == savedCurrentTerm) {
				numReplicated := 1
				for peerID := range rf.peers {
					if rf.matchIndex[peerID] >= i {
						numReplicated++
					}
				}
				// DPrintf("num replicated is %v and commit index is %v and last applied is %v", numReplicated, rf.commitIndex, rf.lastApplied)
				if (numReplicated > len(rf.peers)/2) {
					rf.commitIndex = i
					if (rf.commitIndex > rf.lastApplied) {
						// go func(lastApplied, commitIndex int) {
							// rf.mu.Lock()
							// defer rf.mu.Unlock()
							for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
								applyIdx := i
								applyMsg := ApplyMsg{
									CommandValid: true,
									Command: rf.log[applyIdx].Command,
									CommandIndex: applyIdx + 1,
								}
								DPrintf("Leader [%v] applying %v", rf.me, applyMsg)
									rf.applyCh <- applyMsg
									rf.lastApplied++
							}
						// 	return
						// }(rf.lastApplied, rf.commitIndex)
					}
				}
			}
		}

	} else {
		if (prevLogIndex > -1 && len(entries) != 0) {
			if (appendReplyArgs.XTerm == -1) {
				rf.nextIndex[server] = appendReplyArgs.XIndex
			} else {
				firstOccurenceOfXTerm := rf.FindFirstIndexOfTerm(appendReplyArgs.XTerm)
				if (firstOccurenceOfXTerm == -1) {
					rf.nextIndex[server] = appendReplyArgs.XIndex
				} else {
					rf.nextIndex[server] = firstOccurenceOfXTerm + 1
				}
			}
		}
	}
	
}


// GetState returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}


// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.

type PersistedState struct {
	CurrentTerm int
	Log []LogEntry
	VotedFor int 

}
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	PersistedState := &PersistedState{
		CurrentTerm: rf.currentTerm,
		Log: rf.log,
		VotedFor: rf.votedFor,
	}
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.log)
	// e.Encode(rf.votedFor)
	e.Encode(*PersistedState)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}



// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// var currentTerm int
	// var log []LogEntry
	// var votedFor int
	var PersistedState PersistedState
	if d.Decode(&PersistedState) != nil  {
		DPrintf("Error reading from persisted state")
	} else {
	  rf.currentTerm = PersistedState.CurrentTerm
	  rf.log = PersistedState.Log
	  rf.votedFor = PersistedState.VotedFor
	}
}





type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
	LogLength int
}


type RequestVoteReply struct {
	CurrentTerm int
	VoteGranted bool
}


func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.votedFor != -1 {
		return 
	}

	// current server last log idx and last log term
	csLastLogIndex := len(rf.log) - 1
	csLastLogTerm := -1
	if (csLastLogIndex != -1) {
		csLastLogTerm = rf.log[csLastLogIndex].Term
	}

	if (args.Term >= rf.currentTerm) {
		// DPrintf("incoming args.LastLogTerm from [%v] is %v and %v last term is %v", args.CandidateId,args.LastLogTerm, rf.me, csLastLogTerm)
		rf.turnToFollower(args.Term)
		if ((args.LastLogIndex == -1 && csLastLogIndex == -1) ||
		args.LastLogTerm > csLastLogTerm || (args.LastLogTerm == csLastLogTerm && args.LastLogIndex >= csLastLogIndex)) {
			// DPrintf("[%v] voted for %v on term %v", rf.me, args.CandidateId, args.Term)
			rf.votedFor = args.CandidateId
			rf.persist()
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	}
	reply.CurrentTerm = rf.currentTerm
}


func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	
	// DPrintf("[%v] is sending request vote to %v", rf.me, server)
	peer := rf.peers[server]
	ok := peer.Call("Raft.HandleRequestVote", args, reply)
	return ok

}

// AppendEntriesArgs ...
type AppendEntriesArgs struct {
	Term int
	LeaderID int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

// AppendEntriesReply ...
type AppendEntriesReply struct {
	Term int
	Success bool
	XTerm int
	XIndex int
	Xlen int
}

// HandleAppendEntries ...
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.timeout = resetTimer()


	reply.Success = false
	if (args.Term < rf.currentTerm) {
		reply.Term = rf.currentTerm
		return
	} 

	rf.turnToFollower(args.Term)
	if (args.PrevLogIndex == -1 || (args.PrevLogIndex < len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term)) {
		reply.Success = true
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		rf.votedFor = args.LeaderID
		rf.persist()

		rf.applyCommand(args.LeaderCommit, args.PrevLogIndex, args.Entries)
		return
	} else {
		if (args.PrevLogIndex >= len(rf.log)) {
			reply.XTerm = -1
			reply.XIndex  = len(rf.log)
		} else {
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			reply.XIndex = rf.FindFirstIndexOfTerm(reply.XTerm)
		}
		return
	}		
	
}

func (rf *Raft) applyCommand(leaderCommitIdx int, leaderPrevLogIndex int, entries []LogEntry) {

	if (leaderCommitIdx > rf.commitIndex && leaderCommitIdx < len(rf.log)) {
		// rf.commitIndex = int(math.Min(float64(leaderCommitIdx), float64(len(rf.log)-1)))
		savedCommitIdx := rf.commitIndex
		for i := savedCommitIdx+1; i <= leaderCommitIdx; i++ {	
			rf.timeout = resetTimer()
			// DPrintf("[%v] log is %v and is is about to apply %v at at index %v and current CI is %v", rf.me, rf.log, rf.log[i].Command, i, i-1)
			applyIdx := i
			applyMsg := ApplyMsg{
				CommandValid: true,
				CommandIndex: applyIdx + 1,
			}
			applyMsg.Command = rf.log[applyIdx].Command
			rf.commitIndex = applyIdx
			rf.applyCh <- applyMsg
		}
		rf.lastApplied++
	}
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}





// Start ... the service using Raft (e.g. a k/v server) wants to start
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
	defer	rf.mu.Unlock()
	index := 0
	term := rf.currentTerm
	isLeader := rf.state == Leader
	if (isLeader) {
		index = len(rf.log) + 1
		logEntry := LogEntry{
			Term: term,
			Command: command,
		}
		rf.log = append(rf.log, logEntry)
		// DPrintf("[%v] Started receiving command %v and leaders log is currently %v", rf.me, logEntry.Command, rf.log)
		rf.timeout = resetTimer()
		rf.persist()
		for server := range rf.peers {
			if (server == rf.me) {
				continue
			}
			go func(server int, term int) {	
				rf.SendAppendRPC(server, term)
			}(server, term)
		}
	}
	return index, term, isLeader
}


func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
		// Your initialization code here (2A, 2B, 2C).

	rf := &Raft{}
	rf.mu.Lock()
	rf.applyCh = applyCh
	rf.votedFor = -1
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.matchIndex = initializeIndexSlice(len(peers), -1)
	rf.currentTerm = 0
	rf.timeout = resetTimer()
	rf.state = Follower
	rf.lastApplied = -1
	rf.commitIndex = -1
	rf.readPersist(persister.ReadRaftState())  	// initialize from state persisted before a crash
	rf.mu.Unlock()

	go func() { 
		for {	
			rf.mu.Lock()
			if (time.Now().After(rf.timeout)) {
				rf.mu.Unlock()
				rf.BeginElection()
				continue
			}
			rf.mu.Unlock()
			time.Sleep(30 * time.Millisecond)
		}
	}()

	return rf
}





