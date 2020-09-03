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
import "math/rand"
import "time"
// import "bytes"
// import "dslabs/mit-go/src/labgob"



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


type ServerState string

const (
	Follower = "follower"
	Candidate = "candidate"
	Leader = "leader"
)
//
// A Go object implementing a single Raft peer.
//
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



	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int

}

// LogEntry ...
type LogEntry struct {
	term int
	command interface{}
}


// BeginElection ...
func (rf *Raft) BeginElection() {
	
	// DPrintf("[%v] is in state %v and voted for %v", rf.me, rf.state, rf.votedFor)
	rf.mu.Lock()
	rf.timeout = resetTimer()
	if (rf.votedFor != -1) {
		rf.votedFor = -1
		rf.mu.Unlock()
		return
	}

	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.timeout = resetTimer()
	votes := 1
	term := rf.currentTerm
	rf.mu.Unlock()


	for server := range rf.peers {

		if (server == rf.me) {
			continue
		}
		
		go func(server int, term int) {
			// DPrintf("%v about to send request vote", rf.me)
			reqVoteArgs := &RequestVoteArgs{
				Term: term,
				CandidateId: rf.me}
			reqReplyArgs := &RequestVoteReply{}
			voteGranted := rf.sendRequestVote(server, reqVoteArgs, reqReplyArgs)

			rf.mu.Lock()
			if (rf.currentTerm < reqReplyArgs.CurrentTerm){
				rf.currentTerm = reqReplyArgs.CurrentTerm
			}
			if (!voteGranted) {
				rf.timeout = resetTimer()
				rf.mu.Unlock()
				return
			}

			votes++
			currVotes := votes
			// DPrintf("[%v] has %v votes", rf.me, votes )
			rf.mu.Unlock()
			if (currVotes > len(rf.peers)/2) {
				
				rf.mu.Lock()
				if rf.currentTerm != term {
					rf.mu.Unlock()
					return
				}
				rf.state = Leader
				DPrintf("[%v] has been elected", rf.me)
				rf.votedFor = -1
				rf.mu.Unlock()
				rf.SendHeartBeats()
			}

		}(server, term)

	}
}

// SendHeartBeats ...
func (rf *Raft) SendHeartBeats() { 
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	for {
		rf.mu.Lock()
		rf.timeout = resetTimer()
		rf.mu.Unlock()
		for server := range rf.peers {
			if (server == rf.me) {
				continue
			}
			go func(server int, term int) {				
				// DPrintf("[%v] is sending hearbeat to %v", rf.me, server)
				appendArgs := &AppendEntriesArgs{
					Term: term,
				}
				appendReplyArgs := &AppendEntriesReply{}
				 rf.sendAppendEntries(server, appendArgs, appendReplyArgs)
				// if (!ok) {
				// 	// DPrintf("[%v] result of hearbeat", ok)
				// 	rf.mu.Lock()
				// 	rf.currentTerm = term
				// 	rf.state = Follower
				// 	rf.timeout = resetTimer()
				// 	rf.mu.Unlock()
				// 	return
				// }
			}(server, term)
		}
		// rf.mu.Lock()
		// if (rf.state != Leader) {
		// 	break
		// }
		time.Sleep(250 * time.Millisecond)
	}
}


// return currentTerm and whether this server
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





type RequestVoteArgs struct {
	Term int
	CandidateId int
}


type RequestVoteReply struct {
	CurrentTerm int
	VoteGranted bool
}


func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("[%v] with current term %v attempting to vote for %v on term %v", rf.currentTerm, rf.me, args.CandidateId, args.Term)
	if (args.Term > rf.currentTerm) {
		// DPrintf("[%v] voted for %v on term %v", rf.me, args.CandidateId, args.Term)
		rf.timeout = resetTimer()
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.CurrentTerm = rf.currentTerm
		reply.VoteGranted = false
	}

}


func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	
	// DPrintf("[%v] is sending request vote to %v", rf.me, server)
	
	peer := rf.peers[server]
	ok := peer.Call("Raft.HandleRequestVote", args, reply)
	return ok && reply.VoteGranted

}

// AppendEntriesArgs ...
type AppendEntriesArgs struct {
	Term int
	LeaderID int
	PrevLogIndex int
	PrevLogTerm int
	// Entries []int
	LeaderCommit int
}

// AppendEntriesReply ...
type AppendEntriesReply struct {
	Term int
	Success bool
}

// HandleAppendEntries ...
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (args.Term < rf.currentTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		rf.currentTerm = args.Term
		reply.Success = true
		rf.state = Follower
		rf.votedFor = -1
		rf.timeout = resetTimer()
	}

}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (int, bool) {
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return reply.Term, ok
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


func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

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
	rf.votedFor = -1
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.timeout = resetTimer()
	rf.state = Follower
	rf.mu.Unlock()

	go func() { 
		for {	
			rf.mu.Lock()
			endTimeInt := rf.timeout
			if (time.Now().After(endTimeInt)) {
				rf.mu.Unlock()
				rf.BeginElection()
				continue
			}
			rf.mu.Unlock()
		}
		time.Sleep(50 * time.Millisecond)
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

// Create random timeout period
func resetTimer() time.Time {
	return time.Now().Add(time.Duration(int64(rand.Intn(300) + 300)) * time.Millisecond)
}


