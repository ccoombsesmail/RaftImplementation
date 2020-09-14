package raft

import (
	"log"
	"time"
	"math/rand"

)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


func (rf *Raft) FindFirstIndexOfTerm(term int) int {
		for i := 0 ; i < len(rf.log); i++ {
			if rf.log[i].Term == term {
				return i
			}
		}
		return -1
}


func (rf *Raft) turnToFollower(term int) {
		rf.currentTerm = term
		rf.state = Follower
		rf.timeout = resetTimer()
}


// Create random timeout period
func resetTimer() time.Time {
	return time.Now().Add(time.Duration(int64(rand.Intn(300) + 300)) * time.Millisecond)
}

// Initialize nextIndex or matchIndex with a certain value
func initializeIndexSlice(size int, value int) []int {
	if (value == -1) {
		value = 0
	}
	slice := make([]int, size)
	for i := 0; i < size; i++ {
		slice[i] = value
	}
	return slice

}



