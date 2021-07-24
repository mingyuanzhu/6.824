package raft

import "log"

// Debugging
const Debug = false
const INFO = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func IPrintf(format string, a ...interface{}) (n int, err error) {
	if INFO {
		log.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}