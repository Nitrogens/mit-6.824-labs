package raft

import "time"

const (
	kServerStateFollower  = "follower"
	kServerStateCandidate = "candidate"
	kServerStateLeader    = "leader"
)

const (
	kElectionMinimalTimeLimit = 200
	kElectionMaximalTimeLimit = 499
	kPeriodForNextHeartbeat   = 100 * time.Millisecond
	kRPCWaitTime              = 25 * time.Millisecond
)

const (
	kAppendEntriesStatusOK = 0
	kAppendEntriesStatusIdempotent = 1
	kAppendEntriesStatusTermLatency = 2
	kAppendEntriesStatusLogInconsistency = 3
)
