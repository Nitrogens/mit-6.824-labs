package raft

import "sort"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term            int
	Success         bool
	Replied         bool
	StatusCode      int
	AcceptedIndexes []int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		reply.Replied = true
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
	}()
	reply.AcceptedIndexes = make([]int, 0)
	DPrintf("[Id: %+v][State: %+v] AppendEntries received!! %+v", rf.me, rf.state, *args)
	if args.Term < rf.CurrentTerm {
		// The leader is out-of-date
		reply.Success = false
		reply.StatusCode = kAppendEntriesStatusTermLatency
		return
	}
	rf.timeReset()
	if args.Term > rf.CurrentTerm {
		rf.getIntoNewTerm(args.Term)
	} else {
		rf.state = kServerStateFollower
	}
	if args.PrevLogIndex < len(rf.Log) {
		if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.StatusCode = kAppendEntriesStatusLogInconsistency
			return
		}
	} else {
		reply.Success = false
		reply.StatusCode = kAppendEntriesStatusLogInconsistency
		return
	}
	reply.Success = true
	reply.StatusCode = kAppendEntriesStatusOK
	minIdx := 0x3f3f3f3f
	for _, logEntry := range args.Entries {
		if logEntry.Idx < len(rf.Log) {
			if rf.Log[logEntry.Idx].Term != logEntry.Term {
				if logEntry.Idx < minIdx {
					minIdx = logEntry.Idx
				}
			} else {
				reply.StatusCode = kAppendEntriesStatusIdempotent
			}
		}
	}
	if minIdx < 0x3f3f3f3f {
		rf.Log = rf.Log[:minIdx]
		rf.acceptedCount = rf.acceptedCount[:minIdx]
		rf.persist()
	}
	sort.Slice(args.Entries, func(i, j int) bool {
		return args.Entries[i].Idx < args.Entries[i].Idx
	})
	for _, logEntry := range args.Entries {
		if logEntry.Idx == len(rf.Log) {
			rf.Log = append(rf.Log, logEntry)
			rf.acceptedCount = append(rf.acceptedCount, 1)
			reply.AcceptedIndexes = append(reply.AcceptedIndexes, logEntry.Idx)
			rf.persist()
		}
	}
	if args.LeaderCommit > rf.commitIndex && len(rf.Log)-1 >= args.LeaderCommit {
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Broadcast()
	} else if len(rf.Log)-1 < args.LeaderCommit && len(rf.Log)-1 > rf.commitIndex {
		rf.commitIndex = len(rf.Log) - 1
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
