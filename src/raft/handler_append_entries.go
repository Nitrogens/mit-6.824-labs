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
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	}()
	reply.AcceptedIndexes = make([]int, 0)
	DPrintf("[Id: %+v][State: %+v] AppendEntries received!! %+v", rf.me, string(rf.persister.ReadRaftState()), *args)
	if args.Term < rf.currentTerm {
		// The leader is out-of-date
		reply.Success = false
		reply.StatusCode = kAppendEntriesStatusTermLatency
		return
	}
	rf.timeReset()
	if args.Term > rf.currentTerm {
		rf.getIntoNewTerm(args.Term)
	} else {
		rf.persister.SaveRaftState([]byte(kServerStateFollower))
	}
	if args.PrevLogIndex < len(rf.log) {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
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
		if logEntry.Idx < len(rf.log) {
			if rf.log[logEntry.Idx].Term != logEntry.Term {
				if logEntry.Idx < minIdx {
					minIdx = logEntry.Idx
				}
			} else {
				reply.StatusCode = kAppendEntriesStatusIdempotent
			}
		}
	}
	if minIdx < 0x3f3f3f3f {
		rf.log = rf.log[:minIdx]
		rf.acceptedCount = rf.acceptedCount[:minIdx]
	}
	sort.Slice(args.Entries, func(i, j int) bool {
		return args.Entries[i].Idx < args.Entries[i].Idx
	})
	for _, logEntry := range args.Entries {
		if logEntry.Idx == len(rf.log) {
			rf.log = append(rf.log, logEntry)
			rf.acceptedCount = append(rf.acceptedCount, 1)
			reply.AcceptedIndexes = append(reply.AcceptedIndexes, logEntry.Idx)
		}
	}
	if args.LeaderCommit > rf.commitIndex && len(rf.log)-1 >= args.LeaderCommit {
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Broadcast()
	} else if len(rf.log)-1 < args.LeaderCommit && len(rf.log)-1 > rf.commitIndex {
		rf.commitIndex = len(rf.log) - 1
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
