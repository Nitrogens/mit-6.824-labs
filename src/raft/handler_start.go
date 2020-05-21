package raft

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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Redirect to the leader if the current peer is not a leader
	if rf.dead == 1 || string(rf.persister.raftstate) != kServerStateLeader {
		return index, rf.currentTerm, false
	}

	DPrintf("[Id: %+v][State: %+v] Start: %+v\n", rf.me, string(rf.persister.raftstate), rf)
	newLogEntry := LogEntry{
		Idx:     len(rf.log),
		Term:    rf.currentTerm,
		Command: command,
	}
	index = len(rf.log)
	rf.log = append(rf.log, newLogEntry)
	rf.acceptedCount = append(rf.acceptedCount, 1)

	//for idx := range rf.peers {
	//	if idx == rf.me {
	//		continue
	//	}
	//	go func(id, termId, index int) {
	//		rf.mu.Lock()
	//		defer rf.mu.Unlock()
	//		_, _ = DPrintf("[Id: %+v][State: %+v] Try to send AppendEntries %v\n", rf.me, string(rf.persister.raftstate), id)
	//		for {
	//			if rf.dead == 1 || rf.currentTerm > termId || index-1 >= len(rf.log) || index >= len(rf.log) {
	//				return
	//			}
	//			args := &AppendEntriesArgs{
	//				Term:         rf.currentTerm,
	//				LeaderId:     rf.me,
	//				PrevLogIndex: index - 1,
	//				PrevLogTerm:  rf.log[index-1].Term,
	//				Entries:      []LogEntry{rf.log[index]},
	//				LeaderCommit: rf.commitIndex,
	//			}
	//			reply := &AppendEntriesReply{}
	//			rf.mu.Unlock()
	//
	//			ok := rf.sendAppendEntries(id, args, reply)
	//			//time.Sleep(kRPCWaitTime) // Wait
	//
	//			// if get no response from the remote server, retry to send requests
	//			if !ok {
	//				time.Sleep(kPeriodForNextHeartbeat) // Sleep and retry
	//				rf.mu.Lock()
	//				continue
	//			}
	//
	//			rf.mu.Lock()
	//			if rf.currentTerm > termId {
	//				break
	//			}
	//			if reply.Term > rf.currentTerm {
	//				rf.getIntoNewTerm(reply.Term)
	//				return
	//				// rf.timeReset()
	//			}
	//			if reply.Success == false {
	//				if reply.StatusCode == kAppendEntriesStatusLogInconsistency {
	//					if rf.nextIndex[id] > 1 {
	//						rf.nextIndex[id]--
	//					}
	//					rf.mu.Unlock()
	//					time.Sleep(kRPCWaitTime) // Sleep and retry
	//					rf.mu.Lock()
	//					continue
	//				} else {
	//					break
	//				}
	//			} else {
	//				// success
	//				rf.acceptedCount[index]++
	//				if rf.acceptedCount[index] >= len(rf.peers)/2+1 {
	//					if index > rf.commitIndex {
	//						rf.commitIndex = index
	//						rf.applyCond.Broadcast()
	//					}
	//				}
	//				if len(args.Entries) > 0 {
	//					rf.matchIndex[id] = args.PrevLogIndex + len(args.Entries)
	//					rf.nextIndex[id] = rf.matchIndex[id] + 1
	//				}
	//				break
	//			}
	//		}
	//	}(idx, rf.currentTerm, index)
	//}

	if rf.dead == 1 {
		return -1, rf.currentTerm, false
	}

	DPrintf("Start() RETURNED!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!, %+v, %+v, %+v\n", index, rf.currentTerm, string(rf.persister.raftstate) == kServerStateLeader)

	return index, rf.currentTerm, string(rf.persister.raftstate) == kServerStateLeader
}
