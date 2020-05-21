package raft

import "time"

func (rf *Raft) TimeoutChecker() {
	for {
		rf.mu.Lock()
		select {
		case <-rf.doneChan:
			// Return immediately if this goroutine has been killed
			//_, _ = DPrintf("%v: Returned\n", rf.me)
			rf.mu.Unlock()
			return
		default:
			//_, _ = DPrintf("%v: TimeoutChecker\n", rf.me)
			if string(rf.persister.ReadRaftState()) != kServerStateLeader && time.Since(rf.startTime) >= rf.timeLimit {
				// Timeout!
				// Restart the election
				rf.timeReset()
				//_, _ = DPrintf("%v: Timeout\n", rf.me)
				rf.getIntoNewTerm(rf.currentTerm + 1)
				rf.persister.SaveRaftState([]byte(kServerStateCandidate))
				// vote for itself
				rf.votesCount = 1
				rf.votedFor = rf.me
				for idx := range rf.peers {
					if idx == rf.me {
						continue
					}
					go func(id, termId int) {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						//_, _ = DPrintf("%v: Try to send RequestVote %v\n", rf.me, id)

						for {
							if rf.currentTerm > termId {
								return
							}
							//fmt.Printf("%v: LastLogIndex: %+v\n", rf.me, len(rf.log) - 1)
							args := &RequestVoteArgs{
								Term:         rf.currentTerm,
								CandidateId:  rf.me,
								LastLogIndex: len(rf.log) - 1,
								LastLogTerm:  rf.log[len(rf.log)-1].Term,
							}
							reply := &RequestVoteReply{}
							rf.mu.Unlock()

							ok := rf.sendRequestVote(id, args, reply)
							//time.Sleep(kRPCWaitTime) // Wait

							if !ok {
								time.Sleep(kPeriodForNextHeartbeat) // Sleep and retry
								rf.mu.Lock()
								continue
							}

							rf.mu.Lock()
							if rf.currentTerm > termId {
								return
							}
							if reply.Term > rf.currentTerm {
								rf.getIntoNewTerm(reply.Term)
								return
							}
							if reply.VoteGranted {
								rf.votesCount++
							}
							break
						}
					}(idx, rf.currentTerm)
				}
			}
			rf.mu.Unlock()
		}
		time.Sleep(10*time.Millisecond)
	}
}

func (rf *Raft) Work() {
	for {
		rf.mu.Lock()
		_, _ = DPrintf("[Id: %+v][State: %+v] %+v", rf.me, string(rf.persister.raftstate), rf)
		select {
		case <-rf.doneChan:
			// Return immediately if this goroutine has been killed
			rf.mu.Unlock()
			return
		default:
			switch string(rf.persister.ReadRaftState()) {
			case kServerStateCandidate:
				if rf.votesCount > len(rf.peers)/2 {
					rf.persister.SaveRaftState([]byte(kServerStateLeader))
					for idx := range rf.peers {
						rf.nextIndex[idx] = len(rf.log)
						rf.matchIndex[idx] = 0
					}
					for idx := range rf.acceptedCount {
						rf.acceptedCount[idx] = 0
					}
				}
				rf.mu.Unlock()
			case kServerStateLeader:
				for idx := range rf.peers {
					if idx == rf.me {
						continue
					}
					go func(id, termId int) {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						_, _ = DPrintf("[Id: %+v][State: %+v] Try to send AppendEntries %v", rf.me, string(rf.persister.raftstate), id)
						for {
							if rf.currentTerm > termId {
								break
							}
							//fmt.Printf("%v: LastLogIndex: %+v\n", rf.me, len(rf.log) - 1)
							args := &AppendEntriesArgs{
								Term:         rf.currentTerm,
								LeaderId:     rf.me,
								LeaderCommit: rf.commitIndex,
								PrevLogIndex: rf.nextIndex[id] - 1,
								PrevLogTerm:  rf.log[rf.nextIndex[id]-1].Term,
								Entries:      make([]LogEntry, 0),
							}
							if rf.nextIndex[id] < len(rf.log) {
								args.Entries = make([]LogEntry, len(rf.log[rf.nextIndex[id]:]))
								copy(args.Entries, rf.log[rf.nextIndex[id]:])
							}
							reply := &AppendEntriesReply{
								Replied: false,
							}
							rf.mu.Unlock()

							ok := rf.sendAppendEntries(id, args, reply)
							//time.Sleep(kRPCWaitTime) // Wait

							// if get no response from the remote server, retry to send requests
							if !ok {
								_, _ = DPrintf("[Id: %+v][State: %+v] Try to send AppendEntries Failed! [%+v] | [%+v]", rf.me, string(rf.persister.raftstate), args, reply)
								time.Sleep(kPeriodForNextHeartbeat) // Sleep and retry
								rf.mu.Lock()
								continue
							}

							rf.mu.Lock()
							if rf.currentTerm > termId {
								break
							}
							if reply.Term > rf.currentTerm {
								_, _ = DPrintf("[Id: %+v][State: %+v] Try to send AppendEntries Failed! [%+v] | [%+v]", rf.me, string(rf.persister.raftstate), args, reply)
								rf.getIntoNewTerm(reply.Term)
								return
								// rf.timeReset()
							}
							if reply.Success == false {
								_, _ = DPrintf("[Id: %+v][State: %+v] Try to send AppendEntries Failed! [%+v] | [%+v]", rf.me, string(rf.persister.raftstate), args, reply)
								if reply.StatusCode == kAppendEntriesStatusLogInconsistency {
									if rf.nextIndex[id] > 1 {
										rf.nextIndex[id]--
									}
									rf.mu.Unlock()
									time.Sleep(kRPCWaitTime) // Sleep and retry
									rf.mu.Lock()
									continue
								} else {
									break
								}
							} else {
								_, _ = DPrintf("[Id: %+v][State: %+v] Try to send AppendEntries successful! [%+v] | [%+v]", rf.me, string(rf.persister.raftstate), args, reply)
								if len(args.Entries) > 0 {
									rf.matchIndex[id] = args.PrevLogIndex + len(args.Entries)
									rf.nextIndex[id] = rf.matchIndex[id] + 1
									isCommitIndexUpdated := false
									for _, idx := range reply.AcceptedIndexes {
										rf.acceptedCount[idx]++
										if rf.acceptedCount[idx] >= len(rf.peers)/2+1 && idx > rf.commitIndex {
											isCommitIndexUpdated = true
											rf.commitIndex = idx
										}
									}
									if isCommitIndexUpdated {
										rf.applyCond.Broadcast()
									}
								}
								break
							}
						}
					}(idx, rf.currentTerm)
				}
				rf.mu.Unlock()
				time.Sleep(kPeriodForNextHeartbeat)
				continue
			case kServerStateFollower:
				rf.mu.Unlock()
			}
		}
		time.Sleep(10*time.Millisecond)
	}
}

func (rf *Raft) ApplyWork() {
	//rf.applyCond.L.Lock()
	//defer rf.applyCond.L.Unlock()
	for {
		//rf.applyCond.Wait()
		rf.mu.Lock()
		select {
		case <-rf.doneChan:
			// Return immediately if this goroutine has been killed
			//_, _ = DPrintf("%v: Returned\n", rf.me)
			rf.mu.Unlock()
			return
		default:
			//_, _ = DPrintf("%v: ApplyWork\n", rf.me)
			logId := -1
			if rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				logId = rf.lastApplied
			}
			var command interface{}
			if logId > -1 {
				command = rf.log[logId].Command
			}
			applyCh := rf.applyCh
			rf.mu.Unlock()	// unlock now to avoid the block of the channel
			if logId > -1 {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      command,
					CommandIndex: logId,
				}
				applyCh <- applyMsg
			}
		}
		time.Sleep(10*time.Millisecond)
	}
}
