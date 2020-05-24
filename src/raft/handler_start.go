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
	if rf.dead == 1 || rf.state != kServerStateLeader {
		return index, rf.CurrentTerm, false
	}

	DPrintf("[Id: %+v][State: %+v] Start: %+v\n", rf.me, rf.state, rf)
	newLogEntry := LogEntry{
		Idx:     len(rf.Log),
		Term:    rf.CurrentTerm,
		Command: command,
	}
	index = len(rf.Log)
	rf.Log = append(rf.Log, newLogEntry)
	rf.acceptedCount = append(rf.acceptedCount, 1)
	rf.persist()

	if rf.dead == 1 {
		return -1, rf.CurrentTerm, false
	}

	DPrintf("Start() RETURNED!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!, %+v, %+v, %+v\n", index, rf.CurrentTerm, rf.state == kServerStateLeader)

	return index, rf.CurrentTerm, rf.state == kServerStateLeader
}
