// Leader向peers发送追加日志的命令
func (rf *Raft) leaderSendEntries(serverId int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(serverId, args, &reply)
	// 发送追加日志命令异常
	if !ok {
		return
	}
	// 发送追加日志命令成功
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 比当前Leader的term还大，term异常
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	// term正常
	if args.Term == rf.currentTerm {
		// rules for leader 3.1, 更新matchIndex和nextIndex
		// 复制日志成功
		if reply.Success {
			// matchIndex: 已知复制到该服务器的最高日志条目索引
			match := args.PrevLogIndex + len(args.Entries)
			// nextIndex
			next := match + 1
			// 更新Follower的nextIndex和matchIndex
			rf.nextIndex[serverId] = max(rf.nextIndex[serverId], next)
			rf.matchIndex[serverId] = max(rf.matchIndex[serverId], match)
			DPrintf("[%v]: %v append success next %v match %v", rf.me, serverId, rf.nextIndex[serverId], rf.matchIndex[serverId])
		} else if reply.Conflict {
			DPrintf("[%v]: Conflict from %v %#v", rf.me, serverId, reply)
			// Follower.lastLogIndex < PrevLogIndex
			if reply.XTerm == -1 {
				// 日志缺失，nextIndex设置为Follower的日志条目数量
				rf.nextIndex[serverId] = reply.XLen
			} else {
				// Follower.log.at(args.PrevLogIndex).Term != Leader.PrevLogTerm
				// 即Follower的日志条目中某条日志的prevLogIndex对应的prevLogTerm不一样
				// reply.XTerm为Follower.log[PrevLogIndex].Term
				// Leader找到自己这个Term对应的最后一条日志条目索引
				lastIndexOfXTerm := rf.findLastLogInTerm(reply.XTerm)
				DPrintf("[%v]: lastLogInXTerm %v", rf.me, lastIndexOfXTerm)
				if lastIndexOfXTerm > 0 {
					// 找得到，则直接复制为nextIndex
					rf.nextIndex[serverId] = lastIndexOfXTerm
				} else {
					// Leader日志中不存在这个term，则设置为Follower这个term的第一个日志条目索引
					rf.nextIndex[serverId] = reply.XIndex
				}
			}
			DPrintf("[%v]: leader nextIndex[%v] %v", rf.me, serverId, rf.nextIndex[serverId])
		} else if rf.nextIndex[serverId] > 1 {
			// 如果AppendEntries因为日志不一致而失败：递减NextIndex并重试
			rf.nextIndex[serverId]--
		}
		rf.leaderCommitRule()
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesRequest, response *AppendEntriesResponse) {
	if rf.state == StateLeader && rf.currentTerm == request.Term {
		if response.Success {
			rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.advanceCommitIndexForLeader()
		} else {
			if response.Term > rf.currentTerm {
				rf.ChangeState(StateFollower)
				rf.currentTerm, rf.votedFor = response.Term, -1
				rf.persist()
			} else if response.Term == rf.currentTerm {
				rf.nextIndex[peer] = response.ConflictIndex
				if response.ConflictTerm != -1 {
					firstIndex := rf.getFirstLog().Index
					for i := request.PrevLogIndex; i >= firstIndex; i-- {
						if rf.logs[i-firstIndex].Term == response.ConflictTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
			}
		}
	}
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after handling AppendEntriesResponse %v for AppendEntriesRequest %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), response, request)
}
