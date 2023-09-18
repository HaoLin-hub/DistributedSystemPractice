package raft

import (
	"6.5840/labgob"
	"bytes"
	"log"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type logEntry struct {
	index   int
	term    int
	command interface{}
}

// example RequestVote RPC arguments structure. field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm int
	CandidateId   int
	LastLogIndex  int
	LastLogTerm   int
}

// example RequestVote RPC reply structure. field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 返回receiver当前的任期，若是比candidate要大，则candidate更新至term
	VoteGranted bool // 若是选择将投票给candidate，则返回true
}

type AppendEntriesArgs struct {
	leaderTerm   int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []logEntry // 需要同步的日志
	leaderCommit int        // Leader已提交的最高日志索引(即已被多数节点复制成功的最大索引的日志)
}

type AppendEntriesReply struct {
	Term          int
	Sucess        bool
	ConflictTerm  int // 发生冲突的日志的任期号
	ConflictIndex int // Follower中任期为conflictTerm的第一条日志所在的槽位号, 若缺失日志则表示的是第一个空白槽位号
}

type InstallSnapshotArgs struct {
	LeaderTerm        int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// 枚举raft节点所处的状态
type nodeState int

const (
	Follower nodeState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logs        []logEntry

	commitIndex int // 此项开始的都是易失的数据
	lastApplied int // 任意时刻, lastApplied <= commitIndex

	nextIndex  []int // leader才有的状态
	matchIndex []int

	// 论文中没说的工程细节，由我追加的
	curState       nodeState
	electionTimer  *time.Timer // 选举超时的时间应是随机的，这样才能够保证论文中所说的尽量避免split vote的case
	heartbeatTimer *time.Timer // 心跳发送的频率应是固定的，根据lab的提示，每秒发送心跳不能超过十次

	applyCh        chan ApplyMsg // 为每个新提交的日志条目 发送一个ApplyMsg到Make()的applyCh通道参数。
	applyCond      *sync.Cond    // 条件变量，用于等待一个或一组goroutines满足条件后唤醒的场景,实现线程同步
	replicatorCond []*sync.Cond  // 条件变量数组，为每个peer建立一个复制线程, 满足"相应的条件变量"后才能运行
}

func RandomElectionOutDateTime() int {
	t := 600 + rand.Intn(300)
	return t
}

func HeartbeatSendTime() int {
	return 100
}

func min(num1 int, num2 int) int {
	if num1 > num2 {
		return num2
	}
	return num1
}

func max(num1 int, num2 int) int {
	if num1 > num2 {
		return num1
	}
	return num2
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.curState == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

func (rf *Raft) getLastLog() logEntry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) findLastEntryForTerm(term int) int {
	lastIncludeIndex := rf.logs[0].index
	idx := rf.getLastLog().index
	for idx >= lastIncludeIndex && rf.logs[idx-lastIncludeIndex].term != term {
		idx--
	}
	if idx < lastIncludeIndex {
		return -1
	}
	return idx
}

func (rf *Raft) appendNewLogEntry(command interface{}) logEntry {
	newLog := logEntry{
		index:   rf.getLastLog().index + 1,
		term:    rf.currentTerm,
		command: command,
	}
	rf.logs = append(rf.logs, newLog)
	rf.persist()
	return newLog
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []logEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		log.Fatalf("Node %v 恢复失败", rf.me)
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.commitIndex = rf.logs[0].index
		rf.lastApplied = rf.logs[0].index
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIdx := rf.logs[0].index
	if index <= snapshotIdx {
		DPrintf("Node %v 拒绝在日志槽位号%v处做快照, 因为当前节点备份的快照更加新", rf.me, index)
		return
	}
	if index > rf.getLastLog().index {
		DPrintf("Node %v 拒绝在日志槽位号%v处做快照, 因为该槽位号越界", rf.me, index)
		return
	}
	// 做快照: 截断日志
	tempLog := make([]logEntry, len(rf.logs)-index+snapshotIdx)
	copy(tempLog, rf.logs[index-snapshotIdx:])
	rf.logs = tempLog
	rf.logs[0].command = nil
	rf.persist()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 若是收到候选者任期小于自己的任期，可以明确知道候选者已经过时了，不可能投票给它，可直接返回
	if args.CandidateTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// 而如果是相等
	if rf.currentTerm == args.CandidateTerm {
		// 则判断votedFor != -1和args.candidateId, 说明当前自己已经把当前任期currentTerm的选票投给其他候选人了
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		} else {
			// 票还没投出去 或 已经投给了args.candidateId,但由于网络原因丢失了reply, 现在又被重新请求
			// 遵照Figure2中所说还要判断一下请求候选者的日志至少跟自己一样新
		}
	}
	// 根据Figure2中的Rules for Servers所示，当收到候选者的任期大于自己的任期时，及时更新自己的任期，并转换为Follower状态
	if args.CandidateTerm > rf.currentTerm {
		rf.curState = Follower
		rf.currentTerm = args.CandidateTerm
		rf.votedFor = -1
		rf.persist()
	}
	// 下面正式开始判断是否应该投票给请求候选者（走到这步。说明候选者的任期 >= 自己的任期，那么其实就是判断谁的日志较新）
	myLastLogTerm := rf.getLastLog().term
	myLastLogIndex := rf.getLastLog().index
	if args.CandidateTerm > myLastLogTerm || (args.CandidateTerm == myLastLogTerm && args.LastLogIndex > myLastLogIndex) {
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.electionTimer.Reset(time.Duration(RandomElectionOutDateTime()) * time.Millisecond)
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}
	// 否则，不能投票
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
}

// Follower收到AppendEntries RPC后的动作
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.leaderTerm < rf.currentTerm {
		// 收到过时的AppendEntries消息
		reply.Sucess = false
		reply.Term = rf.currentTerm
		return
	}
	rf.curState = Follower
	// 重置选举超时定时器
	rf.electionTimer.Reset(time.Duration(RandomElectionOutDateTime()) * time.Millisecond)
	/* 下面处理arg.leaderTerm >= rf.currentTerm  */
	if args.leaderTerm > rf.currentTerm {
		// 说明当前节点落后了，需要更新自己的当前任期号
		rf.currentTerm = args.leaderTerm
		rf.votedFor = -1
		rf.persist()
	}
	// 如果leader发来的prevLogIndex小于当前跟随者的lastIncludeIndex, 说明当前跟随者缺失那些“已被leader删除了的entry”或者有冲突
	if args.prevLogIndex <= rf.logs[0].index {
		reply.Term = 0
		reply.Sucess = false
		return
	}

	if args.prevLogIndex > rf.getLastLog().index { // 第一种情况: 当前节点缺失日志
		reply.Term = rf.currentTerm
		reply.Sucess = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.getLastLog().index + 1
		DPrintf("Node [%v]: 缺失了日志, 第一个空白槽位号为 %v", rf.me, reply.ConflictIndex)
		return
	} else if args.prevLogTerm != rf.logs[args.prevLogIndex-rf.logs[0].index].term { // 第二种情况: 同一个索引处的日志出现冲突
		reply.Term = rf.currentTerm
		reply.ConflictTerm = rf.logs[args.prevLogIndex-rf.logs[0].index].term
		// 找到当前follower中任期为reply.conflictTerm的第一条日志所在槽位
		idx := args.prevLogIndex
		for idx >= rf.logs[0].index && rf.logs[idx-rf.logs[0].index].term == reply.ConflictTerm {
			idx--
		}
		reply.ConflictIndex = idx + 1
		DPrintf("Node [%v]: 发现了冲突日志, ConflictTerm为 %v, ConflictIndex为 %v", rf.me, reply.ConflictTerm, reply.ConflictIndex)
		reply.Sucess = false
		return
	}
	// 下面处理Leader的prevIndex处的日志与当前节点在prevIndex处的日志相匹配的情况
	lastIncludeIndex := rf.logs[0].index
	for idx, entry := range args.entries {
		if entry.index <= rf.getLastLog().index && entry.term != rf.logs[entry.index-lastIncludeIndex].term {
			// appendEntrirs发来的日志条目符合要求,逐步截断prevIndex后面的日志项
			rf.logs = rf.logs[:entry.index-lastIncludeIndex]
			rf.persist()
		} else if entry.index > rf.getLastLog().index {
			rf.logs = append(rf.logs, args.entries[idx:]...)
			DPrintf("Node %v 作为follower同步了leader的日志 [%v]", rf.me, args.entries[idx:])
			rf.persist()
			break
		}
	}
	reply.Term = rf.currentTerm
	reply.Sucess = true

	// 实现Figure2中AppendEntries RPC的第(5)项
	if args.leaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.leaderCommit, rf.getLastLog().index)
		rf.applyCond.Signal()
	}
}

// Follower收到InstallSnapshot RPC后的动作
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.LeaderTerm < rf.currentTerm {
		return
	} else if args.LeaderTerm > rf.currentTerm {
		rf.currentTerm = args.LeaderTerm
		rf.votedFor = -1
		rf.persist()
	}
	rf.curState = Follower
	rf.electionTimer.Reset(time.Duration(RandomElectionOutDateTime()) * time.Millisecond)

	// Figure13规则5: 判断leader发送来的快照是否过时（可能被重传或者延误）
	if args.LastIncludedIndex <= rf.logs[0].index {
		DPrintf("installSnapshot消息包含的快照旧于当前节点%v的已有快照", rf.me)
		return
	} else if args.LastIncludedIndex <= rf.lastApplied { // 表明leader发来的快照虽然比当前节点已有的快照新，但当前节点的状态机已经比发来的快照新，仅更新快照即可
		rf.Snapshot(args.LastIncludedIndex, args.Data)
	} else { // leader发来的快照不仅比当前节点新，还比状态机新，此时需要保存快照并更新状态机
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = max(args.LastIncludedIndex, rf.commitIndex)
		// 规则8: 将快照应用到状态机
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			Command:       nil,
			CommandIndex:  0,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		// 规则6: 判断快照的最后一个日志项与当前节点中相应的日志项是否匹配
		if args.LastIncludedIndex <= rf.getLastLog().index && rf.logs[args.LastIncludedIndex-rf.logs[0].index].term == args.LastIncludedTerm {
			// 匹配，简单更新一下快照即可
			rf.Snapshot(args.LastIncludedIndex, args.Data)
		} else {
			// 不匹配，则日志项冲突，至少到args.lastIncludeIndex的日志都冲突，丢弃当前节点的整个日志,直接将args.lastIncludeIndex+1作为与同步的起点
			rf.logs = make([]logEntry, 1)
			rf.logs[0] = logEntry{
				index:   args.LastIncludedIndex,
				term:    args.LastIncludedTerm,
				command: nil,
			}
			// 直接将persist()的代码黏贴到这
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(rf.currentTerm)
			e.Encode(rf.votedFor)
			e.Encode(rf.logs)
			raftstate := w.Bytes()
			rf.persister.Save(raftstate, args.Data)
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	return ok
}

func (rf *Raft) leaderSendInstallSnapshot() {

}

func (rf *Raft) leaderSendAppendEntries(heartbeat bool) {
	for peerID, _ := range rf.peers {
		if peerID == rf.me {
			continue
		}
		if heartbeat {
			// 需要给所有节点发送心跳包以维持领导力
			go rf.replicateEntries(peerID)
		} else {
			// 唤醒”peerID对应的节点“的复制线程，令其开始发送复制日志消息
			rf.replicatorCond[peerID].Signal()
		}
	}
}

func (rf *Raft) replicateEntries(peerID int) {
	rf.mu.RLock()
	if rf.curState != Leader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peerID] - 1
	// 判断prevIndex是否小于lastIncludeIndex, 即leader已删除了跟随者需要的日志
	if prevLogIndex <= rf.logs[0].index {
		// 直接发送自己(leader)的快照给跟随者
		reqArgs := InstallSnapshotArgs{
			LeaderTerm:        rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.logs[0].index,
			LastIncludedTerm:  rf.logs[0].term,
			Data:              rf.persister.ReadSnapshot(),
		}
		reply := new(InstallSnapshotReply)
		DPrintf("发送InstallSnapshot 给 Node %v, currentTerm为 %v, snapshot index为 %v, snapshot term为 %v", peerID, rf.currentTerm, rf.logs[0].index, rf.logs[0].term)
		// 发送InstallSnapshot RPC
		rf.mu.RUnlock()
		ok := rf.sendInstallSnapshot(peerID, &reqArgs, reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 防止收到过期的RPC回复, 判断一下当前任期是否与当初发送AppendEntries RPC时的任期一致
			if rf.curState == Leader && rf.currentTerm == reqArgs.LeaderTerm {
				// 试图更新leader的nextIndex[peerID] (有可能peerID对应的follower全部日志被快照替换)
				rf.nextIndex[peerID] = max(rf.nextIndex[peerID], reqArgs.LastIncludedIndex+1)
				rf.matchIndex[peerID] = max(rf.matchIndex[peerID], reqArgs.LastIncludedIndex)
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.curState = Follower
					rf.votedFor = -1
					rf.electionTimer.Reset(time.Duration(RandomElectionOutDateTime()) * time.Millisecond)
					rf.persist()
				}
			}
		}
		return
	}
	// 下面是发送同步复制日志RPC给follower
	requestArgs := AppendEntriesArgs{
		leaderTerm:   rf.currentTerm,
		leaderId:     rf.me,
		prevLogIndex: prevLogIndex,
		prevLogTerm:  rf.logs[prevLogIndex].term,
		leaderCommit: rf.commitIndex,
	}
	// 判断将要发送给Follower的日志条目有哪些
	if rf.nextIndex[peerID] <= rf.getLastLog().index {
		// nextIndex[peerID]是逻辑上的日志槽位号，由于做了快照，实际对应的槽位号应该是nextIndex - lastIncludeIndex
		requestArgs.entries = rf.logs[rf.nextIndex[peerID]-rf.logs[0].index:]
	}
	rf.mu.RUnlock()
	reply := new(AppendEntriesReply)
	// 发送AppendEntries RPC给peerID对应的节点
	ok := rf.sendAppendEntries(peerID, &requestArgs, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 防止收到过期的RPC回复, 判断一下当前任期是否与当初发送AppendEntries RPC时的任期一致
	if rf.curState == Leader && rf.currentTerm == requestArgs.leaderTerm {
		if reply.Sucess == true {
			// 向peerID节点复制日志条目成功，更新matchIndex 和 nextIndex
			rf.matchIndex[peerID] = requestArgs.prevLogIndex + len(requestArgs.entries)
			rf.nextIndex[peerID] = rf.matchIndex[peerID] + 1
			DPrintf("Node %v 成功将log entry复制到 Node %v, 更新matchIndex为: %v, nextIndex为: %v",
				rf.me, peerID, rf.matchIndex[peerID], rf.nextIndex[peerID])

		} else {
			// 判断未能复制成功的原因
			if reply.Term > rf.currentTerm { // 原因一: (可能出现了网络分区，导致自己一直认为自己是leader)
				rf.curState = Follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.electionTimer.Reset(time.Duration(RandomElectionOutDateTime()) * time.Millisecond)
				rf.persist()
			} else if reply.ConflictTerm == -1 { // 原因二: 跟随者peerID缺失日志
				rf.nextIndex[peerID] = reply.ConflictIndex
			} else if reply.ConflictTerm != -1 { // 原因三: 在prevIndex处的日志条目与跟随者产生冲突
				lastIndexForTerm := rf.findLastEntryForTerm(reply.ConflictTerm)
				if lastIndexForTerm != -1 {
					rf.nextIndex[peerID] = lastIndexForTerm + 1
				} else {
					rf.nextIndex[peerID] = reply.ConflictIndex
				}
			}
		}
		rf.tryUpdateCommitIndex()
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1       // 命令提交后出现的索引
	term := -1        // 当前任期
	isLeader := false // 当前节点是否认为自己是leader

	// Your code here (2B).
	if rf.curState == Leader {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		newLog := rf.appendNewLogEntry(command)
		DPrintf("Node %v 在任期 %v 收到了客户端发来的一条新命令: [%v]", rf.me, rf.currentTerm, newLog)
		// 发送复制请求AppendEntries给所有节点
		rf.leaderSendAppendEntries(false)
		// 更改
		index = newLog.index
		term = newLog.term
		isLeader = true
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A) Check if a leader election should be started.
		select {
		// 如果心跳定时器结束，那么就发送一个心跳包给所有peers
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.curState == Leader {
				// 发送心跳包给所有节点

				// 重设心跳计时器
				rf.heartbeatTimer.Reset(time.Duration(HeartbeatSendTime()) * time.Millisecond)
			}
		// 选举定时器超时，转换为候选者状态并请求投票
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.curState = Candidate
			rf.currentTerm += 1
			rf.persist()
			rf.startElection()
			// 尝试成为leader过后, 无论成功与否都重置选举定时器
			rf.electionTimer.Reset(time.Duration(RandomElectionOutDateTime()) * time.Millisecond)
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350 milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	// 初始化一个请求参数
	reqArgs := RequestVoteArgs{
		CandidateId:   rf.me,
		CandidateTerm: rf.currentTerm,
		LastLogIndex:  rf.getLastLog().index,
		LastLogTerm:   rf.getLastLog().term,
	}
	getVoteNums := 1
	rf.votedFor = rf.me
	DPrintf("Node %v 开始选举，发送RequestVote请求: %v", rf.me, reqArgs)
	rf.persist()
	// 遍历集群上的每一个服务器，向它们发送投票请求
	for peerID, _ := range rf.peers {
		if peerID == rf.me {
			continue
		}
		// 并发请求投票,可提升效率
		go func(peerID int) {
			reply := new(RequestVoteReply)
			ok := rf.sendRequestVote(peerID, &reqArgs, reply)
			if ok { // 如果收到了回复
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("Node %v 在任期%v发送了投票请求: %v后，收到了Node %v的回复: %v", rf.me, rf.currentTerm, reqArgs, peerID, reply)
				// 这里正如guidence中所说，（可能因为网络延迟）收到了过时的rpc回复，要忽略,因此只处理正确的rpc回复
				//(即在发送rpc投票请求之时的任期与收到回复时的自己的任期一致)
				if rf.currentTerm == reqArgs.CandidateTerm && rf.curState == Candidate {
					if reply.VoteGranted {
						// 票是投给自己的
						getVoteNums += 1
						if getVoteNums > len(rf.peers)/2 {
							DPrintf("Node %v 在任期 %v 收到了集群大多数服务器的投票", rf.me, rf.currentTerm)
							rf.curState = Leader
							// 向其他服务器发送心跳，宣示权威

						}
					} else if reply.Term > rf.currentTerm {
						// 说明自己已经过时了，连有任期更高的节点都不知道,在当前任期内退位
						DPrintf("Node %v 找到了具有任期 %v 的Node %v, 从任期 %v 内退位", rf.me, reply.Term, peerID, rf.currentTerm)
						rf.curState = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
					}
				} else {
					// 收到过时rpc, 则什么也不做
				}
			}
		}(peerID)

	}
}

func (rf *Raft) tryUpdateCommitIndex() {
	// 遵循Figure2中AllServer->leader规则:如果存在一个N > commitIndex,且过半服务器的matchIndex >= N, 且要求log[N].term 与 curTerm一致
	if rf.curState == Leader {
		for i := rf.commitIndex + 1; i <= rf.getLastLog().index; i++ {
			if rf.logs[i-rf.logs[0].index].term != rf.currentTerm {
				continue
			}
			haveReplicatedCount := 1 // 当前节点已将logEntry复制到本地
			for peerID := 0; peerID < len(rf.peers); peerID++ {
				if peerID != rf.me && rf.matchIndex[peerID] >= i {
					haveReplicatedCount++
				}
			}
			// 判断是否有过半服务器添加entry成功
			if haveReplicatedCount > len(rf.peers)/2 {
				rf.commitIndex = i
				DPrintf("leader Node %v 尝试提交index 成功 %v", rf.me, rf.commitIndex)
				// 启动apply线程，将日志项应用到状态机
				rf.applyCond.Signal()
			}
		}
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 只要当前节点还没挂
	for !rf.killed() {
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		// 获取[lastApplied, CommitIndex]范围的日志
		entries := rf.logs[rf.lastApplied+1-rf.logs[0].index : rf.commitIndex+1-rf.logs[0].index]
		commitIndexBeforePushChannel := rf.commitIndex // (记录发送应用状态之前的commitIndex)
		rf.mu.Unlock()                                 // !! 将消息发送到applych的过程中不能上锁
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.command,
				CommandIndex: entry.index,
			}
		}
		rf.mu.Lock() // 重新上锁
		rf.lastApplied = commitIndexBeforePushChannel
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()

	// 只要服务节点没挂(实现论文中5.3节第一段所述, 若复制日志条目失败将无限重试)
	for !rf.killed() {
		// 判断是否需要向peer节点发送复制日志RPC, 不需要的话就继续阻塞
		needReplicate := rf.curState == Leader && rf.matchIndex[peer] < rf.getLastLog().index
		if !needReplicate {
			rf.replicatorCond[peer].Wait()
		}
		// 否则开始发送复制消息
		rf.replicateEntries(peer)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.curState = Follower
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.votedFor = -1

	rf.logs = make([]logEntry, 1) // 初始化一个raft节点时，第一个log保存的是snapshot元信息
	rf.logs[0].index = 0
	rf.logs[0].term = 0
	rf.logs[0].command = nil

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// 设心跳发送频率固定为100毫秒一次，而选举超时时间则为600~900毫秒之间随机
	rf.heartbeatTimer = time.NewTimer(time.Duration(HeartbeatSendTime()) * time.Millisecond)
	rf.electionTimer = time.NewTimer(time.Duration(RandomElectionOutDateTime()) * time.Millisecond)
	rf.applyCh = applyCh
	rf.replicatorCond = make([]*sync.Cond, len(peers))
	rf.applyCond = sync.NewCond(&rf.mu)

	// 初始化为节点崩溃前的状态（如果之前发生过崩溃的话）
	rf.readPersist(persister.ReadRaftState())

	// 继续初始化matchIndex、nextIndex和replicatorCond[.../ rf.me]
	lastLog := rf.logs[len(rf.logs)-1]
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = lastLog.index + 1
		// 为除自己外的所有节点分别启动一个复制线程，管理复制流程，直到满足条件才运行
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(i)
		}
	}

	// 开启定时器线程, 以发送心跳和appendEntries(前提: 自己是leader)
	go rf.ticker()
	// 为自己开启一个apply线程,用于将已提交的日志应用到状态机.(要满足一定条件才可运行，否则阻塞在条件变量applyCond)
	go rf.applier()

	return rf
}
