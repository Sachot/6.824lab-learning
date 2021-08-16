package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
//import "bytes"
import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	FOLLOWER int = 0//-1
	CANDIDATE int = 1//0
	LEADER int = 2//1
)

const (
	electionTimeoutMin = 200
	electionTimeoutMax = 800
	heartbeatInterval  = 90 * time.Millisecond  // 心跳间隔
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//persitent state on all server(持续的状态)
	currentTerm int
	votedFor  int

	logs      []Log

	//volatile state on all server(不稳定状态)
	commitIndex int //已知需要提交的最新日志索引
	lastApplied int //应用到状态机的最新日志的索引
	role int

	//volatile state on leaders (选举后重新初始化)
	nextIndex []int
	matchIndex []int


	//voteNum int // 记录选票数
	heartbeatsCheck bool  // 检测有无收到心跳
	randomTimeout int // 随机超时时间
	timerReset bool

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg

}

type Log struct {
	//INDEX int //index
	TERM int //term
	//KEY string // key
	//VALUE string //value
	COMMAND interface{}
}

func (rf *Raft) String() string {
	return fmt.Sprintf("[%d:%d;Term:%d;VotedFor:%d;logLen:%v;Commit:%v;Apply:%v]",
		rf.role, rf.me, rf.currentTerm, rf.votedFor, len(rf.logs), rf.commitIndex, rf.lastApplied)
}

func (rf *Raft) getRandTimeout(min, max int) int {
	random := rand.Intn(max-min) + min
	return random
}

func (rf *Raft) checkElectionLoop() {
	for !rf.killed() {
		A:
		checkCount := 10
		divDuration := (electionTimeoutMin + rand.Intn(4) * 100) / checkCount
		//i := rf.getRandTimeout(electionTimeoutMin, electionTimeoutMax)
		//time.Sleep(time.Duration(rf.randomTimeout) * time.Millisecond)
		//time.Sleep(time.Duration(electionTimeoutMin + rand.Intn(4) * 100) * time.Millisecond)
		//fmt.Printf("主机：%d,term:%d,commitIndex:%d,lastApplied:%d\n",rf.me,rf.currentTerm,rf.commitIndex,rf.lastApplied)
		if rf.role == LEADER {
			continue
		}

		for checkIndex:=0;checkIndex<checkCount;checkIndex++ {
			time.Sleep(time.Millisecond * time.Duration(divDuration))
			rf.mu.Lock()
			if rf.timerReset {
				rf.timerReset = false
				rf.mu.Unlock()
				goto A
				//break
			}
			rf.mu.Unlock()
		}

		fmt.Printf("主机:%d,term:%d,role:%v,len(logs):%v,HB为false,变为candidate\n",rf.me,rf.currentTerm,rf.role,len(rf.logs))
		rf.mu.Lock()
		rf.turnCandidate()
		rf.persist()
		rf.mu.Unlock()
		// 开始选举
		go rf.sendRequestVote(rf.me, rf.currentTerm)  // 选举超时需要自增term，重新发起选举
		//fmt.Printf("主机:%v, role:%v, term:%v, 选举完成\n", rf.me, rf.role, rf.currentTerm)
	}
}

func (rf *Raft) pingLoop() {
	for !rf.killed(){
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.SendAppendEntries(rf.currentTerm)
		time.Sleep(heartbeatInterval)
	}
}

// 将已经 commit 的 log 不断应用到状态机
func (rf *Raft) applyLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			rf.apply(rf.lastApplied, rf.logs[rf.lastApplied], rf.applyCh)
		}
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) apply(lastApplied int, entry Log, applyCh chan ApplyMsg) {
	//fmt.Printf("主机:%d, lastApplied:%d, commitIndex:%d, applying, log content:%v\n",rf.me,rf.lastApplied,rf.commitIndex,entry.COMMAND)
	msg := ApplyMsg{}
	msg.Command = entry.COMMAND
	msg.CommandIndex = lastApplied
	msg.CommandValid = true

	applyCh <- msg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool

	// Your code here (2A).
	term = rf.currentTerm
	if rf.role == LEADER {
		isLeader = true
	}else {
		isLeader = false
	}

	return term, isLeader
}

func (rf *Raft) turnCandidate() {
	rf.role = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	//fmt.Println("turn to Candidate:"+strconv.Itoa(rf.me)+"当前term："+strconv.Itoa(rf.currentTerm))
}

func (rf *Raft) turnLeader() {
	rf.role = LEADER
	rf.timerReset = true
	fmt.Printf("new leader:%d,term:%d,role:%v\n",rf.me,rf.currentTerm,rf.role)
	for i:=0;i<len(rf.peers);i++ {
		rf.matchIndex[i]=0
		rf.nextIndex[i] = len(rf.logs)
	}
	go rf.pingLoop()
	//fmt.Println("leader's term:"+ strconv.Itoa(rf.currentTerm))
	//DPrintf("no.%d change to leader!", rf.me)
}

func (rf *Raft) turnFollower(term int) {
	rf.role = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	//fmt.Println("turn to Follower:"+strconv.Itoa(rf.me)+"当前term："+strconv.Itoa(rf.currentTerm))
}

func notLessUpToDate(currTerm, currIndex int, dstTerm, dstIndex int) bool {
	if currTerm != dstTerm {
		return currTerm > dstTerm
	}

	return currIndex >= dstIndex
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	err1 := e.Encode(rf.currentTerm)
	if err1 != nil {
		fmt.Printf("主机%v,term:%v,encode currentTerm error:%v\n",rf.me,rf.currentTerm,err1)
	}

	err2 := e.Encode(rf.votedFor)
	if err2 != nil {
		fmt.Printf("主机%v,term:%v,encode votedFor error:%v\n",rf.me,rf.currentTerm,err2)
	}

	err3 := e.Encode(rf.logs)
	if err3 != nil {
		fmt.Printf("主机%v,term:%v,encode logs error:%v\n",rf.me,rf.currentTerm,err3)
	}

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm1 int
	var voteFor1 int
	var logs1 []Log

	err1 := d.Decode(&currentTerm1)
	if err1 != nil {
		fmt.Printf("readPersist,decode currentTerm err:%v\n",err1)
	}

	err2 := d.Decode(&voteFor1)
	if err2 != nil {
		fmt.Printf("readPersist,decode voteFor err:%v\n",err2)
	}

	err3 := d.Decode(&logs1)
	if err3 != nil {
		fmt.Printf("readPersist,decode logs err:%v\n",err3)
	}

	if err1==nil && err2==nil && err3==nil {
		rf.currentTerm = currentTerm1
		rf.votedFor = voteFor1
		rf.logs = logs1
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	TERM int
	CANDIDATEID int
	LASTLOGINDEX int
	LASTLOGTERM int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	TERM int  //  返回currentTerm用于candidate更新
	VOTEGRANTED bool // true表示获得了选票
}

type AppendEntriesArgs struct {
	TERM int // leader的term
	LEADERID int  // leaderId
	PREVLOGINDEX int // prevLogIndex, 此次追加请求的上一个日志的索引
	PREVLOGTERM int // prevLogTerm, 此次追加请求的上一个日志的任期
	ENTRIES []Log // entries, 追加的日志（空则为心跳请求）
	LEADERCOMMIT int // leaderCommit, Leader上已经Commit的Index
}

type AppendEntriesReply struct {
	TERM int // 接收者的currentTerm，用于leader更新状态
	SUCCESS bool // success, 如果Follower节点匹配prevLogIndex和prevLogTerm，返回true
}
/*
接收者实现逻辑
1.返回false，如果收到的任期比当前任期小
2.返回false，如果不包含之前的日志条目（没有匹配prevLogIndex和prevLogTerm）
3.如果存在index相同但是term不相同的日志，删除从该位置开始所有的日志
4.追加所有不存在的日志
5.如果leaderCommit>commitIndex，将commitIndex设置为commitIndex = min(leaderCommit, index of last new entry)
 */

//
// example RequestVote RPC handler.
// 接收选举请求
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("第%v号收到第%v的选举请求,term:%v,logs长度:%v,lastApplied:%v,args-term:%v,args-lastindex:%v,lastlogterm:%v\n",rf.me,args.CANDIDATEID,rf.currentTerm,len(rf.logs),rf.lastApplied,args.TERM,args.LASTLOGINDEX,args.LASTLOGTERM)
	// Your code here (2A, 2B).

	if args.TERM < rf.currentTerm { // 接收到的term比自己的term小
		reply.TERM = rf.currentTerm // candidate接收到reply，发现自己term比别人低，立刻变回follower
		reply.VOTEGRANTED = false
		return
	}else if args.TERM > rf.currentTerm {  // 接收到的term比自己的term大
		reply.TERM = rf.currentTerm

		if args.LASTLOGTERM < rf.logs[len(rf.logs)-1].TERM  { //candidate没有本机的日志up-to-date
			DPrintf("error1,触发选举限制1,term:%v,args-term:%v,节点:%v拒绝向主机:%v投票,lastindex:%v,lastlogterm:%v\n",rf.currentTerm,args.TERM,rf.me,args.CANDIDATEID,args.LASTLOGINDEX,args.LASTLOGTERM)
			reply.VOTEGRANTED = false
			rf.mu.Lock()
			rf.turnFollower(args.TERM)
			rf.persist()
			rf.timerReset = true
			rf.mu.Unlock()
			return
		} else if args.LASTLOGTERM == rf.logs[len(rf.logs)-1].TERM && (args.LASTLOGINDEX < len(rf.logs)-1) {
			DPrintf("error2,触发选举限制2,term:%v,args-term:%v,节点:%v拒绝向主机:%v投票,lastindex:%v,lastlogterm:%v\n",rf.currentTerm,args.TERM,rf.me,args.CANDIDATEID,args.LASTLOGINDEX,args.LASTLOGTERM)
			reply.VOTEGRANTED = false
			rf.mu.Lock()
			rf.turnFollower(args.TERM)
			rf.persist()
			rf.timerReset = true
			rf.mu.Unlock()
			return
		} else {       // 投票成功
			//reply.TERM = rf.currentTerm
			reply.VOTEGRANTED = true
			rf.mu.Lock()
			rf.turnFollower(args.TERM)
			rf.timerReset = true
			rf.votedFor = args.CANDIDATEID
			rf.persist()
			rf.mu.Unlock()
			return
		}
	}else if args.TERM == rf.currentTerm {
		if rf.votedFor == -1 || rf.votedFor == args.CANDIDATEID {

			if args.LASTLOGTERM < rf.logs[len(rf.logs)-1].TERM { //candidate没有本机的日志up-to-date
				DPrintf("error3,触发选举限制3,term:%v,args-term:%v,节点:%v拒绝向主机:%v投票,lastindex:%v,lastlogterm:%v\n",rf.currentTerm,args.TERM,rf.me,args.CANDIDATEID,args.LASTLOGINDEX,args.LASTLOGTERM)
				reply.VOTEGRANTED = false
				return
			} else if args.LASTLOGTERM == rf.logs[len(rf.logs)-1].TERM && (args.LASTLOGINDEX < len(rf.logs)-1) {
				DPrintf("error4,触发选举限制4,term:%v,args-term:%v,节点:%v拒绝向主机:%v投票,lastindex:%v,lastlogterm:%v\n",rf.currentTerm,args.TERM,rf.me,args.CANDIDATEID,args.LASTLOGINDEX,args.LASTLOGTERM)
				reply.VOTEGRANTED = false
				return
			} else {       // 投票成功
				reply.TERM = rf.currentTerm
				reply.VOTEGRANTED = true
				rf.mu.Lock()
				rf.turnFollower(args.TERM)
				rf.timerReset = true
				rf.votedFor = args.CANDIDATEID
				rf.persist()
				rf.mu.Unlock()
				return
			}
		}
		reply.TERM = rf.currentTerm
		reply.VOTEGRANTED = false
		return
	}
	/*reply.TERM = rf.currentTerm
	reply.VOTEGRANTED = false
	return*/
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// 发起选举
//
/*func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.countRequestVoteReply(reply)
	}
	return ok
}*/

func (rf *Raft) sendRequestVote(server int, term int) {
	// var wg sync.WaitGroup
	//fmt.Println("进入sendRequestVote:"+ strconv.Itoa(rf.me))
	count := 1   // 先给自己投一票
	finish := 1
	rf.mu.Lock()
	rf.timerReset = true
	rf.mu.Unlock()


	//cond := sync.NewCond(&rf.mu)
	cond := sync.NewCond(new(sync.Mutex))

	for i:=0; i< len(rf.peers); i++ {
		if i==rf.me {
			continue
		}
		// wg.Add(1)
		go func(x int) bool {
			//fmt.Println(strconv.Itoa(rf.me)+"号，向"+strconv.Itoa(x)+"发送request")
			args := RequestVoteArgs{}
			reply := RequestVoteReply{}
			//args.term = rf.currentTerm
			//args.candidateId = rf.me
			args.TERM = term
			args.CANDIDATEID = server
			args.LASTLOGTERM = rf.logs[len(rf.logs)-1].TERM
			args.LASTLOGINDEX = len(rf.logs)-1
			ok := rf.peers[x].Call("Raft.RequestVote", &args, &reply)
			//fmt.Printf("ok1: %t\n",ok)
			if ok {
				DPrintf("主机:%d,role:%v,主机term:%d,响应方:%v, 响应term:%d,reply-voteGranted:%t\n",rf.me,rf.role,rf.currentTerm,x,reply.TERM,reply.VOTEGRANTED)
				rf.mu.Lock()
				//defer rf.mu.Unlock()
				if reply.VOTEGRANTED {
					count++
				}
				finish++
				if reply.TERM > rf.currentTerm {
					DPrintf("主机%v,term:%v,reply-term:%v,sendRequestVote(),出现reply.TERM > rf.currentTerm\n",rf.me,rf.currentTerm,reply.TERM)
					voteFor1 := rf.votedFor
					rf.turnFollower(reply.TERM)
					rf.votedFor = voteFor1
					rf.persist()
					rf.timerReset = true
				}
				cond.L.Lock()
				cond.Broadcast()
				cond.L.Unlock()
				rf.mu.Unlock()
			}else {
				DPrintf("主机:%d没有得到%v响应,term:%v,len(logs):%v\n",server, x, term,len(rf.logs))
				rf.mu.Lock()
				finish++
				cond.L.Lock()
				cond.Broadcast()
				cond.L.Unlock()
				rf.mu.Unlock()
			}

			// wg.Done()
			//fmt.Println("选举请求发送完成:"+ strconv.Itoa(rf.me))
			//fmt.Printf("选举请求发送完成,主机:%d,term:%d,目标主机:%d\n",rf.me,rf.currentTerm,x)
			return ok
		}(i)
	}

	//rf.mu.Lock()
	// Golang整除向下取整
	for count <= (len(rf.peers)/2) && finish != len(rf.peers) {
		if rf.role==FOLLOWER {
			//rf.mu.Unlock()
			return
		}
		cond.L.Lock()
		cond.Wait()
		cond.L.Unlock()
	}
	//fmt.Printf("主机:%d,当前term:%d,finish: %d\n",rf.me,rf.currentTerm,finish)
	if count > (len(rf.peers)/2) {
		// double check
		if rf.role != CANDIDATE || rf.currentTerm != term {
			//fmt.Printf("当前任期选举失败,term:"+ strconv.Itoa(term))
			//rf.mu.Unlock()
			return
		}
		// 选举成功
		rf.mu.Lock()
		rf.turnLeader()
		rf.mu.Unlock()
		return
	} else {
		// 失败
		DPrintf("election fail--peer:%d,选举term:%v,当前term:%v,len(logs):%v\n",rf.me,term,rf.currentTerm,len(rf.logs))
		//rf.mu.Unlock()
		return
	}
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Println("第"+strconv.Itoa(rf.me)+"收到hb，来自"+ strconv.Itoa(args.LEADERID)+"，当前term："+strconv.Itoa(rf.currentTerm)+" leaderTerm:"+strconv.Itoa(args.TERM))
	if (*args).TERM < rf.currentTerm {
		(*reply).TERM = rf.currentTerm
		(*reply).SUCCESS = false
		return
	}else {
		rf.mu.Lock()
		rf.timerReset = true
		rf.mu.Unlock()
		if args.TERM > rf.currentTerm {
			rf.mu.Lock()
			voteFor := rf.votedFor
			rf.turnFollower(args.TERM)
			rf.votedFor = voteFor
			rf.persist()
			rf.mu.Unlock()
		}else if rf.role == CANDIDATE && args.TERM == rf.currentTerm {
			DPrintf("主机：%v,term: %v, CANDIDATE恢复Follower\n",rf.me, rf.currentTerm)
			rf.mu.Lock()
			rf.turnFollower(args.TERM)
			rf.votedFor = rf.me
			rf.persist()
			rf.mu.Unlock()
		}else if rf.role == LEADER && args.TERM == rf.currentTerm {
			fmt.Printf("error, 两个leader\n")
		}

		if args.PREVLOGINDEX == -1 {
			rf.logs = append(rf.logs, args.ENTRIES...)
			rf.persist()
		}else if args.PREVLOGINDEX>=0 {
			// 日志不匹配
			if args.PREVLOGINDEX >= len(rf.logs) || rf.logs[args.PREVLOGINDEX].TERM != args.PREVLOGTERM {
				DPrintf("主机:%v, 日志不匹配,term:%v,len(logs):%v\n",rf.me, rf.currentTerm,len(rf.logs))
				if args.PREVLOGINDEX < len(rf.logs) {
					rf.logs = rf.logs[0:args.PREVLOGINDEX] // delete the log in prevLogIndex and after it
					rf.persist()
				}
				(*reply).TERM = rf.currentTerm
				(*reply).SUCCESS = false
				return
			}

			rf.logs = append(rf.logs[0:args.PREVLOGINDEX+1],args.ENTRIES...)
			rf.persist()
		}

		DPrintf("AE,主机:%v, role:%v,  args.PREVLOGINDEX:%v, logs长度:%v, term:%v\n", rf.me, rf.role, args.PREVLOGINDEX, len(rf.logs), rf.currentTerm)
		if args.LEADERCOMMIT > rf.commitIndex {
			//prevIndex := rf.commitIndex
			rf.commitIndex = minInt(args.LEADERCOMMIT, len(rf.logs))
		}

		//fmt.Printf("主机:%v, commitIndex:%v, term:%v\n",rf.me, rf.commitIndex, rf.currentTerm)
		(*reply).TERM = rf.currentTerm
		(*reply).SUCCESS = true
		return
	}
}

func minInt(a,b int) int {
	if a<=b {
		return a
	}else{
		return b
	}
}

func getMajorityAgreementIndex(matchIndex []int) int {
	//DPrintf("getMajorityAgreementIndex()---------------")
	tmp := make([]int, len(matchIndex))
	copy(tmp, matchIndex)
	sort.Ints(tmp)
	index := len(tmp)/2
	return tmp[index]
}

func (rf *Raft) SendAppendEntries(term int) {
	//var wg sync.WaitGroup
	for i:=0; i< len(rf.peers); i++ {
		if i==rf.me {
			rf.nextIndex[rf.me] = len(rf.logs)
			rf.matchIndex[rf.me] = len(rf.logs)-1
			continue
		}
		//wg.Add(1)
		go func(x, y int) bool {
			if rf.role != LEADER {
				return false
			}
			args := AppendEntriesArgs{}
			reply := AppendEntriesReply{}
			prevLogIndex := rf.nextIndex[x]-1
			args.TERM = y
			args.LEADERID = rf.me
			args.LEADERCOMMIT = rf.commitIndex
			if prevLogIndex >= 0 {
				args.PREVLOGINDEX = prevLogIndex
				args.PREVLOGTERM = rf.logs[prevLogIndex].TERM
				args.ENTRIES = rf.logs[rf.nextIndex[x]:]
			}else {
				DPrintf("prevLogIndex为: %v", prevLogIndex)
				args.PREVLOGINDEX = prevLogIndex
				args.PREVLOGTERM = -1
				if len(rf.logs) >0 {
					args.ENTRIES = rf.logs[0:]
				}
			}
			DPrintf("主机%v,role:%v,发送AE至%v,term:%v, nextIndex[x]:%v, prevLogIndex:%v, logs长度:%v\n",rf.me,rf.role, x, rf.currentTerm, rf.nextIndex[x], args.PREVLOGINDEX, len(rf.logs))
			ok := rf.peers[x].Call("Raft.AppendEntries", &args, &reply)
			if ok {
				/*if y < rf.currentTerm {
					wg.Done()
					return ok
				}*/
				if (&reply).TERM > rf.currentTerm {
					DPrintf("主机%v,term:%v,得到reply-term:%v,恢复Follower\n",rf.me, rf.currentTerm,reply.TERM)
					rf.mu.Lock()
					voteFor1 := rf.votedFor
					rf.turnFollower(reply.TERM)
					rf.votedFor = voteFor1
					rf.timerReset = true
					rf.persist()
					rf.mu.Unlock()
					//wg.Done()
					return ok
				}else{
					if rf.role != LEADER || rf.currentTerm != args.TERM {
						DPrintf("主机:%v is not leader, 当前role:%v, currentTerm为:%d, args.term为: %v\n", rf.me, rf.role, rf.currentTerm, args.TERM)
						//wg.Done()
						return ok
					}
					if rf.role == LEADER && rf.currentTerm == args.TERM {
						if (&reply).SUCCESS {
							//DPrintf("AE-reply-success, 主机:%d, 当前term:%d\n",rf.me,rf.currentTerm)
							rf.matchIndex[x] = args.PREVLOGINDEX + len(args.ENTRIES)
							rf.nextIndex[x] = rf.matchIndex[x] + 1
							rf.mu.Lock()
							majorAgreementIndex := getMajorityAgreementIndex(rf.matchIndex)
							rf.mu.Unlock()
							DPrintf("主机:%v,term:%v, role:%v, x:%v, matchIndex[x]:%v, nextIndex[x]:%v, majorAgreementIndex:%v\n",rf.me, rf.currentTerm, rf.role,x,rf.matchIndex[x],rf.nextIndex[x], majorAgreementIndex)

							// 这里每次提交到状态机时对应最后一条日志term必须等于leader当前的term
							if majorAgreementIndex >= 0 && len(rf.logs)>majorAgreementIndex && rf.logs[majorAgreementIndex].TERM == rf.currentTerm && majorAgreementIndex > rf.commitIndex {
								rf.commitIndex = majorAgreementIndex
								//fmt.Printf("主机:%d, 当前term:%d, 更新commitIndex为:%d\n",rf.me,rf.currentTerm,rf.commitIndex)
							}
						}else if args.PREVLOGINDEX > 0 { // 发现没有匹配成功，直接跳到前一个term再试探，提高试探速度
							prevIndex := args.PREVLOGINDEX
							for prevIndex>0 && rf.logs[prevIndex].TERM==args.PREVLOGTERM {
								prevIndex--
							}
							rf.nextIndex[x] = prevIndex + 1
							DPrintf("主机:%v,term:%v,len(logs):%v,更新对x:%v的nextIndex:%v\n",rf.me,rf.currentTerm,len(rf.logs),x,rf.nextIndex[x])
						}else if args.PREVLOGINDEX == 0 {
							rf.nextIndex[x] = 0
						}else {
							//fmt.Printf("PREVLOGINDEX=-1，出现reply-fail情况，leader:%v,follower:%v",rf.me,x)
						}
					}else {
						//fmt.Printf("prevLogIndex为: %v", prevLogIndex)
					}
				}
			}
			//wg.Done()
			return ok
		}(i,term)
	}
	//wg.Wait()
}


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
	DPrintf("Start()-------------------")
	index := -1
	term := -1
	isLeader := false

	if rf.role == LEADER {
		DPrintf("Start()--leader:%d-------------",rf.me)
		log := Log{COMMAND: command, TERM: rf.currentTerm}
		rf.mu.Lock()
		rf.logs = append(rf.logs, log)
		rf.persist()
		index = len(rf.logs)-1 //第一个index是0，论文要求1
		term = rf.currentTerm
		isLeader = true
		fmt.Printf("leader:%d,term:%v new log:%v, len(logs):%v\n",rf.me, rf.currentTerm, log.COMMAND, len(rf.logs))
		rf.mu.Unlock()
		return index, term, isLeader
	}

	// Your code here (2B).
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
// peers是网络标识符数组，用于RPC；me是peers数组中，自己所在的索引号；
// persisted：持久的，persister：持久性
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	DPrintf("DPrintf-----------------\n")
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	//rf.role = FOLLOWER   // 重新加入集群后可能不是follower
	//rf.logs

	rf.commitIndex = 0
	rf.lastApplied = -1
	rf.matchIndex = []int{0,0,0,0,0,0,0,0,0,0,0}
	rf.nextIndex = []int{0,0,0,0,0,0,0,0,0,0,0}
	rf.matchIndex = rf.matchIndex[0:len(peers)]
	rf.nextIndex = rf.nextIndex[0:len(peers)]
	log := Log{}
	var command interface{} = 1
	log.COMMAND = command
	log.TERM = -1
	rf.logs = append(rf.logs, log)
	//rf.persist()


	//rf.randomTimeout = rf.getRandTimeout(electionTimeoutMin, electionTimeoutMax)
	rf.heartbeatsCheck = false
	rf.timerReset = false
	fmt.Printf("7.7\n")
	//rf.nextIndex
	//rf.matchIndex


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.checkElectionLoop()
	go rf.applyLoop()
	return rf
}
