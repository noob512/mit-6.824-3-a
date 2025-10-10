package raft

import (
	"../labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

// 当每个 Raft 节点意识到后续日志条目已被提交时，
// 该节点应通过传给 Make () 函数的 applyCh 通道，
// 向同一服务器上的服务（或测试器）发送 ApplyMsg。
// 将 CommandValid 设为 true，以表明该 ApplyMsg 包含一条新提交的日志条目。
// 在 Lab 3 中，你需要通过 applyCh 发送其他类型的消息（例如快照）；
// 届时你可以为 ApplyMsg 添加新字段，但对于这些其他用途，需将 CommandValid 设为 false。

// labrpc 包模拟了一个不可靠的网络环境：服务器可能无法访问，请求和回复可能丢失。
// Call () 发送请求并等待回复。若在超时时间内收到回复，Call () 返回 true；否则返回 false。因此 Call () 可能需要一段时间才返回。
// 返回 false 可能是由服务器宕机、服务器可达性差、请求丢失或回复丢失等原因导致的。
// 除非服务器端的处理器函数未返回，否则 Call () 保证最终会返回（可能有延迟）。因此无需在 Call () 之外自行实现超时机制。
// 可查看 ../labrpc/labrpc.go 中的注释获取更多细节。

const raftElectionTimeout = 100 * time.Millisecond
const HeartbeatInterval = 40 * time.Millisecond

// 定义Raft节点的三种角色（用整数表示）
const (
	Follower  int = iota // 追随者，值为 0
	Candidate            // 候选者，值为 1（iota 自动递增）
	Leader               // 领导者，值为 2（继续递增）
)

// ApplyMsg 作用：当 Raft 节点的日志条目被提交后，需要通过该结构体将命令发送给上层服务（或测试器）。
// 字段说明：
// 每当有新的日志条目被提交到日志中时，每个 Raft 节点
// 都应向同一服务器中的服务（或测试器）发送一个 ApplyMsg。
// CommandValid：布尔值，标识该消息是否包含一个新提交的日志条目（true 表示有效命令）。
// Command：实际的命令内容（由上层服务传入，如键值对操作）。
// CommandIndex：该命令在日志中的索引（用于上层服务确认命令顺序）。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type one_log struct {
	Cmd   interface{}
	Term  int
	Index int
}

// Raft 一个实现了单个 Raft 节点的 Go 对象。
type Raft struct {
	mu              sync.Mutex          // 保护共享状态的互斥锁
	peers           []*labrpc.ClientEnd // 所有节点的 RPC 端点（包括自身）
	persister       *Persister          // 持久化存储对象（用于崩溃后恢复状态）
	me              int                 // 当前节点在 peers 数组中的索引
	dead            int32               // 标识节点是否已被终止（由 Kill() 设置）
	currentTerm     int                 //服务器见过的最新任期号（首次启动时为 0，单调递增）
	votedFor        int                 //在当前任期中投票给了哪个候选者 ID（若无则为 null）
	logs            []one_log           //日志条目数组；每个条目包含一条状态机命令，以及该条目被领导者接收时的任期号（首条索引为 1）
	commitIndex     int                 // 已提交的最高日志条目的索引（初始为 0）
	lastApplied     int                 // 已应用的最高日志条目的索引（初始为 0）
	nextIndex       []int               // 领导者发送给每个追随者的下一个日志条目的索引（初始为日志长度 + 1）
	matchIndex      []int               // 每个追随者已复制的最高日志条目的索引（初始为 0）
	state           int                 // 节点当前的角色（Follower、Candidate、Leader）
	lastLogIndex    int                 //	最后一条日志的索引
	lastMessageTime time.Time           // 上次收到消息的时间（用于选举超时）
	ElectionTimeout time.Duration       // 选举超时时间（随机值，用于触发选举）
	turnToLeader    int                 //控制initleader只在转换为leader时进行
	applyCh			chan ApplyMsg
	// 待实现的状态（对应论文图 2）
	// 2A 阶段：需要添加与 leader 选举相关的状态（如当前任期、角色、投票记录等）
	// 2B 阶段：添加日志相关状态（日志条目数组、提交索引等）
	// 2C 阶段：添加需要持久化的状态（任期、投票记录等）
}

func (rf *Raft) randomTimeout() time.Duration {
	halfBase := int64(raftElectionTimeout / 2) // 50ms 对应的纳秒数
	offset := rand.Int63n(halfBase)
	return raftElectionTimeout + time.Duration(offset)
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	DPrintf("主机：%d,GetState()启动\n", rf.me)
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

func (rf *Raft) updateTime() {
	rf.lastMessageTime = time.Now()
	rf.ElectionTimeout = rf.randomTimeout()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// // 2C 阶段实现：将持久化状态编码并保存到 persister
// 示例：使用 labgob 编码状态，通过 rf.persister.SaveRaftState() 保存
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
// // 2C 阶段实现：从 persister 读取并解码持久化状态
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
}

// 使用 Raft 的服务（例如键值服务器）希望发起对 “下一条待追加到 Raft 日志的命令” 的共识流程。
// 若当前服务器不是领导者，则返回 false。若为领导者，则启动共识流程并立即返回。
// 无法保证该命令最终一定会被提交到 Raft 日志中 —— 因为领导者可能崩溃或在选举中失利。
// 即使 Raft 实例已终止，此函数也应优雅返回。
//
// 第一个返回值是：若该命令最终被提交，它在日志中会出现的索引。
// 第二个返回值是当前的任期号。
// 第三个返回值是：当前服务器是否认为自己是领导者（若为 true 则是领导者）。
// 作用：上层服务调用该方法，要求 Raft 节点将命令追加到日志并启动共识（让集群多数节点同意该日志）
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		isLeader = false
		DPrintf("主机：%d,Start()不是领导者\n", rf.me)
		return index, term, isLeader
	}
	index = len(rf.logs)
	term = rf.currentTerm
	rf.logs = append(rf.logs, one_log{Cmd: command, Term: term, Index: index})
	DPrintf("主机：%d是leader,日志增加完成\n", rf.me)
	rf.persist()
	return index, term, isLeader
}

// 测试器在每次测试结束后不会终止 Raft 创建的 goroutine，但会调用 Kill () 方法。
// 你的代码可以使用 killed () 来检查 Kill () 是否已被调用。这里使用 atomic 包来避免加锁需求。

// 问题在于，长时间运行的 goroutine 会占用内存并可能消耗 CPU 时间，这或许会导致后续测试失败，
// 并产生令人困惑的调试输出。任何包含长时间运行循环的 goroutine 都应该调用 killed () 来检查自己是否应该停止。

// Kill() 用于终止节点（测试器调用），killed() 用于检查节点是否已终止
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Init() {
	rf.dead = 0
	rf.currentTerm = 1
	rf.votedFor = 0
	rf.logs = make([]one_log, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.state = Follower
	rf.votedFor = -1
	rf.lastMessageTime = time.Now()
	rf.ElectionTimeout = rf.randomTimeout()
	rf.lastLogIndex = len(rf.logs)
	rf.turnToLeader=0
	zero_log := one_log{
    Term:  0,
	}
	rf.logs = append(rf.logs, zero_log)
	DPrintf("主机：%d,RF建立完成\n", rf.me)
}

// Make 当前服务器的端口是 peers [me]。所有服务器的 peers [] 数组顺序相同。
// persister 是当前服务器用于保存其持久化状态的地方，并且初始时可能包含最近保存的状态（如果有的话）。
// applyCh 是一个通道，测试器或服务期望 Raft 通过该通道发送 ApplyMsg 消息。
// Make () 必须快速返回，因此它应该为任何长时间运行的工作启动 goroutine
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.applyCh=applyCh
	rf.me = me
	rf.Init()
	//计划建立两个协程，一个协程定期检查是否超时，如果超时就转变为候选者
	//第二个协程会检查当前是否是leader，如果是leader则定期向所有peer发送心跳
	// Your initialization code here (2A, 2B, 2C).
	go rf.LeaderAction()
	DPrintf("主机：%d,LeaderAction()启动\n", rf.me)
	go rf.FollowerAction()
	DPrintf("主机：%d,FollowerAction()启动\n", rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
