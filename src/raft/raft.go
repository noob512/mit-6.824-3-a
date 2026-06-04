package raft

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

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

const raftElectionTimeout = 240 * time.Millisecond
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
// CommandIndex：该命令在提交后日志中的索引（用于上层服务确认命令顺序）（现在我们在底层日志中加入了nil日志）。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type one_log struct {
	Cmd   interface{}
	Term  int
	Index int
	//true_index int
	Committed bool//其实可以舍弃，但为了方便调试保留
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
	committed       []bool
	commitIndex     int                 // 已提交的最高日志条目的索引（初始为 0）
	lastApplied     int                 // 已应用的最高日志条目的索引（初始为 0）
	nextIndex       []int               // 领导者发送给每个追随者的下一个日志条目的索引（初始为日志长度 + 1）
	matchIndex      []int               // 每个追随者已复制的最高日志条目的索引（初始为 0）
	state           int                 // 节点当前的角色（Follower、Candidate、Leader）
	lastLogIndex    int                 //	最后一条日志的索引
	lastMessageTime time.Time           // 上次收到消息的时间（用于选举超时）
	ElectionTimeout time.Duration       // 选举超时时间（随机值，用于触发选举）
	turnToLeader    int                 //控制initleader只在转换为leader时进行
	applyCh			chan ApplyMsg		//用于向上层提交命令
	MaxnilNum       int					//最大空白命令，用于用户在向主机提交日志时，返回正确的索引
	CurnilNum       int					//当前空白命令，用于主机向上层提交日志时确定真实索引
	commitCond       *sync.Cond
	// 待实现的状态（对应论文图 2）
	// 2A 阶段：需要添加与 leader 选举相关的状态（如当前任期、角色、投票记录等）
	// 2B 阶段：添加日志相关状态（日志条目数组、提交索引等）
	// 2C 阶段：添加需要持久化的状态（任期、投票记录等）
}

//随机产生一个超时时间
func (rf *Raft) randomTimeout() time.Duration {
	halfBase := int64(raftElectionTimeout / 2) // 50ms 对应的纳秒数
	offset := rand.Int63n(halfBase)
	return raftElectionTimeout + time.Duration(offset)
}

//2A时实现，用于确定当前是否是leader
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	DPrintf("主机：%d,GetState()启动\n", rf.me)
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//更新收到消息的时间（如果超时了是要选举的）
func (rf *Raft) updateTime() {
	rf.lastMessageTime = time.Now()
	rf.ElectionTimeout = rf.randomTimeout()
}


// // 2C 阶段实现：将持久化状态编码并保存到 persister
// 示例：使用 labgob 编码状态，通过 rf.persister.SaveRaftState() 保存
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.logs)        // ✅ 
	//e.Encode(rf.committed)
    data := w.Bytes()
    rf.persister.SaveRaftState(data)  // ✅ 完全覆盖之前的内容
}

// readPersist 是 Raft 实现持久化恢复的核心方法。
// 它的作用是当节点重启（Crash & Restart）时，从底层的持久化存储器（Persister）
// 中读取之前保存的二进制状态快照，并将其还原到 Raft 对象的内存内存字段中，
// 从而确保节点能够“找回记忆”，维持分布式系统的连续性。
//
// 参数：
//   data: 从 persister.ReadRaftState() 获取的原始二进制数据流。
// func (rf *Raft) readPersist(data []byte) {
//     // 1. 基础校验：如果 data 为空（例如节点第一次启动），则无需执行任何恢复逻辑，直接跳出。
//     if data == nil || len(data) < 1 { 
//         return
//     }

//     // 2. 创建读取缓冲区 (Buffer)：将 byte 数组转换成一个可流式读取的 Reader 对象，
//     // 供后续的 gob 解码器使用。
//     r := bytes.NewBuffer(data)
    
//     // 3. 初始化 labgob 解码器：labgob 是 MIT 为此课程封装的 go 标准库 encoding/gob 的包装器，
//     // 它能将内存中的数据结构自动序列化为字节流，并在此处反序列化恢复。
//     d := labgob.NewDecoder(r)
    
//     // 4. 定义用于承接恢复数据的临时中间变量。
//     // 注意：变量类型必须与 persist() 函数写入时的顺序和类型完全严格对应！
//     var currentTerm int
//     var votedFor int
//     var logs []one_log // 注意这里必须对应你底层使用的日志存储结构类型
//     var committed []bool
    
//     // 5. 按顺序执行反序列化 (Decode)。
//     // 这里使用了短路逻辑，如果任何一步解码失败（返回 error），则整个恢复动作放弃，
//     // 保护内存状态不被破坏（即“原子性恢复”）。
//     if d.Decode(&currentTerm) != nil ||
//        d.Decode(&votedFor) != nil ||
//        d.Decode(&logs) != nil ||
//        d.Decode(&committed) != nil {
        
//         // 如果序列化数据损坏，通常意味着磁盘数据异常，打印错误日志以便排查。
//         DPrintf("Error decoding Raft state")
//     } else {
//         // 6. 原子性更新内存状态：
//         // 只有在所有字段都成功读取后，才一次性覆盖原有的内存变量。
//         // 这样可以确保节点要么恢复到崩溃前的完整状态，要么保持空白，绝不会出现“半恢复”的混乱状态。
//         rf.currentTerm = currentTerm
//         rf.votedFor = votedFor
//         rf.logs = logs
        
//         // 🌟 辅助索引更新：根据恢复后的日志长度，即时更新末尾索引。
//         rf.lastLogIndex = len(rf.logs)
//         rf.committed = committed
        
//         DPrintf("节点 %d 成功恢复状态：Term=%d, LogLen=%d, VotedFor=%d", 
//                  rf.me, rf.currentTerm, rf.lastLogIndex, rf.votedFor)
//     }
// }

// readPersist 是 Raft 实现持久化恢复的核心方法。
// 它的作用是当节点重启（Crash & Restart）时，从底层的持久化存储器（Persister）
// 中读取之前保存的二进制状态快照，并还原非易失性状态 (Persistent State)。
// 
// 🚨 警告：绝对不能恢复 commitIndex、lastApplied 以及 committed 数组！
// 这些易失性状态必须在重启时清零，以触发状态机的强制回放。
func (rf *Raft) readPersist(data []byte) {
    // 1. 基础校验：如果 data 为空（例如节点第一次启动），直接跳出
    if data == nil || len(data) < 1 { 
        return
    }

    // 2. 创建读取缓冲区 (Buffer)
    r := bytes.NewBuffer(data)
    
    // 3. 初始化 labgob 解码器
    d := labgob.NewDecoder(r)
    
    // 4. 仅仅声明 Raft 论文规定的三大【持久化状态】
    var currentTerm int
    var votedFor int
    var logs []one_log // 注意这里必须对应你底层使用的日志存储结构类型
    
    // ❌ 删除了 var committed []bool，严禁将提交状态落盘！

    // 5. 按顺序执行反序列化 (Decode)。
    // 必须和 persist() 函数里 e.Encode() 的顺序严格一致！
    if d.Decode(&currentTerm) != nil ||
       d.Decode(&votedFor) != nil ||
       d.Decode(&logs) != nil {
        
        DPrintf("Error decoding Raft state")
    } else {
        // 6. 原子性更新内存中的持久化状态：
        rf.currentTerm = currentTerm
        rf.votedFor = votedFor
        rf.logs = logs
        
        // 7. 更新辅助索引
        rf.lastLogIndex = len(rf.logs)
        
        // 🌟 核心修复：易失性状态绝不能从磁盘读！必须全新初始化。
        // 这里重新 make 一个全为 false 的布尔数组。
        // 预留一些缓冲容量 (比如 len+1000)，防止你在后续高并发追加时频繁触发扩容或越界 Panic。
        rf.committed = make([]bool, len(rf.logs) + 1000) 
        
        DPrintf("节点 %d 成功恢复状态：Term=%d, LogLen=%d, VotedFor=%d", 
                 rf.me, rf.currentTerm, rf.lastLogIndex, rf.votedFor)
    }
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		isLeader = false
		//DPrintf("主机：%d,Start()不是领导者\n", rf.me)
		
		return index, term, isLeader
	}
	index = len(rf.logs)
	term = rf.currentTerm
	rf.logs = append(rf.logs, one_log{Cmd: command, Term: term, Index: index, Committed: false})
	rf.committed = append(rf.committed, false)
	DPrintf("主机：%d是leader,日志增加完成,日志内容为：%v\n", rf.me,rf.logs)
	log.Printf("主机：%d是leader,日志增加完成,日志内容为：%v\n", rf.me,rf.logs)
	rf.persist()
	//DPrintf("主机：%d,index:%d,rf.MaxnilNum:%d\n", rf.me,index,rf.MaxnilNum)
	return index-rf.MaxnilNum, term, isLeader
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

//初始化
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
	Committed: false,
	}
	rf.logs = append(rf.logs, zero_log)
	rf.MaxnilNum = 0
	rf.CurnilNum = 0
	rf.committed = append(rf.committed, false)
	
	DPrintf("主机：%d,RF建立完成\n", rf.me)
	
}

func (rf *Raft) updateCommitIndex() {
	for i := rf.commitIndex + 1; i < len(rf.logs); i++ {
		if rf.committed[i] {
			rf.commitIndex = i
		} else {
			break
		}
	}
}

// Make 函数是 Raft 实例的入口点。它负责初始化所有必要的内部结构，
// 从持久化存储中加载崩溃前的状态，并启动后台守护协程（Goroutines）来驱动 Raft 的共识逻辑。
//
// 参数说明：
//   peers: 集群中所有节点的 RPC 连接端点数组，用于向其他节点发送投票或同步日志。
//   me: 当前节点的唯一 ID，也是在 peers 数组中的索引。
//   persister: 持久化存储接口，用于在崩溃后通过读取磁盘来恢复 (Term, Log, Vote) 等数据。
//   applyCh: 该节点应用日志后，通过此通道通知上层应用（如 KVServer）执行具体的 Command。
func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {
    
    // 初始化 Raft 结构体实例
    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.applyCh = applyCh
    rf.me = me
    rf.commitCond = sync.NewCond(&rf.mu) // 🌟 用 rf 的互斥锁初始化条件变量
    // ==========================================
    // 阶段 1：内存数据结构的初始化
    // ==========================================
    // rf.Init() 负责初始化所有关键内部计数器：
    // - currentTerm (当前任期)
    // - votedFor (当前任期投给谁)
    // - log[] (日志条目数组，下标需从 1 开始防止空索引污染)
    // - commitIndex, lastApplied 等状态变量
    rf.Init()
    
    // ==========================================
    // 阶段 2：恢复持久化状态（崩溃恢复的关键）
    // ==========================================
    // 从 persister 中读取之前保存的字节流，将其反序列化回当前内存中。
    // 如果是第一次运行，persister 可能为空，此处应处理零值情况。
    rf.readPersist(persister.ReadRaftState())
    
    // ==========================================
    // 阶段 3：状态一致性检查
    // ==========================================
    // 强制执行一次持久化，确保新实例的初始状态（即使是默认值）已正确写入存储，
    // 防止因重启后的写操作没来得及刷盘就再次崩溃导致数据丢失。
    rf.persist()
    
    // 检查并更新本地的提交索引，确保恢复后的节点对当前全局提交情况有正确的认知。
    rf.updateCommitIndex()

    // ==========================================
    // 阶段 4：启动后台驱动协程 (Goroutines)
    // ==========================================
    // 必须确保这些循环函数是异步的，避免阻塞 Make() 的快速返回。
    
    // 1. LeaderAction: 若当前节点被选举为 Leader，该协程会定时发送心跳 (AppendEntries)
    //    并负责管理日志复制（更新 NextIndex/MatchIndex）。
    go rf.LeaderAction()
    DPrintf("主机：%d, LeaderAction() 启动\n", rf.me)
    
    // 2. FollowerAction: 实现 Raft 的超时机制。如果超过选举超时 (Election Timeout) 
    //    未收到心跳，该协程会自动切换状态，发起新一轮选举。
    go rf.FollowerAction()
    
    // 3. applyLog: 监听提交的日志索引变化，一旦有新的日志被 commit，
    //    将其包装成 ApplyMsg 放入 applyCh，通知 KVServer 应用到状态机。
    go rf.applyLog()
    
    DPrintf("主机：%d, FollowerAction() 启动\n", rf.me)
    
    return rf // 返回构建完成、准备就绪的 Raft 节点实例
}
