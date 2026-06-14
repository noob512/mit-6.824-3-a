package raft

import (
	"bytes"
	//"log"
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

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type one_log struct {
	Cmd   interface{}
	Term  int
	Index int
	//true_index int
	Committed bool //其实可以舍弃，但为了方便调试保留
}

// Raft 一个实现了单个 Raft 节点的 Go 对象。
type Raft struct {
	mu          sync.Mutex          // 保护共享状态的互斥锁
	peers       []*labrpc.ClientEnd // 所有节点的 RPC 端点（包括自身）
	persister   *Persister          // 持久化存储对象（用于崩溃后恢复状态）
	me          int                 // 当前节点在 peers 数组中的索引
	dead        int32               // 标识节点是否已被终止（由 Kill() 设置）
	currentTerm int                 //服务器见过的最新任期号（首次启动时为 0，单调递增）
	votedFor    int                 //在当前任期中投票给了哪个候选者 ID（若无则为 null）
	logs        []one_log           //日志条目数组；每个条目包含一条状态机命令，以及该条目被领导者接收时的任期号（首条索引为 1）
	committed   []bool
	commitIndex int   // 已提交的最高日志条目的索引（初始为 0）
	lastApplied int   // 已应用的最高日志条目的索引（初始为 0）
	nextIndex   []int // 领导者发送给每个追随者的下一个日志条目的索引（初始为日志长度 + 1）
	matchIndex  []int // 每个追随者已复制的最高日志条目的索引（初始为 0）
	state       int   // 节点当前的角色（Follower、Candidate、Leader）
	//lastLogIndex    int                 //	最后一条日志的索引
	lastMessageTime         time.Time     // 上次收到消息的时间（用于选举超时）
	ElectionTimeout         time.Duration // 选举超时时间（随机值，用于触发选举）
	turnToLeader            int           //控制initleader只在转换为leader时进行
	applyCh                 chan ApplyMsg //用于向上层提交命令
	MaxnilNum               int           //最大空白命令，用于用户在向主机提交日志时，返回正确的索引
	CurnilNum               int           //当前空白命令，用于主机向上层提交日志时确定真实索引
	commitCond              *sync.Cond
	lastIncludedIndex       int
	lastIncludedTerm        int
	lastIncludedPublicIndex int
	// 待实现的状态（对应论文图 2）
	// 2A 阶段：需要添加与 leader 选举相关的状态（如当前任期、角色、投票记录等）
	// 2B 阶段：添加日志相关状态（日志条目数组、提交索引等）
	// 2C 阶段：添加需要持久化的状态（任期、投票记录等）
}

// 随机产生一个超时时间
func (rf *Raft) randomTimeout() time.Duration {
	halfBase := int64(raftElectionTimeout / 2) // 50ms 对应的纳秒数
	offset := rand.Int63n(halfBase)
	return raftElectionTimeout + time.Duration(offset)
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// ------------------------------
func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.logs) - 1
}

func (rf *Raft) termAt(index int) int {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.logs[index-rf.lastIncludedIndex].Term
}

// realToPublicIndex 用于将 Raft 内部的全局“真实”日志索引 (real) 
// 转换为对外（状态机或客户端）可见的“公共”日志索引 (public)。
func (rf *Raft) realToPublicIndex(real int) int {
	// 1. 获取基准索引：将 public 初始值设为快照中最后包含的公共索引。
	// lastIncludedPublicIndex 记录的是发生日志压缩（打快照）前，状态机最后一次应用的有效命令索引。
	public := rf.lastIncludedPublicIndex 
	
	// 2. 遍历当前保留在内存中的日志条目（即尚未被压缩进快照的日志）。
	// offset 从 1 开始，因为在实现了快照的 Raft 中，rf.logs[0] 通常作为哨兵/占位符（Dummy Entry），
	// 用来保存上一次快照的元数据（如 lastIncludedIndex 和 lastIncludedTerm）。
	for offset := 1; offset < len(rf.logs); offset++ {
		
		// 3. 判断是否为有效命令日志：
		// 如果 Cmd 不为 nil，说明这是一条真实的客户端请求命令，
		// 而不是 Raft 内部生成的控制日志（比如 Leader 刚上任时为了提交之前任期的日志而追加的 no-op 空日志）。
		if rf.logs[offset].Cmd != nil {
			public++ // 只有遇到有效的客户端命令时，对外可见的公共索引才加 1
		}
		
		// 4. 匹配目标真实索引：
		// rf.lastIncludedIndex + offset 计算的是当前遍历到的日志在整个 Raft 历史中的“全局绝对真实索引”。
		// 如果计算出的绝对索引等于我们要查找的目标 real 索引，则代表找到了。
		if rf.lastIncludedIndex+offset == real {
			return public // 返回累加计算得到的公共索引
		}
	}
	
	// 5. 兜底返回：
	// 如果遍历完了当前内存中的所有日志都没有命中 target (可能是 real 值越界或者错误)，
	// 则返回当前能计算出的最大 public 索引。
	return public
}

// publicToRealIndex 用于将对外可见的公共索引 (publicIndex) 
// 映射回 Raft 内部的真实物理全局索引 (realIndex)。
// 返回值：
// int: 转换后的真实全局索引。如果未找到或已过期，通常返回 -1。
// bool: 是否成功在当前保留的日志（或快照边界）中找到了该索引。
func (rf *Raft) publicToRealIndex(publicIndex int) (int, bool) {
	
	// 1. 检查快照边界与已清理的日志
	// 如果请求的 publicIndex 小于或等于快照最后包含的公共索引，
	// 说明该日志要么正好是快照的最后一个条目，要么已经被压缩丢弃了。
	if publicIndex <= rf.lastIncludedPublicIndex {
		// 1.1 精准命中快照边界：
		// 如果正好等于快照的最后一条记录的公共索引，
		// 直接返回快照对应的真实全局索引 (lastIncludedIndex)。
		if publicIndex == rf.lastIncludedPublicIndex {
			return rf.lastIncludedIndex, true
		}
		// 1.2 日志已被压缩（丢弃）：
		// 请求的公共索引比快照边界还小，说明对应日志早被清理了，
		// 内存中无法查到，返回 -1 和 false。
		return -1, false
	}
	
	// 2. 准备遍历内存中的活跃日志
	// 基准索引：从快照最后包含的公共索引开始累加
	public := rf.lastIncludedPublicIndex
	
	// 遍历当前内存日志，offset=1 同样是因为 logs[0] 是辅助打快照的占位符（Dummy Entry）
	for offset := 1; offset < len(rf.logs); offset++ {
		
		// 3. 过滤出真实的客户端请求
		// 只有遇到非空的客户端命令时，公共索引 public 才加 1。
		// Raft 内部的空操作（No-op）会被跳过，不计入 public 索引。
		if rf.logs[offset].Cmd != nil {
			public++
			
			// 4. 匹配目标公共索引
			// 累加后，如果当前的 public 正好等于我们要找的 publicIndex，
			// 说明找到了对应的日志条目。
			if public == publicIndex {
				// 返回计算得出的绝对真实索引 (被截断的日志数量 + 当前数组偏移量)
				return rf.lastIncludedIndex + offset, true
			}
		}
	}
	
	// 5. 越界或未找到
	// 如果遍历完所有的内存日志都没有找到，说明该 publicIndex 超出了当前 Raft 节点已知的日志范围
	// （例如，客户端查询了一个还没被生成的未来索引）。
	return -1, false
}

// func (rf *Raft) persistData() []byte {
// 	w := new(bytes.Buffer)
// 	e := labgob.NewEncoder(w)

// 	e.Encode(rf.currentTerm)
// 	e.Encode(rf.votedFor)
// 	e.Encode(rf.logs)
// 	e.Encode(rf.lastIncludedIndex)
// 	e.Encode(rf.lastIncludedTerm)

// 	return w.Bytes()
// }

// // // 2C 阶段实现：将持久化状态编码并保存到 persister
// // 示例：使用 labgob 编码状态，通过 rf.persister.SaveRaftState() 保存
// func (rf *Raft) persist() {
// 	w := new(bytes.Buffer)
//     e := labgob.NewEncoder(w)
//     e.Encode(rf.currentTerm)
//     e.Encode(rf.votedFor)
//     e.Encode(rf.logs)        // ✅
// 	//e.Encode(rf.committed)
//     data := w.Bytes()
//     rf.persister.SaveRaftState(data)  // ✅ 完全覆盖之前的内容
// }

func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastIncludedPublicIndex)

	return w.Bytes()
}

func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.persistData())
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

// Snapshot 由上层应用 (kvserver) 主动调用。
// 参数 index: 告诉 Raft“这个 index 及之前的日志已经被我打包进快照了，你可以删了”。
// 参数 snapshot: 上层应用传来的状态机序列化字节数组 (里面包含 KV 数据和客户端去重表)。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// 加锁保护 Raft 的内部状态，防止与其他 RPC (如 AppendEntries) 并发冲突
	rf.mu.Lock()
	defer rf.mu.Unlock()
	realIndex, ok := rf.publicToRealIndex(index)
	if !ok {
		DPrintf("主机：%d 创建快照中止 | public index (%d) 无法映射到真实日志\n",
			rf.me, index)
		return
	}

	// 打印修改前的初始状态：机器 ID、请求截断的 index、当前的快照 index 以及完整的旧日志数组
	DPrintf("主机：%d 创建快照启动 | 目标 public index: %d | 目标 real index: %d | 当前 lastIncludedIndex: %d | 旧日志状态: %+v\n",
		rf.me, index, realIndex, rf.lastIncludedIndex, rf.logs)

	// 【防御性编程 1：过期请求拦截】
	if realIndex <= rf.lastIncludedIndex {
		DPrintf("主机：%d 创建快照中止 | 传入的 real index (%d) 已过期 (<= %d)\n",
			rf.me, realIndex, rf.lastIncludedIndex)
		return
	}

	// // 【防御性编程 2：越界拦截】
	// if index > rf.lastLogIndex() {
	// 	return
	// }

	// 获取将要成为快照最后一条记录的 Term (任期)。
	term := rf.termAt(realIndex)

	// 【核心逻辑 1：创建新的切片，避免内存泄漏】
	newLog := make([]one_log, 0)
	newCommitted := make([]bool, 0)

	// 【核心逻辑 2：设置“哨兵”/“虚拟”节点】
	newLog = append(newLog, one_log{
		Term: term,
	})
	newCommitted = append(newCommitted, true)
	DPrintf("主机：%d 刚创建的一条日志为: %+v\n",
		rf.me, newLog)

	// 【核心逻辑 3：拷贝保留的尾部日志】
	if realIndex < rf.lastLogIndex() {
		// 计算这个绝对 index 在当前尚未被替换的 rf.log 数组中的相对下标 (offset)
		offset := realIndex - rf.lastIncludedIndex

		// 采用 append + nil 初始化技巧进行【深度拷贝】。
		tail := append([]one_log(nil), rf.logs[offset+1:]...)
		newLog = append(newLog, tail...)
		committedTail := append([]bool(nil), rf.committed[offset+1:offset+1+len(tail)]...)
		newCommitted = append(newCommitted, committedTail...)
	}

	// 【状态更新】
	rf.logs = newLog
	rf.committed = newCommitted
	rf.lastIncludedIndex = realIndex
	rf.lastIncludedTerm = term
	rf.lastIncludedPublicIndex = index

	// 打印修改后的最终状态：更新后的快照 index、Term 以及裁剪后的新日志数组
	DPrintf("主机：%d 创建快照完成 | 新 lastIncludedIndex: %d | 新 lastIncludedPublicIndex: %d | 新 lastIncludedTerm: %d | 新日志状态: %+v\n",
		rf.me, rf.lastIncludedIndex, rf.lastIncludedPublicIndex, rf.lastIncludedTerm, rf.logs)

	// 【持久化：原子写入】
	rf.persistWithSnapshot(snapshot)
}

//------------------------------

// 2A时实现，用于确定当前是否是leader
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	DPrintf("主机：%d,GetState()启动\n", rf.me)
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// 更新收到消息的时间（如果超时了是要选举的）
func (rf *Raft) updateTime() {
	rf.lastMessageTime = time.Now()
	rf.ElectionTimeout = rf.randomTimeout()
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
	var lastIncludedIndex int
	var lastIncludedTerm int
	var lastIncludedPublicIndex int

	// ❌ 删除了 var committed []bool，严禁将提交状态落盘！

	// 5. 按顺序执行反序列化 (Decode)。
	// 必须和 persist() 函数里 e.Encode() 的顺序严格一致！
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&lastIncludedPublicIndex) != nil {

		DPrintf("Error decoding Raft state")
	} else {
		// 6. 原子性更新内存中的持久化状态：
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastIncludedPublicIndex = lastIncludedPublicIndex

		// 7. 更新辅助索引
		//rf.lastLogIndex = len(rf.logs)

		// 🌟 核心修复：易失性状态绝不能从磁盘读！必须全新初始化。
		// 这里重新 make 一个全为 false 的布尔数组。
		// 预留一些缓冲容量 (比如 len+1000)，防止你在后续高并发追加时频繁触发扩容或越界 Panic。
		rf.committed = make([]bool, len(rf.logs)+1000)

		DPrintf("节点 %d 成功恢复状态：Term=%d, LogLen=%d, VotedFor=%d",
			rf.me, rf.currentTerm, rf.lastLogIndex(), rf.votedFor)
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
		DPrintf("主机：%d,Start()不是领导者\n", rf.me)

		return index, term, isLeader
	}
	index = rf.lastLogIndex() + 1
	term = rf.currentTerm
	rf.logs = append(rf.logs, one_log{Cmd: command, Term: term, Index: index, Committed: false})
	rf.committed = append(rf.committed, false)
	publicIndex := rf.realToPublicIndex(index)
	DPrintf("主机：%d是leader,日志增加完成,日志内容为：%v\n", rf.me, rf.logs)
	//log.Printf("主机：%d是leader,日志增加完成,日志内容为：%v\n", rf.me,rf.logs)
	rf.persist()
	//DPrintf("主机：%d,index:%d,rf.MaxnilNum:%d\n", rf.me,index,rf.MaxnilNum)
	return publicIndex, term, isLeader
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

// 初始化
// Init 负责在创建新的 Raft 节点时初始化其所有的内存状态。
// 注意：如果是从崩溃中重启，这里的部分状态后续会被 persister.ReadRaftState() 覆盖。
func (rf *Raft) Init() {
	// 标记节点是否存活的标志位（通常在 Kill() 中设置为 1）
	rf.dead = 0

	// 【注意】Raft 论文通常建议初始 Term 为 0。
	// 这里设为 1 也可以正常工作，只要全网统一即可。
	rf.currentTerm = 1

	// 初始化日志数组为空切片
	rf.logs = make([]one_log, 0)

	// 初始化提交和应用索引。
	// 【警告】因为你在下面引入了第 0 个“哨兵节点”，
	// 如果哨兵节点占据了 index 0，那么真实的日志是从 index 1 开始的。
	// 在这种情况下，commitIndex 和 lastApplied 往往初始化为 0 更合适，代表“哨兵节点已被隐式提交”。
	// 设为 -1 可能会在后续的数组下标计算中引发 index out of bounds 异常。
	rf.commitIndex = -1
	rf.lastApplied = -1

	// Leader 专用的状态字典。
	// 其实这里初始化意义不大，因为每次当前节点当选 Leader 时，
	// 必须且一定会重新将 nextIndex 重置为 len(rf.logs)，matchIndex 重置为 0。
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// 所有节点启动时默认都是 Follower（跟随者）状态
	rf.state = Follower

	// 正确的 votedFor 初始化：-1 表示当前任期还没投票给任何 peer
	rf.votedFor = -1

	// 重置最后一次收到心跳的时间，用于触发心跳超时选举
	rf.lastMessageTime = time.Now()
	// 获取一个随机的选举超时时间（通常在 150ms ~ 300ms 之间或类似范围）
	rf.ElectionTimeout = rf.randomTimeout()

	// 自定义状态：可能是你用来记录当选 Leader 次数或时间戳的变量
	rf.turnToLeader = 0

	// 【核心逻辑：哨兵节点的建立】
	// 创建第 0 条虚拟日志（Dummy Log）。
	// 它的作用是占位，使得第一条真实客户端请求的 index 为 1。
	// 同时，它也充当了快照机制中初始的 lastIncludedTerm 和 lastIncludedIndex 的载体。
	zero_log := one_log{
		Term:      0, // 任期 0，作为整个 Raft 生命周期的绝对起点
		Committed: false,
	}
	rf.logs = append(rf.logs, zero_log)

	// 自定义状态：可能用于统计你自定义的 nil 操作或某种限流控制
	rf.MaxnilNum = 0
	rf.CurnilNum = 0

	// 【高危设计】初始化一个与日志长度平行的布尔切片来记录是否提交。
	rf.committed = append(rf.committed, false)

	// 打印调试信息，表明该节点的内存状态初始化完毕
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
//
//	peers: 集群中所有节点的 RPC 连接端点数组，用于向其他节点发送投票或同步日志。
//	me: 当前节点的唯一 ID，也是在 peers 数组中的索引。
//	persister: 持久化存储接口，用于在崩溃后通过读取磁盘来恢复 (Term, Log, Vote) 等数据。
//	applyCh: 该节点应用日志后，通过此通道通知上层应用（如 KVServer）执行具体的 Command。
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
	//rf.updateCommitIndex()
	//DPrintf("主机：%d,此处rf.commitIndex=%d\n", rf.me,rf.commitIndex)

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
	DPrintf("主机：%d, FollowerAction() 启动\n", rf.me)
	// 3. applyLog: 监听提交的日志索引变化，一旦有新的日志被 commit，
	//    将其包装成 ApplyMsg 放入 applyCh，通知 KVServer 应用到状态机。
	go rf.applyLog()
	return rf // 返回构建完成、准备就绪的 Raft 节点实例
}
