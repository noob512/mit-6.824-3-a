package shardmaster


import "../raft"
import "../labrpc"
import "sync"
import "../labgob"
import "log"
import "time"
import "sort"


const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
    mu      sync.Mutex
    me      int
    rf      *raft.Raft
    applyCh chan raft.ApplyMsg

    // 1. 去重记录表 (Duplicate Detection)
    // Key: ClientId (客户端的唯一标识)
    // Value: 记录该客户端成功执行的最新 CommandId / SeqNum
    lastApplied map[int64]int
	// 2. RPC 唤醒通道表 (Notification Channels)
    // Key: Raft Log Index (Raft 日志的索引号)
    // Value: 一个 Channel，用于将 applyCh 送来的执行结果，异步传回给正在阻塞等待的 RPC 协程
    notifyChans map[int]chan Op

    configs []Config // indexed by config num
}


type Op struct {
	// 基础控制参数
	OpType    string // 操作类型，例如 "Join", "Leave", "Move", "Query"
	ClientId  int64  // 客户端唯一标识（用于去重防重放）
	CommandId int    // 客户端递增的请求序列号（用于去重防重放）

	// 业务参数（根据 OpType 的不同，只会用到其中一部分）
	Num     int              // Query 用：期望查询的配置版本号
	Servers map[int][]string // Join 用：新加入的 GID 及对应的服务器列表
	GIDs    []int            // Leave 用：需要移除的 GID 列表
	Shard   int              // Move 用：需要强制移动的分片编号
	GID     int              // Move 用：强制指定的的目标副本组 GID
}



// rebalance 负载均衡函数：在发生 Join 或 Leave 时，重新分配 Shards
func (sm *ShardMaster) rebalance(config *Config) {
	// ==========================================
	// 阶段 1：极端情况处理（集群缩容到 0）
	// ==========================================
	if len(config.Groups) == 0 {
		for i := 0; i < len(config.Shards); i++ {
			config.Shards[i] = 0 // 所有的分片重置为无效 GID 0
		}
		return
	}

	// ==========================================
	// 阶段 2：获取并排序所有活跃的 GID（防止 map 遍历随机性）
	// ==========================================
	var gids []int
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids) // 极其重要：必须排序！保证所有的 Raft 节点计算出的分配结果绝对一致

	// ==========================================
	// 阶段 3：盘点当前家底（统计现状）
	// ==========================================
	// gidToShards 记录当前每个【活跃的 GID】手里握着哪些分片
	gidToShards := make(map[int][]int)
	for _, gid := range gids {
		gidToShards[gid] = make([]int, 0)
	}

	// unassigned 记录“无主分片”（即分配给了 GID 0，或者分配给了刚刚 Leave 离开的 GID 的分片）
	var unassigned []int
	for shard, gid := range config.Shards {
		if _, exists := config.Groups[gid]; exists {
			// 如果分片属于当前活跃的组，记录到该组的名下
			gidToShards[gid] = append(gidToShards[gid], shard)
		} else {
			// 如果组已经不存在了（Leave），或者初始状态（GID 0），加入“无主分片”列表
			unassigned = append(unassigned, shard)
		}
	}

	// 定义一个内部闭包函数，用于在排好序的 GID 中找出当前分片【最少】和【最多】的 GID
	getMinMaxGid := func() (int, int) {
		minGid, maxGid := gids[0], gids[0]
		for _, gid := range gids {
			// 注意：因为 gids 是递增遍历的，如果数量相等，会默认选择较小的 GID (确定的 Tie-breaker)
			if len(gidToShards[gid]) < len(gidToShards[minGid]) {
				minGid = gid
			}
			if len(gidToShards[gid]) > len(gidToShards[maxGid]) {
				maxGid = gid
			}
		}
		return minGid, maxGid
	}

	// ==========================================
	// 阶段 4：执行劫富济贫的分配
	// ==========================================

	// 步骤 4.1：先把所有的“无主分片”安顿好 (优先处理 Leave 操作遗留的分片)
	// 每次都把一个无主分片交给当前最穷的组
	for _, shard := range unassigned {
		minGid, _ := getMinMaxGid()
		gidToShards[minGid] = append(gidToShards[minGid], shard)
		config.Shards[shard] = minGid
	}

	// 步骤 4.2：处理老组之间的贫富差距 (处理 Join 操作导致的不平衡)
	for {
		minGid, maxGid := getMinMaxGid()

		// 检查平衡的终极条件：最富的组和最穷的组分片数之差不能超过 1
		if len(gidToShards[maxGid]) <= len(gidToShards[minGid])+1 {
			break // 已经达到完美平衡，退出循环
		}

		// 执行剥夺：从最富的组里拿出一个分片
		// 为了确定性，总是拿它手里的第 0 个分片
		shardToMove := gidToShards[maxGid][0]
		
		// 更新内部统计 map
		gidToShards[maxGid] = gidToShards[maxGid][1:]
		gidToShards[minGid] = append(gidToShards[minGid], shardToMove)
		
		// 真正修改 Config 的映射关系
		config.Shards[shardToMove] = minGid
	}
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// 1. 将客户端请求封装成状态机可以识别的操作 (Op)
	op := Op{
		OpType:    "Join",
		Servers:   args.Servers,   // 把需要加入的 map[int][]string 传进去
		ClientId:  args.ClientId,  // 防重放机制的关键
		CommandId: args.CommandId, // 防重放机制的关键
	}

	// 2. 提交给底层的 Raft 节点
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	// 3. 注册并获取用于接收通知的 Channel
	sm.mu.Lock()
	if _, ok := sm.notifyChans[index]; !ok {
		// channel 容量设为 1，防止 applier 往里写数据时被阻塞
		sm.notifyChans[index] = make(chan Op, 1)
	}
	ch := sm.notifyChans[index]
	sm.mu.Unlock()

	// 4. 阻塞等待：可能是 applyCh 传来了结果，也可能是超时
	select {
	case appliedOp := <-ch:
		// 5. 二次校验：当前日志槽位执行的，到底是不是我提交的这个客户端的指令？
		// 如果因为网络分区等原因导致 Leader 降级，这里的日志可能会被新 Leader 覆盖。
		if appliedOp.ClientId != args.ClientId || appliedOp.CommandId != args.CommandId {
			reply.WrongLeader = true
			reply.Err = "ErrWrongLeader"
		} else {
			// 匹配成功，说明 Join 操作已经被状态机成功执行完毕（或者被去重逻辑识别为成功）
			reply.WrongLeader = false
			reply.Err = "OK"
		}
	case <-time.After(500 * time.Millisecond): // 500ms 超时限制
		// 如果超过 500ms Raft 还没达成共识，说明可能发生分区，让客户端去找别人重试
		reply.WrongLeader = true
		reply.Err = "ErrTimeout"
	}

	// 6. 清理现场：删除 map 中的 channel 防止内存泄漏
	sm.mu.Lock()
	delete(sm.notifyChans, index)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// 1. 将客户端请求封装成状态机可以识别的操作 (Op)
	op := Op{
		OpType:    "Leave",
		GIDs:      args.GIDs,      // 把需要移除的 GID 列表传进去
		ClientId:  args.ClientId,  // 防重放机制的关键
		CommandId: args.CommandId, // 防重放机制的关键
	}

	// 2. 提交给底层的 Raft 节点
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	// 3. 注册并获取用于接收通知的 Channel
	sm.mu.Lock()
	if _, ok := sm.notifyChans[index]; !ok {
		// channel 容量设为 1，防止 applier 往里写数据时被阻塞
		sm.notifyChans[index] = make(chan Op, 1)
	}
	ch := sm.notifyChans[index]
	sm.mu.Unlock()

	// 4. 阻塞等待：可能是 applyCh 传来了结果，也可能是超时
	select {
	case appliedOp := <-ch:
		// 5. 二次校验：当前日志槽位执行的，到底是不是我提交的这个客户端的指令？
		// 如果因为网络分区等原因导致 Leader 降级，这里的日志可能会被新 Leader 覆盖。
		if appliedOp.ClientId != args.ClientId || appliedOp.CommandId != args.CommandId {
			reply.WrongLeader = true
			reply.Err = "ErrWrongLeader"
		} else {
			// 匹配成功，说明 Leave 操作已经被状态机成功执行完毕（或者被去重逻辑识别为成功）
			reply.WrongLeader = false
			reply.Err = "OK"
		}
	case <-time.After(500 * time.Millisecond): // 500ms 超时限制
		// 如果超过 500ms Raft 还没达成共识，说明可能发生分区，让客户端去找别人重试
		reply.WrongLeader = true
		reply.Err = "ErrTimeout"
	}

	// 6. 清理现场：删除 map 中的 channel 防止内存泄漏
	sm.mu.Lock()
	delete(sm.notifyChans, index)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// 1. 将客户端请求封装成状态机可以识别的操作 (Op)
	op := Op{
		OpType:    "Move",
		Shard:     args.Shard,     // 需要强制移动的分片编号
		GID:       args.GID,       // 目标副本组的 GID
		ClientId:  args.ClientId,  // 防重放机制的关键
		CommandId: args.CommandId, // 防重放机制的关键
	}

	// 2. 提交给底层的 Raft 节点
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	// 3. 注册并获取用于接收通知的 Channel
	sm.mu.Lock()
	if _, ok := sm.notifyChans[index]; !ok {
		// channel 容量设为 1，防止 applier 往里写数据时被阻塞
		sm.notifyChans[index] = make(chan Op, 1)
	}
	ch := sm.notifyChans[index]
	sm.mu.Unlock()

	// 4. 阻塞等待：可能是 applyCh 传来了结果，也可能是超时
	select {
	case appliedOp := <-ch:
		// 5. 二次校验：当前日志槽位执行的，到底是不是我提交的这个客户端的指令？
		// 如果因为网络分区等原因导致 Leader 降级，这里的日志可能会被新 Leader 覆盖。
		if appliedOp.ClientId != args.ClientId || appliedOp.CommandId != args.CommandId {
			reply.WrongLeader = true
			reply.Err = "ErrWrongLeader"
		} else {
			// 匹配成功，说明 Move 操作已经被状态机成功执行完毕（或者被去重逻辑识别为成功）
			reply.WrongLeader = false
			reply.Err = "OK"
		}
	case <-time.After(500 * time.Millisecond): // 500ms 超时限制
		// 如果超过 500ms Raft 还没达成共识，说明可能发生分区，让客户端去找别人重试
		reply.WrongLeader = true
		reply.Err = "ErrTimeout"
	}

	// 6. 清理现场：删除 map 中的 channel 防止内存泄漏
	sm.mu.Lock()
	delete(sm.notifyChans, index)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// 1. 封装状态机操作
	op := Op{
		OpType:    "Query",
		Num:       args.Num,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}

	// 2. 提交给底层 Raft 集群
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	// 3. 为当前日志索引创建一个通知 Channel，并等待结果
	sm.mu.Lock()
	// 假设你在 ShardMaster 中维护了一个 map[int]chan Op 用于唤醒 RPC
	if _, ok := sm.notifyChans[index]; !ok {
		sm.notifyChans[index] = make(chan Op, 1)
	}
	ch := sm.notifyChans[index]
	sm.mu.Unlock()

	// 4. 阻塞等待 Raft 达成共识或超时
	select {
	case appliedOp := <-ch:
		// 关键点：校验取出的日志是否真的是当前客户端提交的这一个
		// 如果 CommandId 或 ClientId 不符合，说明该位置的日志被新的 Leader 覆盖了
		if appliedOp.ClientId != args.ClientId || appliedOp.CommandId != args.CommandId {
			reply.WrongLeader = true
			reply.Err = "ErrWrongLeader"
		} else {
			// 校验通过，根据 args.Num 填充配置并返回
			reply.WrongLeader = false
			reply.Err = "OK"

			sm.mu.Lock()
			// 如果 Num 为 -1，或者超出了当前最大的配置版本号，返回最新配置
			if args.Num == -1 || args.Num >= len(sm.configs) {
				reply.Config = sm.configs[len(sm.configs)-1]
			} else {
				// 否则返回指定版本的历史配置
				reply.Config = sm.configs[args.Num]
			}
			sm.mu.Unlock()
		}
	case <-time.After(500 * time.Millisecond):
		// 5. 超时处理：Raft 集群可能瘫痪或当前节点丢失了 Leader 身份
		reply.WrongLeader = true
		reply.Err = "ErrTimeout"
	}

	// 6. 清理使用完毕的 Channel，防止内存泄漏
	sm.mu.Lock()
	delete(sm.notifyChans, index)
	sm.mu.Unlock()
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}


func (sm *ShardMaster) applier() {
	for msg := range sm.applyCh {
		if msg.CommandValid {
			sm.mu.Lock()
			op := msg.Command.(Op)
			
			// 1. 去重检查：判断该操作是否已经被执行过
			// 注意：Query 是读操作，不改变状态，不需要去重拦截，直接放行即可
			isDuplicate := false
			if lastSeq, exists := sm.lastApplied[op.ClientId]; exists && op.CommandId <= lastSeq {
				isDuplicate = true
			}

			// 2. 如果不是重复请求，且是写操作（Join/Leave/Move），则修改状态机
			if !isDuplicate && op.OpType != "Query" {
				// 获取系统当前的最新配置
				lastConfig := sm.configs[len(sm.configs)-1]
				
				// 【极其重要】：深拷贝生成新的 Config 快照！
				newConfig := Config{
					Num:    lastConfig.Num + 1,        // 版本号递增
					Shards: lastConfig.Shards,         // 数组（[10]int）在 Go 中是值传递，可以直接赋值拷贝
					Groups: make(map[int][]string),    // map 是引用类型，必须 make 一个新的
				}
				// 逐个拷贝原有的组信息
				for gid, servers := range lastConfig.Groups {
					newConfig.Groups[gid] = servers 
				}

				// 根据具体的操作类型，修改这份全新的配置
				switch op.OpType {
				case "Join":
					// 将新加入的集群节点写入字典
					for gid, servers := range op.Servers {
						newConfig.Groups[gid] = servers
					}
					// 触发负载均衡，重新分配 Shards
					sm.rebalance(&newConfig) 

				case "Leave":
					// 将离开的集群节点从字典中移除
					for _, gid := range op.GIDs {
						delete(newConfig.Groups, gid)
					}
					// 触发负载均衡，把离开组的分片匀给剩下的组
					sm.rebalance(&newConfig) 

				case "Move":
					// 强制移动分片：将特定分片指向特定的 GID（不需要全局均衡）
					newConfig.Shards[op.Shard] = op.GID
				}

				// 将生成好的新配置追加到历史切片中
				sm.configs = append(sm.configs, newConfig)
				
				// 更新该客户端的去重记录，标记这个 CommandId 已经执行完毕
				sm.lastApplied[op.ClientId] = op.CommandId
				// 🟢 新增的日志打印：直观展示每次配置变更后的最终盘面
				// DPrintf("[ShardMaster %d] 执行 %s 完毕 | ConfigNum: %d | Groups: %v | Shards 分配: %v\n", 
				// 	sm.me, op.OpType, newConfig.Num, newConfig.Groups, newConfig.Shards)
			}

			// 3. 唤醒正在等待的 RPC Handler
			// 注意：无论是正常执行、被去重拦截，还是 Query 操作，最终都要通知 RPC 层
			// 告诉 RPC 层：“这个 index 对应的日志已经过完状态机了，你可以给客户端回信了”
			if ch, ok := sm.notifyChans[msg.CommandIndex]; ok {
				ch <- op // 把日志原封不动地发过去，RPC 端会校验 ClientId
			}
			
			sm.mu.Unlock()
		} else if msg.SnapshotValid {
			// Lab 4 暂不要求快照，可以先忽略，或者留空
		}
	}
}

// StartServer 启动一个容错的分片主控（ShardMaster）服务器节点
// 参数 servers: 集群中所有服务器节点的网络发送端点（用于 Raft 内部的 RPC 通信）
// 参数 me: 当前节点在 servers 数组中的索引编号（即当前节点的 ID）
// 参数 persister: 指向持久化存储的指针，保存了 Raft 崩溃前状态和快照
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
    // 实例化一个新的 ShardMaster 结构体
    sm := new(ShardMaster)
    // 记录当前节点的 ID
    sm.me = me

    // 初始化历史配置切片（configs），初始长度为 1
    sm.configs = make([]Config, 1)
    // 按照 Lab 4 要求，初始化第一个配置（Config 0）
    // Config 0 不包含任何有效的副本组，因此 Groups map 被初始化为空
    sm.configs[0].Groups = map[int][]string{}

    // 向 labgob 注册状态机操作结构体 Op{}
    // 这是必须的，因为 Op{} 会作为 interface{} 放入 Raft 的日志中，
    // labgob 需要提前知道它的具体类型才能在 RPC 传输时进行序列化和反序列化
    labgob.Register(Op{})
    
    // 创建一个 applyCh 管道
    // 底层的 Raft 节点在日志达成共识（被 commit）后，会通过这个管道将日志发送给 ShardMaster 状态机执行
    sm.applyCh = make(chan raft.ApplyMsg)
    
    // 启动底层的 Raft 实例，将网络端点、自身 ID、持久化器和提交通道传递给它
    // 从此刻起，该节点开始参与 Raft 选举和日志同步
    sm.rf = raft.Make(servers, me, persister, sm.applyCh)

    // Your code here.
    // 此处需要补充你的初始化代码，例如：
    // 1. 初始化防止客户端重复请求的去重记录（如：map[int64]int）
    // 2. 初始化用于等待 Raft 提交结果的通知机制（如：条件变量、Channel 映射表）
    // 3. 启动一个后台 Goroutine 去不断监听 sm.applyCh，并将提交的日志应用到 sm.configs 状态机中
	sm.notifyChans = make(map[int]chan Op)      // 初始化通知通道表
	sm.lastApplied = make(map[int64]int)        // 初始化去重表 (如果你在结构体里定义了的话)
    // 返回初始化完毕的服务器实例
	// 启动后台应用日志的协程
    go sm.applier()
    return sm
}
