package shardkv

// import "../shardmaster"
import "../labrpc"
import "../raft"
import "sync"
import "../labgob"
import "log"
import "../shardmaster"
import "time"
import "bytes"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// 定义分片状态类型
type ShardStatus int

const (
	// Invalid: 初始状态，或者配置更新后该分片不再属于本组。直接拒绝读写请求。
	Invalid ShardStatus = iota

	// Serving: 正常服务状态。该分片属于本组，且数据已经准备就绪，可以正常处理客户端的 Get/Put/Append。
	Serving

	// Pulling: 正在拉取状态。配置更新后，该分片分配给了本组，但我们还没从老组那里拿到真实数据。此时需要阻塞客户端请求。
	Pulling

	// Pushing: 正在推送状态（垃圾回收期的中间态）。配置更新后，该分片不再属于本组。
	// 我们已经停止对外服务，正等待新组把数据拉走，并发送 ACK 确认。收到 ACK 后才会将其转为 Invalid 并清理内存。
	Pushing

	GCing
)

// ShardData 表示一个独立的“微型键值数据库”
// 整个 ShardKV 就是由 10 个这样的微型数据库组成的组合体。
type ShardData struct {
	KVStore   map[string]string // 存储真实的键值对数据
	ClientSeq map[int64]int     // 针对该分片的客户端去重表：ClientId -> 已经执行过的最大 CommandId
	Status    ShardStatus       // 分片当前的状态
}

type PullTask struct {
	shardId   int
	configNum int      // 当前的配置版本号
	oldGID    int      // 老东家的 GID
	servers   []string // 老东家的服务器地址列表
}

type GCTask struct {
	shardId   int
	configNum int
	oldGID    int
	servers   []string
}

// FetchShardArgs 新组向老组拉取分片数据的请求参数
type FetchShardArgs struct {
	ShardId   int // 请求拉取的分片编号 (0-9)
	ConfigNum int // 请求方当前的配置版本号 (Config N)
}

// FetchShardReply 老组返回给新组的数据响应
type FetchShardReply struct {
	Err  Err       // RPC 执行结果 (OK, ErrWrongLeader, ErrNotReady)
	Data ShardData // 核心载荷：分片的真实数据（包含 KVStore 和去重表 ClientSeq）
}

// DeleteShardArgs 新组向老组发送的“垃圾回收/清理确认”请求参数
type DeleteShardArgs struct {
	ShardId   int // 已经成功接收并落地的分片编号
	ConfigNum int // 发生迁移时的配置版本号 (极其重要，用于防止误删)
}

// DeleteShardReply 老组返回给新组的清理结果
type DeleteShardReply struct {
	Err Err // 执行结果 (通常是 OK 或 ErrWrongLeader)
}

// Op 是提交给 Raft 达成共识的统一操作指令
type Op struct {
	// 基础控制字段
	OpType string // 操作类型："Get", "Put", "Append", "UpdateConfig", "InsertShard", "DeleteShard"

	// 客户端防重放验证字段 (用于常规读写)
	ClientId  int64
	CommandId int

	// 常规键值对读写字段 (用于 Get/Put/Append)
	Key   string
	Value string

	// ==========================================
	// Lab 4B 独有的分片管理字段
	// ==========================================

	// 配置升级字段 (用于 UpdateConfig)
	Config shardmaster.Config // 携带 ConfigTicker 轮询到的新配置

	// 分片迁移与垃圾回收字段 (用于 InsertShard / DeleteShard)
	ShardId   int       // 需要插入或删除的分片编号 (0-9)
	ConfigNum int       // 迁移发生时的配置版本号（极其重要：防止过期的乱序 RPC 破坏状态机）
	Data      ShardData // 传输分片数据时，携带真实的键值对 map 和去重表 ClientSeq
}

// =================================================================
// 以下是 applier 内部调用的具体状态机执行函数（空函数占位符）
// 注意：这些函数被调用时，外层 applier 已经持有了 kv.mu 锁，
// 所以这些函数内部【绝对不能】再次加锁，否则会导致死锁！
// =================================================================

// applyClientOperation 处理常规的 Get/Put/Append 请求
// ⚠️ 注意：调用此函数前，外层 applier 已经持有了 kv.mu 锁，切勿在此处再次加锁！
func (kv *ShardKV) applyClientOperation(op *Op) {
	// 1. 根据 op.Key 计算 ShardId (时间复杂度 O(1))
	shardId := key2shard(op.Key)

	// 2. 致命校验：检查该分片当前是否处于正常服务状态 (Serving)
	// 如果此时分片正在迁移 (Pulling/Pushing) 或根本不归我管 (Invalid)
	// 我们绝对不能修改本地状态机！直接 return 丢弃该操作。
	// 当后续对应这个 index 的 channel 唤醒 RPC 协程时，
	// RPC 协程会在退出前的“二次校验”中发现状态不对，从而给客户端返回 ErrWrongGroup。
	if kv.shards[shardId].Status != Serving && kv.shards[shardId].Status != GCing {
		return
	}

	// 3. 检查 op.CommandId 是否已经执行过 (防重放校验)
	isDuplicate := false
	if lastSeq, ok := kv.shards[shardId].ClientSeq[op.ClientId]; ok && op.CommandId <= lastSeq {
		isDuplicate = true
	}

	// 4. 执行具体的 map 读写
	// 只有在【不是重复请求】的情况下，才去修改真实的数据和去重表
	if !isDuplicate {
		if op.OpType == "Put" {
			// 直接覆盖写入
			kv.shards[shardId].KVStore[op.Key] = op.Value

		} else if op.OpType == "Append" {
			// 如果 Key 不存在，Go 默认会以空字符串 "" 开始拼接，完全符合 Append 的语义
			kv.shards[shardId].KVStore[op.Key] += op.Value

		}
		// 注意：如果是 "Get"，我们什么都不用做。状态机不需要因为读操作而发生数据改变。

		// 5. 更新该客户端的最新 CommandId
		// 只要成功应用了该操作（包含 Get），都可以更新防重放水位线
		kv.shards[shardId].ClientSeq[op.ClientId] = op.CommandId
	}
}

// applyConfiguration 处理配置升级 (Config N -> Config N+1)
// ⚠️ 注意：调用此函数前，外层 applier 已经持有了 kv.mu 锁！
func (kv *ShardKV) applyConfiguration(op *Op) {
	newConfig := op.Config

	// 1. 严格版本校验 (线性一致性防线)
	// 无论 Raft 怎么乱序回放，状态机必须一步一个脚印地升级。
	// 只有严格等于当前版本 + 1 的配置才被允许应用，拒绝跳跃或过期的配置。
	if newConfig.Num != kv.config.Num+1 {
		return
	}

	// 2. 遍历 10 个分片，计算状态流转
	for i := 0; i < shardmaster.NShards; i++ {
		oldGID := kv.config.Shards[i] // 以前归谁管
		newGID := newConfig.Shards[i] // 现在归谁管

		// 情况 A：我们【获得】了这个分片的所有权
		if newGID == kv.gid && oldGID != kv.gid {
			if oldGID == 0 {
				// 创世特例：从 Config 0 升级上来，不需要向任何人拉取数据，直接就地生效
				kv.shards[i].Status = Serving
			} else {
				// 正常交接：从别的组接手，准备去拉取数据
				kv.shards[i].Status = Pulling
			}
		}

		// 情况 B：我们【失去】了这个分片的所有权
		if oldGID == kv.gid && newGID != kv.gid {
			if newGID != 0 {
				// 正常交接：我们不能直接删数据！必须转为 Pushing 状态，
				// 耐心等待新组把数据拉走，并发来 DeleteShard ACK 后，才能清理内存。
				kv.shards[i].Status = Pushing
			}
		}

		// 情况 C：一直属于我们 (newGID == kv.gid && oldGID == kv.gid)
		// 状态保持 Serving 不变！
		// 完美解决 Challenge 2 (Unaffected Shards)：配置变更期间，不受影响的分片继续提供读写服务。

		// 情况 D：一直不属于我们 (newGID != kv.gid && oldGID != kv.gid)
		// 状态保持 Invalid 不变。
	}

	// 3. 状态机流转完毕，正式更新本地地图
	kv.lastConfig = kv.config
	kv.config = newConfig
}

// applyInsertShard 处理从其他组拉取回来的分片数据
// ⚠️ 注意：调用此函数前，外层 applier 已经持有了 kv.mu 锁！
func (kv *ShardKV) applyInsertShard(op *Op) {
	// 1. 终极防御：版本号校验 (防止“前朝的剑斩本朝的官”)
	// 假设网络极度拥堵，这条 InsertShard(ConfigNum=3) 的日志在 Raft 里卡了很久，
	// 而当前集群早就演进到了 Config 5。如果此时不拦截，它就会把旧数据强行覆盖上去！
	if op.ConfigNum != kv.config.Num {
		return
	}

	// 2. 状态幂等性校验
	// 只有当该分片确实处于 Pulling (正在拉取) 状态时，才需要这笔数据。
	// 如果它已经是 Serving，说明可能由于网络重传等原因，这笔日志被重复提交了，直接忽略即可。
	if kv.shards[op.ShardId].Status != Pulling {
		return
	}

	// 3. 深度拷贝数据 (Deep Copy)
	// 虽然经由 Raft 的 labgob 解码后，op.Data 通常已经是独立分配的内存了，
	// 但为了 100% 杜绝并发 map 读写引发的 Panic，手动执行一轮深拷贝是最稳妥的工程实践。

	// 拷贝真实的 KV 键值对数据
	for k, v := range op.Data.KVStore {
		kv.shards[op.ShardId].KVStore[k] = v
	}

	// 拷贝客户端防重放表 (ClientSeq)
	for clientId, seq := range op.Data.ClientSeq {
		// 为了绝对的安全，如果是覆盖，可以取一个最大值 (不过正常情况下直接赋值也行，因为这个分片之前是不提供服务的)
		if lastSeq, ok := kv.shards[op.ShardId].ClientSeq[clientId]; !ok || seq > lastSeq {
			kv.shards[op.ShardId].ClientSeq[clientId] = seq
		}
	}

	// 4. 状态流转：大功告成，开门迎客！
	// 此时如果有客户端正在重试这个分片的 Get/Put 请求，下一个瞬间就会被正常处理。
	kv.shards[op.ShardId].Status = GCing

	// DPrintf("[ShardKV %d - GID %d] 成功融合分片 %d 的数据 | 当前版本 ConfigNum: %d",
	// 	kv.me, kv.gid, op.ShardId, kv.config.Num)
}

// DeleteShard 是老组接收新组发来的垃圾回收 ACK 的 RPC 接口
func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	// 1. 将删除指令封装成 Op
	op := Op{
		OpType:    "DeleteShard",
		ShardId:   args.ShardId,
		ConfigNum: args.ConfigNum,
	}

	// 2. 提交给底层 Raft 达成共识
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 3. 注册唤醒管道
	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.notifyChans[index] = ch
	kv.mu.Unlock()

	// 函数退出时务必清理管道，防止内存泄漏
	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()

	// 4. 阻塞等待应用结果或超时
	select {
	case appliedOp := <-ch:
		// 身份校验：确认应用到这个 index 的日志确实是我们刚才提交的 DeleteShard。
		// 因为内部 RPC 没有 ClientId，我们通过 OpType、ShardId 和 ConfigNum 来验明正身。
		if appliedOp.OpType == "DeleteShard" &&
			appliedOp.ShardId == args.ShardId &&
			appliedOp.ConfigNum == args.ConfigNum {
			reply.Err = OK
		} else {
			// 如果不匹配，说明之前发生了 Leader 切换，这笔日志被别人的覆盖了
			reply.Err = ErrWrongLeader
		}

	case <-time.After(500 * time.Millisecond):
		// 500ms 超时，大概率是因为发生了网络分区，当前 Leader 无法达成 majority 共识
		reply.Err = ErrTimeout
	}
}

// applyDeleteShard 老组底层 applier 执行擦除操作
func (kv *ShardKV) applyDeleteShard(op *Op) {
	// 校验版本号，防止过期的乱序 ACK 删除了不该删的数据
	if op.ConfigNum != kv.config.Num {
		return
	}

	// 只有处于 Pushing 状态的数据，才允许被擦除
	if kv.shards[op.ShardId].Status == Pushing {
		// 彻底解脱！标记为无效并安全清空内存
		kv.shards[op.ShardId].Status = Invalid
		kv.shards[op.ShardId].KVStore = make(map[string]string)
		kv.shards[op.ShardId].ClientSeq = make(map[int64]int)
	}
}

// applyGCComplete 处理新组完成垃圾回收后的状态流转
// ⚠️ 注意：调用此函数前，外层 applier 已经持有了 kv.mu 锁！
func (kv *ShardKV) applyGCComplete(op *Op) {
	// 1. 版本防御：防止网络极度拥堵时，上个配置的过期待办事项来捣乱
	if op.ConfigNum != kv.config.Num {
		return
	}

	// 2. 幂等性校验：只有状态确实是 GCing 时才需要推进。
	// 如果已经是 Serving，说明这条日志可能是重复提交的，直接忽略。
	if kv.shards[op.ShardId].Status == GCing {
		kv.shards[op.ShardId].Status = Serving

		// DPrintf("[ShardKV %d - GID %d] 分片 %d 垃圾回收彻底完成，状态变更为 Serving | ConfigNum: %d",
		// 	kv.me, kv.gid, op.ShardId, kv.config.Num)
	}
}

// =================================================================
// 快照处理相关函数 (应对 Lab 4B 的 maxraftstate 测试)
// =================================================================

// encodeSnapshot 将当前的配置和所有分片数据序列化为字节数组
// encodeSnapshot 将当前的核心状态序列化为字节数组，交给 Raft 持久化
func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// 🔴 必须按顺序打包这三个核心变量！
	e.Encode(kv.config)
	e.Encode(kv.lastConfig) // 救命变量，绝不能漏
	e.Encode(kv.shards)
	e.Encode(kv.lastApplied)

	return w.Bytes()
}

// readSnapshot 从磁盘恢复宕机前的状态
func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	// 声明干净的局部变量承接数据，防止 map 污染
	var config shardmaster.Config
	var lastConfig shardmaster.Config // 对应新增
	var shards [shardmaster.NShards]ShardData
	var lastApplied int

	// 按与 encode 完全相同的顺序解码
	if d.Decode(&config) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&shards) != nil {
		panic("ShardKV readSnapshot decode error")
	}
	// 兼容之前只保存了 config/lastConfig/shards 的旧快照。
	d.Decode(&lastApplied)

	// 整体赋值覆盖
	kv.config = config
	kv.lastConfig = lastConfig
	kv.shards = shards
	kv.lastApplied = lastApplied
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	notifyChans map[int]chan Op

	// 2. 专门用于向 ShardMaster 集群发起 RPC 调用的客户端代理
	mck *shardmaster.Clerk

	// 3. 当前副本组所知的“最新版”分片配置图
	config      shardmaster.Config
	lastConfig  shardmaster.Config // 上一版配置 (Config N-1)，专门用来找老东家要数据
	shards      [shardmaster.NShards]ShardData
	lastApplied int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// 1. 第一道防线：快速检查分片所有权 (Pre-check)
	kv.mu.Lock()
	shardId := key2shard(args.Key)
	if kv.shards[shardId].Status != Serving && kv.shards[shardId].Status != GCing {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// 2. 封装成 Op 并提交给 Raft 底层进行共识
	op := Op{
		OpType:    "Get",
		Key:       args.Key,
		ClientId:  args.ClientId, // 前提：你在 common.go 的 GetArgs 里也加上了这两个防重放字段
		CommandId: args.CommandId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 3. 注册 Channel，准备阻塞等待 applier 协程的唤醒
	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.notifyChans[index] = ch
	kv.mu.Unlock()

	// 无论正常返回还是超时，退出时务必清理注册的 channel，防止内存泄漏
	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()

	// 4. 阻塞等待：等待 Raft 达成共识或者触发网络超时
	select {
	case appliedOp := <-ch:
		// 校验是不是别人（老 Leader 的遗留日志）的回复覆盖了我们的 index
		if appliedOp.ClientId != args.ClientId || appliedOp.CommandId != args.CommandId {
			reply.Err = ErrWrongLeader
			return
		}

		// 5. 第二道防线：锁内安全读取数据 (Post-check)
		// 必须再次检查！因为在 rf.Start() 期间，分片可能刚好被打包发给别人了。
		kv.mu.Lock()
		if kv.shards[shardId].Status != Serving && kv.shards[shardId].Status != GCing {
			reply.Err = ErrWrongGroup
		} else {
			// 状态完全正常，开始去 map 里读数据
			if val, ok := kv.shards[shardId].KVStore[args.Key]; ok {
				reply.Err = OK
				reply.Value = val
			} else {
				// 如果 map 里没有这个 key，不能报错，应该返回业务级的 ErrNoKey
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		}
		kv.mu.Unlock()

	case <-time.After(500 * time.Millisecond):
		// 500ms 内底层 Raft 没有给出结果，大概率当前节点处于少数派分区，降级处理
		reply.Err = ErrTimeout
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// 1. 第一道防线：入口处的门卫校验 (快速拒绝)
	kv.mu.Lock()
	shardId := key2shard(args.Key)
	// 如果这个分片当前不属于我，或者正在拉取/推送中（不是 Serving 状态），直接拒绝！
	if kv.shards[shardId].Status != Serving && kv.shards[shardId].Status != GCing {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// 2. 封装成状态机可识别的 Op，提交给 Raft
	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 3. 注册 Channel，准备阻塞等待
	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.notifyChans[index] = ch
	kv.mu.Unlock()

	// 无论如何，函数退出时都要清理掉这个 channel，防止内存泄漏
	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()

	// 4. 阻塞等待：可能是 Raft 达成了共识，也可能是超时
	select {
	case appliedOp := <-ch:
		// 二次校验：确保当前位置执行的确实是我的这条指令（防止由于网络分区导致老 Leader 的日志被覆盖）
		if appliedOp.ClientId != args.ClientId || appliedOp.CommandId != args.CommandId {
			reply.Err = ErrWrongLeader
			return
		}

		// ⚠️ 第二道防线：出口处的亡羊补牢 (极其重要的 ShardKV 专属防御)
		// 为什么还要查一次？因为在 rf.Start() 到 <-ch 阻塞等待的这几十毫秒里，
		// 集群可能刚刚经历了一次配置升级 (Config N -> N+1)，这个分片被划给别人了！
		// 如果此时返回 OK，客户端会以为写入成功，但实际上 applier 协程早就因为状态不对而拒绝执行修改了。
		kv.mu.Lock()
		if kv.shards[shardId].Status != Serving && kv.shards[shardId].Status != GCing {
			reply.Err = ErrWrongGroup
		} else {
			reply.Err = OK
		}
		kv.mu.Unlock()

	case <-time.After(500 * time.Millisecond):
		// 500ms 内 Raft 没反应，大概率是发生了网络分区，当前节点变成了少数派的僵尸 Leader
		reply.Err = ErrTimeout
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// GCTicker 定期扫描状态为 GCing 的分片，向老组发送垃圾回收 ACK
func (kv *ShardKV) GCTicker() {
	for {
		time.Sleep(100 * time.Millisecond)

		// 只有 Leader 负责发送，防止多个 Follower 重复发送产生冗余网络包
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}

		kv.mu.Lock()
		// 1. 在锁内收集需要发送 ACK 的任务
		var tasks []GCTask

		for i := 0; i < shardmaster.NShards; i++ {
			// 寻找带有“待清理”标记的分片
			if kv.shards[i].Status == GCing {
				task := GCTask{
					shardId:   i,
					configNum: kv.config.Num,
					oldGID:    kv.lastConfig.Shards[i], // 找老东家
				}
				if servers, ok := kv.lastConfig.Groups[task.oldGID]; ok {
					task.servers = make([]string, len(servers))
					copy(task.servers, servers)
				}
				tasks = append(tasks, task)
			}
		}
		kv.mu.Unlock()

		// 2. 在锁外并发发送 RPC（绝不能带锁发网络请求！）
		var wg sync.WaitGroup
		for _, task := range tasks {
			wg.Add(1)
			go func(t GCTask) {
				defer wg.Done()
				kv.executeGCTask(t)
			}(task)
		}
		wg.Wait()
	}
}

// executeGCTask 执行具体的 ACK 发送动作
func (kv *ShardKV) executeGCTask(task GCTask) {
	args := DeleteShardArgs{
		ShardId:   task.shardId,
		ConfigNum: task.configNum,
	}

	for _, server := range task.servers {
		srv := kv.make_end(server)
		var reply DeleteShardReply

		ok := srv.Call("ShardKV.DeleteShard", &args, &reply)

		// 如果老组成功接收了我们的 ACK 并清理了数据
		if ok && reply.Err == OK {
			// 🟢 极其关键：把“清理完成”这件事记录到本地 Raft！
			// 只有达成共识后，新组才能把 GCing 状态改为 Serving，从而停止发送 ACK。
			op := Op{
				OpType:    "GCComplete",
				ShardId:   task.shardId,
				ConfigNum: task.configNum,
			}
			kv.rf.Start(op)
			break
		}
	}
}

// applier 是一个常驻的后台协程，负责不断从底层 Raft 接收已经达成共识的日志，并应用到状态机。
func (kv *ShardKV) applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			// 1. 获取并强转 Raft 提交的操作指令
			op := msg.Command.(Op)

			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			// 2. 根据不同的指令类型，执行对应的状态机逻辑
			switch op.OpType {
			case "Get", "Put", "Append":
				// 处理常规的客户端读写请求
				kv.applyClientOperation(&op)

			case "UpdateConfig":
				// 处理 ConfigTicker 轮询到的新配置
				// 在这里计算哪些分片变成了 Pulling，哪些变成了 Pushing
				kv.applyConfiguration(&op)

			case "InsertShard":
				// 处理 MigrationTicker 从其他组拉取回来的分片数据
				// 将数据合并到本地，并把对应分片状态从 Pulling 改为 Serving
				kv.applyInsertShard(&op)

			case "DeleteShard":
				// 处理 GCTicker 收到的垃圾回收 ACK
				// 安全地擦除对应的分片数据，并将状态从 Pushing 改为 Invalid
				kv.applyDeleteShard(&op)

				// 🔴 新增：处理新组收到 ACK 后的收尾工作
			case "GCComplete":
				kv.applyGCComplete(&op)
			}
			kv.lastApplied = msg.CommandIndex

			if kv.maxraftstate != -1 {
				// ⚠️ 细节 2：官方推荐使用 kv.persister 来获取精准的磁盘体积。
				// 如果你在 Raft 层自己封装了 RaftStateSize() 方法也是可以的，
				// 但标准做法是 `kv.persister.RaftStateSize()`
				if kv.rf.RaftStateSize() >= kv.maxraftstate {
					// ⚠️ 细节 3：encodeSnapshot 会读取 config 和 shards，
					// 所以此时必须确保外层仍然持有 kv.mu 锁，否则会报 Data Race 甚至 Panic！
					snapshot := kv.encodeSnapshot()
					// 通知 Raft 截断日志并保存当前快照
					kv.mu.Unlock()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
					kv.mu.Lock()
				}
			}

			// 3. 执行完毕，唤醒当时正在阻塞等待的 RPC 协程
			// 如果当前节点是 Leader，它的 RPC 协程正在 notifyChans[msg.CommandIndex] 处等待
			if ch, ok := kv.notifyChans[msg.CommandIndex]; ok {
				ch <- op
			}

			// ==========================================
			// 4. 快照检测 (应对 Lab 4B 中的 Snapshot 测试)
			// ==========================================
			// ⚠️ 细节 1：必须在持久化状态机之前检查是否开启了快照

			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			// 处理其他节点发来的 InstallSnapshot 请求
			kv.mu.Lock()
			// 只有当发来的快照比当前状态机还新时，才进行替换
			if msg.SnapshotIndex > kv.lastApplied {
				kv.readSnapshot(msg.Snapshot)
				kv.lastApplied = msg.SnapshotIndex
			}
			kv.mu.Unlock()
		}
	}
}

// MigrationTicker 定期扫描本地处于 Pulling 状态的分片，并向老组发起拉取请求
func (kv *ShardKV) MigrationTicker() {
	for {
		time.Sleep(50 * time.Millisecond) // 迁移数据的检测频率可以稍微快一点

		// 1. 只有 Leader 才有资格去拉取数据
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}

		kv.mu.Lock()

		// 2. 收集当前所有需要拉取的分片任务
		// 为什么要在锁内收集，然后去锁外发 RPC？
		// 因为发 RPC 是极其耗时的网络阻塞操作，如果带着 kv.mu 锁发 RPC，整个服务器会瞬间卡死！
		var tasks []PullTask

		for i := 0; i < shardmaster.NShards; i++ {
			if kv.shards[i].Status == Pulling {
				task := PullTask{
					shardId:   i,
					configNum: kv.config.Num,
					oldGID:    kv.lastConfig.Shards[i], // 从上一个配置中查出该分片归谁管
				}
				// 深度拷贝服务器列表，防止数据竞态
				if servers, ok := kv.lastConfig.Groups[task.oldGID]; ok {
					task.servers = make([]string, len(servers))
					copy(task.servers, servers)
				}
				tasks = append(tasks, task)
			}
		}
		kv.mu.Unlock()

		// 3. 并发执行拉取任务
		// 如果同时有 3 个分片需要从不同组拉取，并发拉取可以极大提升迁移速度
		var wg sync.WaitGroup
		for _, task := range tasks {
			// 加上这行日志
			// DPrintf("[MigrationTicker] GID %d 准备向 GID %d (Servers: %v) 拉取 Shard %d 的数据",
			// 	kv.gid, task.oldGID, task.servers, task.shardId)
			wg.Add(1)
			go func(t PullTask) {
				defer wg.Done()
				kv.executePullTask(t)
			}(task)
		}
		wg.Wait() // 等待这一轮的所有拉取动作结束
	}
}

// FetchShard 是老组（被动方）处理新组拉取分片数据的 RPC Handler
func (kv *ShardKV) FetchShard(args *FetchShardArgs, reply *FetchShardReply) {
	// 1. 只有 Leader 才能向外提供数据
	// 虽然理论上 Follower 如果配置跟上了也能给数据，但为了防止少数派 Leader 给出脏数据，统一由 Leader 处理最安全。
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 2. 检查自身的配置版本 (极其关键的时序防御)
	// 新组带着 ConfigNum = 5 来要数据，意思是：“在 Config 5 的规划里，这个分片归我了，请把历史数据给我”。
	// 如果老组当前的 kv.config.Num 只有 4，说明老组还没来得及升级配置，该分片在老组这还没被冻结！
	// 此时必须果断拒绝，让新组等会儿再来拉。
	if kv.config.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	// 3. 准备响应数据
	reply.Data = ShardData{
		KVStore:   make(map[string]string),
		ClientSeq: make(map[int64]int),
	}

	// ⚠️ 致命陷阱：必须深拷贝 (Deep Copy) ！！！
	// 在 Go 语言中，map 是引用类型。如果你直接写 `reply.Data = kv.shards[args.ShardId]`，
	// 底层的 RPC 编码器在序列化发往网络时，会直接读取 kv.shards 的物理内存。
	// 一旦此时锁被释放，后台的 applier 协程恰好去修改了这块内存，就会触发 Go 语言极其严格的
	// "concurrent map read and map write" Panic，导致服务器瞬间崩溃宕机！

	// 深度拷贝 KV 字典
	for k, v := range kv.shards[args.ShardId].KVStore {
		reply.Data.KVStore[k] = v
	}

	// 深度拷贝防重放去重表
	for clientId, seq := range kv.shards[args.ShardId].ClientSeq {
		reply.Data.ClientSeq[clientId] = seq
	}

	// 4. 大功告成
	reply.Err = OK
}

// executePullTask 执行具体的拉取动作
func (kv *ShardKV) executePullTask(task PullTask) {
	args := FetchShardArgs{
		ShardId:   task.shardId,
		ConfigNum: task.configNum, // 极其重要：告诉老组，我要的是 Config N 时刻的数据
	}

	// 遍历老组的服务器，寻找老组的 Leader
	for _, server := range task.servers {
		srv := kv.make_end(server)
		var reply FetchShardReply

		ok := srv.Call("ShardKV.FetchShard", &args, &reply)

		if ok && reply.Err == OK {
			// 🎉 成功拉取到数据！
			// 把拿到的数据包装成 InsertShard 操作，扔给本地的 Raft 达成共识
			op := Op{
				OpType:    "InsertShard",
				ShardId:   task.shardId,
				ConfigNum: task.configNum, // 防御过期乱序日志的护城河
				Data:      reply.Data,     // 包含 KVStore 和 ClientSeq
			}
			kv.rf.Start(op)
			break // 成功了就跳出循环，不用找下一台老服务器了
		}
		// 如果 reply.Err == ErrWrongLeader，继续循环试下一台机器
	}
}

// ConfigTicker 是一个常驻的后台协程，周期性地向 ShardMaster 轮询最新配置
func (kv *ShardKV) ConfigTicker() {
	// 每隔 100 毫秒轮询一次（官方推荐频率，既不影响性能，又能快速响应配置升级）
	for {
		time.Sleep(100 * time.Millisecond)

		// 💡 规则 1：只有 Leader 才有资格去拉取配置并提交。
		// 如果 Follower 也去拉取并 Start()，虽然 Raft 能处理重复日志，但纯属浪费网络和 CPU。
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}

		kv.mu.Lock()
		currentNum := kv.config.Num

		// 💡 规则 2：如果当前配置下的分片迁移还没彻底做完，绝对不能进入下一个配置！
		// 必须等所有的 Shards 都在本地变成 Serving（或 Invalid）状态。
		isReconfiguring := false
		for i := 0; i < shardmaster.NShards; i++ {
			if kv.shards[i].Status == Pulling || kv.shards[i].Status == Pushing || kv.shards[i].Status == GCing {
				isReconfiguring = true
				break
			}
		}
		kv.mu.Unlock()

		// 如果还在重配置中（等别人发数据，或者等别人发 ACK），直接跳过本次轮询，继续死等。
		if isReconfiguring {
			continue
		}

		// 💡 规则 3：极其重要！必须一版一版地严格按顺序升级。
		// 传入 currentNum + 1，而不是传 -1 去拿最新版。
		nextConfig := kv.mck.Query(currentNum + 1)

		// 如果拿到的配置确实比本地的新（即等于 currentNum + 1）
		if nextConfig.Num == currentNum+1 {
			// 把新配置打包成操作指令，扔给底层的 Raft 达成共识
			// 注意：绝对不要在这里直接修改 kv.config！必须等 applier 拿出来再改。
			op := Op{
				OpType: "UpdateConfig",
				Config: nextConfig,
			}
			kv.rf.Start(op)
		}
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	// 初始化并发控制机制
	kv.notifyChans = make(map[int]chan Op)

	// 创建用于与 ShardMaster 通信的专用客户端
	// 这个 Clerk 非常关键，后台的配置轮询协程全靠它去拿新 Config
	kv.mck = shardmaster.MakeClerk(kv.masters)

	// 初始化本地配置（默认为 Config 0，一个空配置）
	kv.config = shardmaster.Config{}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// 初始化 10 个分片的独立状态机 (Per-Shard State Machine)
	// 假设我们在 ShardKV 结构体里定义了: shards [shardmaster.NShards]ShardData
	for i := 0; i < shardmaster.NShards; i++ {
		// 每个分片拥有独立的 KV 存储、独立的去重表、以及独立的状态
		kv.shards[i] = ShardData{
			KVStore:   make(map[string]string),
			ClientSeq: make(map[int64]int),
			Status:    Invalid, // 初始时没有任何分片属于这个组，全标记为无效
		}
	}
	kv.readSnapshot(persister.ReadSnapshot())

	// 4. 启动各种后台常驻守护协程 (Daemon Goroutines)

	// a. 日志应用协程：负责从 applyCh 接收 Raft 达成的共识日志，应用到状态机，并唤醒对应的 RPC
	go kv.applier()
	// b. 配置轮询协程：负责定期向 ShardMaster 拉取新配置
	go kv.ConfigTicker()
	// c. 分片迁移协程：定期扫描处于 Pulling 状态的分片，去老组拉取数据
	go kv.MigrationTicker()

	go kv.GCTicker()

	return kv
}
