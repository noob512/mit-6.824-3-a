package kvraft

import (
	//"encoding/gob"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// 这些处理函数应使用 Start() 将一个 Op（操作）写入 Raft 日志；
// 你需要在 server.go 中完善 Op 结构体的定义，使其能够描述一个 Put/Append/Get 操作。
// 每个服务器应在 Raft 提交（commit）这些 Op 命令时（即它们出现在 applyCh 通道中时）依次执行它们。
// RPC 处理函数应能检测到 Raft 已提交它自己的 Op，然后向客户端返回响应。

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string
	Key string
	Value string
	Pos int
	RealServer int
	Commiter int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	kvstore map[string]string
	Changed  int
	Getch chan bool
	Readych chan bool
	CommittedStore map[int64]int
	RealServer map[int64]int
	// 🌟 新增：每个日志 index 对应一个通知 channel，用于通知 RPC 协程日志已提交
    notifyChs      map[int]chan Op
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    // ==========================================
    // 阶段 1：初始化操作并提交给底层的 Raft
    // ==========================================
    kv.mu.Lock()
    
    RaftOp := new(Op)
    RaftOp.Key = args.Key
    RaftOp.Op = "Get"
    RaftOp.Pos = args.RealPos           // 🌟 记录该请求的唯一序列号
    RaftOp.Commiter = args.Commiter // 🌟 记录客户端的唯一ID，用于最后的位置校验
    
    kv.RealServer[args.Commiter] = args.RealPos
    kv.mu.Unlock()

    // 将 Get 操作作为一个日志提案扔给 Raft。
    // 在分布式线性一致性要求下，读操作同样需要经历 Raft 共识确认，以防止读到过期的 Leader。
    index, _, isLeader := kv.rf.Start(*RaftOp)
    
    if !isLeader {
        reply.Err = ErrWrongLeader
        return
    }

    // ==========================================
    // 阶段 2：创建专属通道，精准阻塞等待
    // ==========================================
    kv.mu.Lock()
    // 为当前的日志 index 创建一个私有的通知通道
    ch := make(chan Op, 1)
    kv.notifyChs[index] = ch
    kv.mu.Unlock()

    // 【内存防泄漏】无论该 RPC 最终是成功、超时还是由于中途节点换届失败，
    // 退出函数时必须从 map 中将这个通道删除，否则伴随大量请求，内存会发生 OOM（溢出崩溃）。
    defer func() {
        kv.mu.Lock()
        delete(kv.notifyChs, index)
        kv.mu.Unlock()
    }()

    // ==========================================
    // 阶段 3：等待状态机唤醒并读取数据
    // ==========================================
    select {
    case appliedOp := <-ch:
        // 🌟 【核心安全性校验】
        // 检查从状态机里被应用出来的操作，是不是我们当时发过去的那个客户端的那个请求。
        // 如果是，说明直到该日志被应用的那一刻，我们依然是全网合法的 Leader，当前数据是最新的。
        if appliedOp.Commiter == RaftOp.Commiter && appliedOp.Pos == RaftOp.Pos {
            kv.mu.Lock()
            
            // 从当前的内存数据库中读取最新值
            val, exists := kv.kvstore[args.Key]
            if exists {
                reply.Value = val
                reply.Err = OK
            } else {
                reply.Value = ""
                reply.Err = ErrNoKey // 如果 key 不存在，返回特定错误码
            }
            
            kv.mu.Unlock()
        } else {
            // 如果 Commiter 或 Pos 对不上，说明在我们排队期间，当前节点发生过分区或任期更替，
            // 该 index 上的日志被新 Leader 用其他写请求“鸠占鹊巢”覆盖了，当前节点已不再是合法 Leader。
            reply.Err = ErrWrongLeader
        }
        
    case <-time.After(500 * time.Millisecond):
        // 🌟 【防死锁超时机制】
        // 如果遭遇网络分区，Raft 迟迟无法达成多数派，该 index 永远不会被 Apply。
        // 设置 500ms 超时，及时放客户端去其他可用的 Leader 节点重试。
        reply.Err = ErrWrongLeader
    }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    // ==========================================
    // 阶段 1：请求前置检查与幂等性（去重）判断
    // ==========================================
    kv.mu.Lock() // 🔒 加锁 1：保护内部状态机元数据
    
    RaftOp := new(Op)
    RaftOp.Key = args.Key
    RaftOp.Value = args.Value
    RaftOp.Op = args.Op          
    RaftOp.Pos = args.Pos         
    RaftOp.Commiter = args.Commiter 

    kv.RealServer[args.Commiter] = args.RealPos
    
    // 【核心去重逻辑】
    if RaftOp.Pos <= kv.CommittedStore[RaftOp.Commiter] {
        DPrintf("RaftOp.Pos为%v且主机%v中kv.CommittedStore[%v]为%v", RaftOp.Pos, kv.me, kv.RealServer[args.Commiter], kv.CommittedStore[RaftOp.Commiter])
        reply.Err = OK
        kv.mu.Unlock() // 🔓 释放 1：去重成功，直接释放并返回
        return
    }
    kv.mu.Unlock() // 🔓 释放 1：检查完毕，调用 rf.Start 前必须释放锁
    
    // ==========================================
    // 阶段 2：提交给底层的 Raft 集群
    // ==========================================
    index, _, isLeader := kv.rf.Start(*RaftOp)
    if !isLeader {
        reply.Err = ErrWrongLeader
        return // 不是 Leader 直接返回，此时身上没有任何锁，安全
    }
    
    // ==========================================
    // 阶段 3：创建私有通知通道
    // ==========================================
    kv.mu.Lock() // 🔒 加锁 2：向全局 map 注册专属通道
    ch := make(chan Op, 1)
    kv.notifyChs[index] = ch
    kv.mu.Unlock() // 🔓 释放 2：挂载完毕立刻放锁，让出 CPU 给 Apply 协程执行

    // 【防内存泄漏延迟清理】
    defer func() {
        kv.mu.Lock()
        delete(kv.notifyChs, index)
        kv.mu.Unlock()
    }()

    // ==========================================
    // 阶段 4：阻塞等待状态机通知
    // ==========================================
    select {
    case appliedOp := <-ch:
        // 校验： index 对应的操作是不是我们当时发过去的那个
        if appliedOp.Commiter == RaftOp.Commiter && appliedOp.Pos == RaftOp.Pos {
            reply.Err = OK
        } else {
            // 被覆盖了，说明中途丢失了 Leadership
            reply.Err = ErrWrongLeader
        }
        
    case <-time.After(500 * time.Millisecond): 
        // 500ms 超时未提交，判定为网络分区，让客户端换节点重试
        reply.Err = ErrWrongLeader 
    } 

    // 🌟 原先末尾多余的 reply.Err = OK 和 kv.mu.Unlock() 已被彻底剔除！
}

// 测试器在KVServer实例不再需要时调用Kill()。
// 为了方便起见，我们提供了设置rf.dead的代码
// （无需加锁），以及在长时间运行的循环中测试
// rf.dead的killed()方法。你也可以在Kill()中
// 添加自己的代码。这不是必需的，但可能很方便
// （例如）来抑制被Kill()的实例的调试输出。
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) Apply() {
    applyMsg := raft.ApplyMsg{}
    i := 0
    
    // 后台长驻循环，等待 Raft 提交日志
    for {
        // 1. 阻塞等待（此处严禁加锁）
        applyMsg = <-kv.applyCh
        
        // 2. 收到消息后立刻加锁，安全修改内部数据库
        kv.mu.Lock()
        i++
        
        // 处理有效的用户命令
        if applyMsg.CommandValid && applyMsg.CommandIndex != 0 {
            op := applyMsg.Command.(Op)
            index := applyMsg.CommandIndex // 拿到当前日志在 Raft 中的唯一索引
            
            op.RealServer = kv.RealServer[op.Commiter]
            DPrintf("主机%v apply Op {Op: %v, Key: %v, Value: %v, Pos: %v, Index: %v}\n", kv.me, op.Op, op.Key, op.Value, op.Pos, index)
            
            // 3. 根据操作类型修改状态机（Put / Append 需要去重更新，Get 啥都不用干）
            if op.Op == "Put" && op.Pos > kv.CommittedStore[op.Commiter] {
                kv.kvstore[op.Key] = op.Value
                kv.CommittedStore[op.Commiter] = op.Pos
                
            } else if op.Op == "Append" && op.Pos > kv.CommittedStore[op.Commiter] {
                kv.kvstore[op.Key] += op.Value
                kv.CommittedStore[op.Commiter] = op.Pos
                DPrintf("kv.kvstore[%v]Append之后为：%v", op.Key, kv.kvstore[op.Key])
            }
            
            // 4. 【核心融合点：精准通知】
            // 无论是 Put, Append 还是 Get，只要完成了上面的状态更新（或确认），
            // 都要立刻检查当前 index 是否有对应的 RPC 协程在阻塞等待通知。
            if ch, ok := kv.notifyChs[index]; ok {
                // 将当前在状态机里跑出来的 op 顺着管道推过去。
                // 此时还在锁内，能够保证通道发送的绝对原子性，且不会存在并发抢跑。
                ch <- op 
            }
        }
        
        // 5. 解锁，迎接下一条日志
        kv.mu.Unlock()
    }
}

// servers[] 包含将通过Raft协作形成容错键值服务的服务器端口集合。
// me 是当前服务器在servers[]中的索引。
// 键值服务器应该通过底层Raft实现存储快照，
// Raft应该调用persister.SaveStateAndSnapshot()来原子性地保存Raft状态和快照。
// 当Raft的保存状态超过maxraftstate字节时，键值服务器应该创建快照，
// 以便允许Raft进行日志垃圾回收。如果maxraftstate为-1，则不需要创建快照。
// StartKVServer()必须快速返回，所以它应该为任何长时间运行的工作启动goroutines。
//
// StartKVServer 创建并启动一个KV服务器
// servers: 集群中所有服务器的网络连接端点数组，用于Raft协议通信
// me: 当前服务器的唯一编号ID [0, n-1]
// persister: 持久化器，保存Raft状态(日志、任期、投票信息)
// maxraftstate: 最大Raft状态大小限制，用于日志压缩(-1为无限制)
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
    // 对你希望 Go 的 RPC 库进行序列化/反序列化的结构体调用 labgob.Register。
    // 在 Raft 将 Op 结构体作为 Command 传递时，底层编码库必须先注册该类型。
    labgob.Register(Op{}) 

    kv := new(KVServer)            // 创建新的 KV 服务器实例
    kv.me = me                     // 设置当前服务器在集群中的 ID
    kv.maxraftstate = maxraftstate // 设置最大 Raft 状态大小限制（Lab 3B 快照会用到）
    kv.Changed = 1

    // ==========================================
    // 基础状态机（数据库相关）的内存初始化
    // ==========================================
    kv.kvstore = make(map[string]string)   // 初始化真实的键值存储映射（底层的 KV 数据库）
    kv.RealServer = make(map[int64]int)    // 初始化调试映射表（客户端 ID -> 物理服务器位置）

    // 🌟 修正：将去重表的 value 类型保持与 Op.Pos 一致（建议统一使用 int64）
    // 用于记录每个客户端（int64）已经成功应用到状态机的最大操作序列号（int64），保证幂等性
    kv.CommittedStore = make(map[int64]int) 

    // ==========================================
    // 🌟 核心修复：精准通知通道表的初始化
    // ==========================================
    // 必须在此处显式 make，彻底根治 "panic: assignment to entry in nil map" 错误！
    // 键为 Raft 提交通知的 Log Index，值为对应的阻塞 RPC 通道
    kv.notifyChs = make(map[int]chan Op)

    // ==========================================
    // 底层分布式共识层（Raft）的初始化与协同
    // ==========================================
    // 创建应用消息通道，带缓冲（这里设为 1500）能有效防止 Raft 层和 KVServer 层因并发产生死锁
    kv.applyCh = make(chan raft.ApplyMsg, 1500) 
    
    // 启动底层的 Raft 节点，并将统一的 applyCh 传递给它
    kv.rf = raft.Make(servers, me, persister, kv.applyCh) 

    // ==========================================
    // 启动后台长驻协程
    // ==========================================
    // 启动负责监听底层 Raft 提交日志的状态机应用协程
    go kv.Apply()

    return kv // 返回初始化完毕的 KV 服务器实例指针
}
