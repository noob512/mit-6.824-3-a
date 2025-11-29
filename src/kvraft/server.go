package kvraft

import (
	//"encoding/gob"
	"log"
	"sync"
	"sync/atomic"
	//"time"

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
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//DPrintf("主机%v 在Get()处想要获得锁\n",kv.me)
	kv.mu.Lock()
	//DPrintf("主机%v 在Get()处成功获得锁\n",kv.me)
	RaftOp:=new(Op)
	RaftOp.Key=args.Key
	RaftOp.Op="Get"
	kv.RealServer[args.Commiter]=args.RealPos
	kv.mu.Unlock()
	//DPrintf("主机%v 在Get()处成功解锁\n",kv.me)
	_, _, isLeader := kv.rf.Start(*RaftOp)
	//DPrintf("主机%v 从start后准备获得锁\n",kv.me)
	//kv.mu.Lock()
	//DPrintf("主机%v 从start后成功获得锁\n",kv.me)
	if !isLeader {
		reply.Err = ErrWrongLeader
		//kv.mu.Unlock()
		//DPrintf("主机%v 从start后成功解锁\n",kv.me)
		return
	}
	//kv.mu.Unlock()
	//DPrintf("主机%v 从start后成功解锁并在readych处阻塞\n",kv.me)
	kv.Readych<-true
	//DPrintf("RaftOp.Key:%v的请求通过%v通道处\n",RaftOp.Key,kv.me)
	//DPrintf("主机%v 通过readych处\n",kv.me)
	//kv.mu.Lock()
	reply.Err = OK
	// 等待Raft提交
	//kv.mu.Unlock()
	//DPrintf("主机%v 从readych后成功解锁并阻塞在Getch处\n",kv.me)
	//DPrintf("RaftOp.Key:%v的请求阻塞在%v通道处\n",RaftOp.Key,kv.me)
	_ = <-kv.Getch  // 使用匿名变量接收并丢弃
	//DPrintf("RaftOp.Key:%v的请求通过%v通道处\n",RaftOp.Key,kv.me)
	//DPrintf("主机%v 从kv.Getch准备获得锁\n",kv.me)
	kv.mu.Lock()
	//DPrintf("RaftOp.Key:%v\n",RaftOp.Key)
	//DPrintf("主机%v 从kv.Getch成功获得锁\n",kv.me)
	reply.Value = kv.kvstore[args.Key]
	//DPrintf("args.Key:%v,reply.Value:%v\n",args.Key,reply.Value)
	kv.mu.Unlock()
	//DPrintf("主机%v 从kv.Getch成功解锁\n",kv.me)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//DPrintf("主机%v 在PutAppend()处想要获得锁\n",kv.me)
	kv.mu.Lock()
	//DPrintf("主机%v 在PutAppend()处成功获得锁\n",kv.me)
	RaftOp:=new(Op)
	RaftOp.Key=args.Key
	RaftOp.Value=args.Value
	RaftOp.Op=args.Op
	RaftOp.Pos=args.Pos
	RaftOp.Commiter=args.Commiter
	kv.RealServer[args.Commiter]=args.RealPos
	// if RaftOp.Op=="Put"{
	// 	kv.CommittedStore[RaftOp.Commiter]=0
	// 	DPrintf("主机%v中kv.CommittedStore[%v]为%v",kv.me,RaftOp.Commiter,kv.CommittedStore[RaftOp.Commiter])
	// }
	if  RaftOp.Pos<=kv.CommittedStore[RaftOp.Commiter] {
		DPrintf("RaftOp.Pos为%v且主机%v中kv.CommittedStore[%v]为%v",RaftOp.Pos,kv.me,kv.RealServer[args.Commiter],kv.CommittedStore[RaftOp.Commiter])
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	//DPrintf("KVServer %d PutAppend %v %v %v\n", kv.me, args.Key, args.Value, args.Op)
	kv.mu.Unlock()
	//DPrintf("主机%v 在PutAppend()处成功解锁\n",kv.me)
	_, _, isLeader := kv.rf.Start(*RaftOp)
	//DPrintf("主机%v 从put-start后准备获得锁\n",kv.me)
	kv.mu.Lock()
	//DPrintf("主机%v 从put-start后成功获得锁\n",kv.me)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		//DPrintf("主机%v 从put-start后成功解锁\n",kv.me)
		return
	}
	reply.Err = OK
	kv.mu.Unlock()
	//DPrintf("主机%v 从put-start后成功解锁\n",kv.me)
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
	applyMsg:=raft.ApplyMsg{}
	i:=0
	// 等待Raft提交
	for  {
		applyMsg = <-kv.applyCh
		kv.mu.Lock()
		i++
		if applyMsg.CommandValid&&applyMsg.CommandIndex!=0 {
			op := applyMsg.Command.(Op)
			op.RealServer=kv.RealServer[op.Commiter]
			DPrintf("主机%v apply Op {Op: %v, Key: %v, Value: %v, Pos: %v, RealServer: %v}\n", kv.me, op.Op, op.Key, op.Value, op.Pos, op.RealServer)
			if op.Op == "Put"&& op.Pos>kv.CommittedStore[op.Commiter] {
				//DPrintf("kv.kvstore[%v]原先为：%v",op.Key,kv.kvstore[op.Key])
				kv.kvstore[op.Key] = op.Value
				//DPrintf("kv.kvstore[%v]Put之后为：%v",op.Key,kv.kvstore[op.Key])
				kv.CommittedStore[op.Commiter]=op.Pos
			} else if op.Op == "Append" && op.Pos>kv.CommittedStore[op.Commiter] {
				//DPrintf("kv.kvstore[%v]原先为：%v",op.Key,kv.kvstore[op.Key])
				kv.kvstore[op.Key] += op.Value
				DPrintf("kv.kvstore[%v]Append之后为：%v",op.Key,kv.kvstore[op.Key])
				kv.CommittedStore[op.Commiter]=op.Pos
				DPrintf("kv.CommittedStore[%v]为%v",kv.RealServer[op.Commiter],kv.CommittedStore[op.Commiter])
			} else if op.Op == "Get" {
				select{
					case <-kv.Readych:
						//DPrintf("主机%d:即将进入通道操作",kv.me)
						kv.mu.Unlock()
						kv.Getch<-true//现在主要的问题好像是之前Get操作导致后续Get操作提前完成
						kv.mu.Lock()
						//DPrintf("主机%d:成功进入通道操作",kv.me)
					default:
						//DPrintf("主机%d:即将进入default",kv.me)
				}
			}
		}
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
	// 对你希望Go的RPC库进行序列化/反序列化的结构体调用labgob.Register。
	labgob.Register(Op{}) // 注册Op结构体，用于RPC序列化

	kv := new(KVServer) // 创建新的KV服务器实例
	kv.me = me          // 设置服务器ID
	kv.maxraftstate = maxraftstate // 设置最大Raft状态大小限制
	kv.Getch=make(chan bool)
	kv.Readych=make(chan bool)
	kv.Changed=1

	kv.kvstore = make(map[string]string) // 初始化键值存储映射
	kv.CommittedStore=make(map[int64]int)
	kv.RealServer=make(map[int64]int)

	// 你可能需要在这里初始化代码。
	kv.applyCh = make(chan raft.ApplyMsg,1500) // 创建应用消息通道，用于接收Raft提交的日志
	kv.rf = raft.Make(servers, me, persister, kv.applyCh) // 创建Raft实例
	go kv.Apply()

	// 你可能需要在这里初始化代码。

	return kv // 返回KV服务器实例（注意：这里缺少了重要的goroutine启动）
}
