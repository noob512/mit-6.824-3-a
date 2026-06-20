package shardmaster

import "../labrpc"
import "../raft"
import "testing"
import "os"

// import "log"
import crand "crypto/rand"
import "math/rand"
import "encoding/base64"
import "sync"
import "runtime"
import "time"

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

// Randomize server handles
func random_handles(kvh []*labrpc.ClientEnd) []*labrpc.ClientEnd {
	sa := make([]*labrpc.ClientEnd, len(kvh))
	copy(sa, kvh)
	for i := range sa {
		j := rand.Intn(i + 1)
		sa[i], sa[j] = sa[j], sa[i]
	}
	return sa
}

type config struct {
	mu           sync.Mutex
	t            *testing.T
	net          *labrpc.Network
	n            int
	servers      []*ShardMaster
	saved        []*raft.Persister
	endnames     [][]string // names of each server's sending ClientEnds
	clerks       map[*Clerk][]string
	nextClientId int
	start        time.Time // time at which make_config() was called
}

func (cfg *config) checkTimeout() {
	// enforce a two minute real-time limit on each test
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}

func (cfg *config) cleanup() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for i := 0; i < len(cfg.servers); i++ {
		if cfg.servers[i] != nil {
			cfg.servers[i].Kill()
		}
	}
	cfg.net.Cleanup()
	cfg.checkTimeout()
}

// Maximum log size across all servers
func (cfg *config) LogSize() int {
	logsize := 0
	for i := 0; i < cfg.n; i++ {
		n := cfg.saved[i].RaftStateSize()
		if n > logsize {
			logsize = n
		}
	}
	return logsize
}

// attach server i to servers listed in to
// caller must hold cfg.mu
func (cfg *config) connectUnlocked(i int, to []int) {
	// log.Printf("connect peer %d to %v\n", i, to)

	// outgoing socket files
	for j := 0; j < len(to); j++ {
		endname := cfg.endnames[i][to[j]]
		cfg.net.Enable(endname, true)
	}

	// incoming socket files
	for j := 0; j < len(to); j++ {
		endname := cfg.endnames[to[j]][i]
		cfg.net.Enable(endname, true)
	}
}

func (cfg *config) connect(i int, to []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.connectUnlocked(i, to)
}

// detach server i from the servers listed in from
// caller must hold cfg.mu
func (cfg *config) disconnectUnlocked(i int, from []int) {
	// log.Printf("disconnect peer %d from %v\n", i, from)

	// outgoing socket files
	for j := 0; j < len(from); j++ {
		if cfg.endnames[i] != nil {
			endname := cfg.endnames[i][from[j]]
			cfg.net.Enable(endname, false)
		}
	}

	// incoming socket files
	for j := 0; j < len(from); j++ {
		if cfg.endnames[j] != nil {
			endname := cfg.endnames[from[j]][i]
			cfg.net.Enable(endname, false)
		}
	}
}

func (cfg *config) disconnect(i int, from []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.disconnectUnlocked(i, from)
}

func (cfg *config) All() []int {
	all := make([]int, cfg.n)
	for i := 0; i < cfg.n; i++ {
		all[i] = i
	}
	return all
}

func (cfg *config) ConnectAll() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for i := 0; i < cfg.n; i++ {
		cfg.connectUnlocked(i, cfg.All())
	}
}

// Sets up 2 partitions with connectivity between servers in each  partition.
func (cfg *config) partition(p1 []int, p2 []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	// log.Printf("partition servers into: %v %v\n", p1, p2)
	for i := 0; i < len(p1); i++ {
		cfg.disconnectUnlocked(p1[i], p2)
		cfg.connectUnlocked(p1[i], p1)
	}
	for i := 0; i < len(p2); i++ {
		cfg.disconnectUnlocked(p2[i], p1)
		cfg.connectUnlocked(p2[i], p2)
	}
}

// makeClient 创建一个专属于测试框架的客户端 (Clerk)
// 参数 to: 一个整数切片，表示这个客户端初始状态下“网络连通”的服务器 ID 列表。
// 例如传入 [0, 1]，则该客户端一开始只能和服务器 0 和 1 通信，到服务器 2 的网络会被模拟断开。
func (cfg *config) makeClient(to []int) *Clerk {
    // 加锁，因为接下来要修改测试配置对象（cfg）内部的网络映射表等共享状态
    cfg.mu.Lock()
    defer cfg.mu.Unlock() // 函数执行完毕返回前自动解锁

    // ==========================================================
    // 步骤一：为该客户端分配专属的虚拟网络端口
    // ==========================================================
    // 创建两个数组，分别用于存储网络端点对象和端点的名称
    // cfg.n 是集群中 ShardMaster 服务器的总数
    ends := make([]*labrpc.ClientEnd, cfg.n)
    endnames := make([]string, cfg.n)
    
    for j := 0; j < cfg.n; j++ {
        // 为当前客户端连接到服务器 j 生成一个随机的、长度为 20 的字符串作为“端口名”
        // 这样做是为了在模拟网络中区分不同客户端发出的请求
        endnames[j] = randstring(20)
        
        // 在模拟网络 (labrpc) 中实际创建这个发送端点
        ends[j] = cfg.net.MakeEnd(endnames[j])
        
        // 配置模拟网络的路由器：将这个新建的端点名称与目标服务器 j 连接起来
        // 也就是说，向 ends[j] 发送的数据包，会被 labrpc 投递到服务器 j
        cfg.net.Connect(endnames[j], j)
    }

    // ==========================================================
    // 步骤二：实例化客户端对象
    // ==========================================================
    // 调用你在 client.go 中实现的 MakeClerk 函数来创建客户端实例。
    // random_handles(ends) 会将网络端点的顺序打乱。
    // 【目的】：防止所有客户端启动时都默认先请求 ends[0]（即服务器 0），
    // 从而实现客户端请求在集群中的初始负载均衡。
    ck := MakeClerk(random_handles(ends))
    
    // ==========================================================
    // 步骤三：记录状态与网络拓扑控制
    // ==========================================================
    // 在配置对象中记录下这个客户端（ck）对应使用的所有网络端点名称。
    // 这对于后续测试模拟“网络分区（Network Partition）”非常重要，
    // 测试框架可以通过这些名字随时切断或恢复该客户端到特定服务器的网络连接。
    cfg.clerks[ck] = endnames
    
    // 递增全局的 Client ID 计数器（用于内部状态追踪或辅助去重逻辑）
    cfg.nextClientId++
    
    // 根据传入的参数 `to`，配置该客户端的实际网络连通性。
    // 虽然上面我们为所有 cfg.n 个服务器都创建了端点，
    // 但这个函数会把不在 `to` 列表里的服务器网络“断开”（Enable=false）。
    cfg.ConnectClientUnlocked(ck, to)
    
    // 返回初始化完毕的客户端实例
    return ck
}

func (cfg *config) deleteClient(ck *Clerk) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	v := cfg.clerks[ck]
	for i := 0; i < len(v); i++ {
		os.Remove(v[i])
	}
	delete(cfg.clerks, ck)
}

// caller should hold cfg.mu
func (cfg *config) ConnectClientUnlocked(ck *Clerk, to []int) {
	// log.Printf("ConnectClient %v to %v\n", ck, to)
	endnames := cfg.clerks[ck]
	for j := 0; j < len(to); j++ {
		s := endnames[to[j]]
		cfg.net.Enable(s, true)
	}
}

func (cfg *config) ConnectClient(ck *Clerk, to []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.ConnectClientUnlocked(ck, to)
}

// caller should hold cfg.mu
func (cfg *config) DisconnectClientUnlocked(ck *Clerk, from []int) {
	// log.Printf("DisconnectClient %v from %v\n", ck, from)
	endnames := cfg.clerks[ck]
	for j := 0; j < len(from); j++ {
		s := endnames[from[j]]
		cfg.net.Enable(s, false)
	}
}

func (cfg *config) DisconnectClient(ck *Clerk, from []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.DisconnectClientUnlocked(ck, from)
}

// Shutdown a server by isolating it
func (cfg *config) ShutdownServer(i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	cfg.disconnectUnlocked(i, cfg.All())

	// disable client connections to the server.
	// it's important to do this before creating
	// the new Persister in saved[i], to avoid
	// the possibility of the server returning a
	// positive reply to an Append but persisting
	// the result in the superseded Persister.
	cfg.net.DeleteServer(i)

	// a fresh persister, in case old instance
	// continues to update the Persister.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	kv := cfg.servers[i]
	if kv != nil {
		cfg.mu.Unlock()
		kv.Kill()
		cfg.mu.Lock()
		cfg.servers[i] = nil
	}
}

// StartServer 启动或重启集群中索引为 i 的服务器节点
// 注释提示：如果在测试中要模拟重启节点，必须先调用 ShutdownServer(i) 把旧的实例关掉
func (cfg *config) StartServer(i int) {
    // 1. 加锁保护配置对象的并发修改
    cfg.mu.Lock()

    // =========================================================
    // 步骤一：初始化网络端点名称 (Endpoint Names)
    // =========================================================
    // 为当前服务器 i 创建一套全新的“发件人名称”列表。
    // 因为这台服务器需要能给集群里的所有其他服务器（包括自己）发送 RPC
    cfg.endnames[i] = make([]string, cfg.n)
    for j := 0; j < cfg.n; j++ {
        // 使用随机生成的 20 位字符串作为端点名称，避免重启前后端点名称冲突
        // 这相当于在操作系统里给这个进程分配了一批随机的、全新的 Socket 源端口
        cfg.endnames[i][j] = randstring(20)
    }

    // =========================================================
    // 步骤二：创建网络发送端点 (ClientEnds)
    // =========================================================
    // ends 是用来传递给当前服务器底层 Raft 的。Raft 用这些 ends 来发送 AppendEntries 等 RPC。
    ends := make([]*labrpc.ClientEnd, cfg.n)
    for j := 0; j < cfg.n; j++ {
        // 让模拟网络（labrpc）生成一个绑定到上述随机名称的客户端点
        ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
        // 关键一步：将这个客户端点（源）与目标服务器 j（目的）在路由表中连接起来
        // 这样当前服务器 i 通过 ends[j] 发送的数据，模拟网络就知道该投递给服务器 j
        cfg.net.Connect(cfg.endnames[i][j], j)
    }

    // =========================================================
    // 步骤三：准备持久化存储 (Persister)
    // =========================================================
    // 为即将启动的实例准备一个全新的 Persister 对象。
    // 为什么要“全新”？为了防止旧的（可能因为宕机而悬挂的）协程突然苏醒，
    // 把旧数据写到了新实例的持久化器里，导致状态混乱。
    if cfg.saved[i] != nil {
        // 如果是重启（之前有保存的状态），则将旧状态深拷贝一份给新实例
        // 这样新启动的 Raft 节点在调用 ReadRaftState() 时就能恢复崩溃前的数据
        cfg.saved[i] = cfg.saved[i].Copy()
    } else {
        // 如果是第一次启动，创建一个空的持久化器（相当于一块刚格式化好的新磁盘）
        cfg.saved[i] = raft.MakePersister()
    }

    // 临界区操作完成，解锁
    cfg.mu.Unlock()

    // =========================================================
    // 步骤四：实例化你的服务器逻辑
    // =========================================================
    // 调用你在 server.go 中实现的 StartServer 接口
    // 传入网络端点、它自己的 ID (i) 以及刚刚准备好的“磁盘”(saved[i])
    cfg.servers[i] = StartServer(ends, i, cfg.saved[i])

    // =========================================================
    // 步骤五：注册并暴露 RPC 服务到模拟网络
    // =========================================================
    // 利用 Go 的反射机制，将服务器对象的方法封装成 RPC 服务
    
    // 1. 暴露 ShardMaster 自己的业务 RPC (Join, Leave, Move, Query)
    kvsvc := labrpc.MakeService(cfg.servers[i])
    // 2. 暴露底层的 Raft RPC (RequestVote, AppendEntries, InstallSnapshot)
    rfsvc := labrpc.MakeService(cfg.servers[i].rf)
    
    // 创建一个包含上述两个服务的模拟服务端进程
    srv := labrpc.MakeServer()
    srv.AddService(kvsvc)
    srv.AddService(rfsvc)
    
    // 将这个服务端进程以 ID i 注册到全局模拟网络中
    // 这样步骤二中其他人通过 Connect(..., i) 就能把请求发送给这个 srv 了
    cfg.net.AddServer(i, srv)
}

func (cfg *config) Leader() (bool, int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	for i := 0; i < cfg.n; i++ {
		_, is_leader := cfg.servers[i].rf.GetState()
		if is_leader {
			return true, i
		}
	}
	return false, 0
}

// Partition servers into 2 groups and put current leader in minority
func (cfg *config) make_partition() ([]int, []int) {
	_, l := cfg.Leader()
	p1 := make([]int, cfg.n/2+1)
	p2 := make([]int, cfg.n/2)
	j := 0
	for i := 0; i < cfg.n; i++ {
		if i != l {
			if j < len(p1) {
				p1[j] = i
			} else {
				p2[j-len(p1)] = i
			}
			j++
		}
	}
	p2[len(p2)-1] = l
	return p1, p2
}

// make_config 初始化并返回一个测试配置环境 (config)
// 参数 t: Go 语言的 testing.T，用于控制测试流程和报错
// 参数 n: 期望在集群中启动的 ShardMaster 服务器节点数量（通常是 3 个，构成 Raft 多数派）
// 参数 unreliable: 布尔值，标识是否模拟一个不可靠的网络（丢包、延迟、乱序）
func make_config(t *testing.T, n int, unreliable bool) *config {
    // 强制 Go 运行时至少使用 4 个操作系统线程
    // 目的：在多核环境下增加协程并发交替执行的概率，从而更容易暴露出由于未加锁导致的并发数据竞争（Data Race）Bug
    runtime.GOMAXPROCS(4)
    
    // 初始化配置对象
    cfg := &config{}
    cfg.t = t
    
    // 创建一个模拟的本地网络 (labrpc.Network)
    // 所有的 RPC 通信都不会走真实的 TCP/IP，而是通过这个模拟网络在内存中的 channel 传递
    cfg.net = labrpc.MakeNetwork()
    
    cfg.n = n
    
    // 初始化各类切片，长度均为节点数量 n
    cfg.servers = make([]*ShardMaster, cfg.n)      // 存放 ShardMaster 服务器实例的指针
    cfg.saved = make([]*raft.Persister, cfg.n)     // 存放每个节点模拟的“持久化存储”状态（用于模拟磁盘）
    cfg.endnames = make([][]string, cfg.n)         // 存放每个服务器的各个网络端点（Endpoint）名称
    
    // 初始化用于记录客户端和其关联网络端点的映射表
    cfg.clerks = make(map[*Clerk][]string)
    
    // 初始化全局递增的 ClientID 起始值
    // 故意加上 1000，是为了确保生成的 Client ID 绝对不会和 Server ID（0, 1, 2...）冲突
    // 这对于客户端生成唯一的 RPC 请求 ID（去重机制）至关重要
    cfg.nextClientId = cfg.n + 1000 
    
    // 记录测试开始时间，通常用于限制测试运行的最大时长（超时控制）
    cfg.start = time.Now()

    // 循环启动全部的 n 个 ShardMaster 服务器节点
    // StartServer 内部会实例化 Raft 和 ShardMaster，并将它们绑定到模拟网络的端点上
    for i := 0; i < cfg.n; i++ {
        cfg.StartServer(i)
    }

    // 将所有服务器的网络端点互相连接起来，相当于把所有机器插到了同一个交换机上，网络全通
    cfg.ConnectAll()

    // 根据传入的 unreliable 参数，设置模拟网络是否丢包/延迟
    // cfg.net.Reliable(true) 表示网络畅通无阻
    // cfg.net.Reliable(false) 表示网络极其恶劣，测试你的超时重传和 Raft 选举机制
    cfg.net.Reliable(!unreliable)

    // 返回配置好的测试沙盒环境
    return cfg
}
