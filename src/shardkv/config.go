package shardkv

import "../shardmaster"
import "../labrpc"
import "testing"
import "os"

// import "log"
import crand "crypto/rand"
import "math/big"
import "math/rand"
import "encoding/base64"
import "sync"
import "runtime"
import "../raft"
import "strconv"
import "fmt"
import "time"

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
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

type group struct {
	gid       int
	servers   []*ShardKV
	saved     []*raft.Persister
	endnames  [][]string
	mendnames [][]string
}

type config struct {
	mu    sync.Mutex
	t     *testing.T
	net   *labrpc.Network
	start time.Time // time at which make_config() was called

	nmasters      int
	masterservers []*shardmaster.ShardMaster
	mck           *shardmaster.Clerk

	ngroups int
	n       int // servers per k/v group
	groups  []*group

	clerks       map[*Clerk][]string
	nextClientId int
	maxraftstate int
}

func (cfg *config) checkTimeout() {
	// enforce a two minute real-time limit on each test
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}

func (cfg *config) cleanup() {
	for gi := 0; gi < cfg.ngroups; gi++ {
		cfg.ShutdownGroup(gi)
	}
	cfg.net.Cleanup()
	cfg.checkTimeout()
}

// check that no server's log is too big.
func (cfg *config) checklogs() {
	for gi := 0; gi < cfg.ngroups; gi++ {
		for i := 0; i < cfg.n; i++ {
			raft := cfg.groups[gi].saved[i].RaftStateSize()
			snap := len(cfg.groups[gi].saved[i].ReadSnapshot())
			if cfg.maxraftstate >= 0 && raft > 8*cfg.maxraftstate {
				cfg.t.Fatalf("persister.RaftStateSize() %v, but maxraftstate %v",
					raft, cfg.maxraftstate)
			}
			if cfg.maxraftstate < 0 && snap > 0 {
				cfg.t.Fatalf("maxraftstate is -1, but snapshot is non-empty!")
			}
		}
	}
}

// master server name for labrpc.
func (cfg *config) mastername(i int) string {
	return "master" + strconv.Itoa(i)
}

// shard server name for labrpc.
// i'th server of group gid.
func (cfg *config) servername(gid int, i int) string {
	return "server-" + strconv.Itoa(gid) + "-" + strconv.Itoa(i)
}

// makeClient 创建并返回一个新的客户端对象 (Clerk)。
// 测试框架（或真实的外部应用程序）通过调用这个客户端对象来发送 Get, Put, Append 请求。
func (cfg *config) makeClient() *Clerk {
    // 1. 加锁保护测试框架的内部状态（防止并发创建客户端时产生竞态）
    cfg.mu.Lock()
    defer cfg.mu.Unlock()

    // ==========================================
    // 2. 构建与 ShardMaster (控制面) 通信的 RPC 端点
    // ==========================================
    // ends: 存放所有 ShardMaster 节点的 RPC 客户端代理
    ends := make([]*labrpc.ClientEnd, cfg.nmasters)
    // endnames: 记录这些网络端点的随机名称，方便测试框架后续用来模拟“断网”
    endnames := make([]string, cfg.n) 
    
    // 遍历创建连接到每一个 ShardMaster 的网络通路
    for j := 0; j < cfg.nmasters; j++ {
        endnames[j] = randstring(20)                 // 随机生成一个虚拟网卡/端口号
        ends[j] = cfg.net.MakeEnd(endnames[j])       // 在模拟网络中创建一个端点
        cfg.net.Connect(endnames[j], cfg.mastername(j)) // 将该端点通过网线连向第 j 个 Master 服务器
        cfg.net.Enable(endnames[j], true)            // 启用该网线（允许通信）
    }

    // ==========================================
    // 3. 实例化 Clerk 并注入动态路由函数 (极其核心)
    // ==========================================
    // MakeClerk 是你在 client.go 里需要自己去实现的函数。
    // 第一个参数 ends: 让 Clerk 知道去哪里找 ShardMaster 要配置。
    // 第二个参数 func(...): 这是一个网络连接的【工厂函数】！
    ck := MakeClerk(ends, func(servername string) *labrpc.ClientEnd {
        // 为什么需要这个函数？
        // 因为 ShardMaster 返回的 Config 结构体里，Groups 字典的值只是字符串类型的主机名（如 "Server-A"）。
        // 客户端拿到主机名后，不能直接发送 RPC，必须转换成 labrpc.ClientEnd 对象才能调用 .Call()。
        // 这个闭包函数就是交给 Clerk 的“地址解析器”：只要 Clerk 传进来一个名字，它就当场建立一条网络连接。
        
        name := randstring(20)                        // 生成客户端发件人的随机端口名
        end := cfg.net.MakeEnd(name)                  // 创建发件端点
        cfg.net.Connect(name, servername)             // 连接到目标 ShardKV 服务器
        cfg.net.Enable(name, true)                    // 启用连接
        return end                                    // 返回这个建好的 RPC 通道给 Clerk 使用
    })

    // ==========================================
    // 4. 测试框架内部追踪机制
    // ==========================================
    // 记录下这个新客户端所用到的所有 Master 网络端点，方便在网络分区测试中对它进行精确打击
    cfg.clerks[ck] = endnames
    
    // 递增全局 ID 计数器，保证下一个被创建的客户端拥有不同的 ClientId
    cfg.nextClientId++
    
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

// Shutdown i'th server of gi'th group, by isolating it
func (cfg *config) ShutdownServer(gi int, i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	gg := cfg.groups[gi]

	// prevent this server from sending
	for j := 0; j < len(gg.servers); j++ {
		name := gg.endnames[i][j]
		cfg.net.Enable(name, false)
	}
	for j := 0; j < len(gg.mendnames[i]); j++ {
		name := gg.mendnames[i][j]
		cfg.net.Enable(name, false)
	}

	// disable client connections to the server.
	// it's important to do this before creating
	// the new Persister in saved[i], to avoid
	// the possibility of the server returning a
	// positive reply to an Append but persisting
	// the result in the superseded Persister.
	cfg.net.DeleteServer(cfg.servername(gg.gid, i))

	// a fresh persister, in case old instance
	// continues to update the Persister.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if gg.saved[i] != nil {
		gg.saved[i] = gg.saved[i].Copy()
	}

	kv := gg.servers[i]
	if kv != nil {
		cfg.mu.Unlock()
		kv.Kill()
		cfg.mu.Lock()
		gg.servers[i] = nil
	}
}

func (cfg *config) ShutdownGroup(gi int) {
	for i := 0; i < cfg.n; i++ {
		cfg.ShutdownServer(gi, i)
	}
}

// StartServer 启动第 gi 个副本组（group）中的第 i 个 ShardKV 服务器节点
// gi: cfg.groups 数组的索引（代表它是哪个副本组，如 0, 1, 2）
// i: 该副本组内部服务器的编号（如 0, 1, 2，对应 Raft 里的 me 参数）
func (cfg *config) StartServer(gi int, i int) {
    cfg.mu.Lock()

    // 拿到当前服务器所属的组对象
    gg := cfg.groups[gi]

    // ==========================================
    // 1. 配置组内通信网络 (Intra-group network)
    // ==========================================
    // 这台服务器需要和它【同一个组内】的其他兄弟节点通信（跑 Raft 协议，发 AppendEntries 等）。
    gg.endnames[i] = make([]string, cfg.n)
    for j := 0; j < cfg.n; j++ {
        gg.endnames[i][j] = randstring(20) // 随机生成本节点连接到组内节点 j 的发件端口名
    }

    ends := make([]*labrpc.ClientEnd, cfg.n)
    for j := 0; j < cfg.n; j++ {
        ends[j] = cfg.net.MakeEnd(gg.endnames[i][j])
        // 连线：将当前端口连接到同组第 j 个节点的目标地址（cfg.servername 生成形如 "ShardKV-100-j" 的名字）
        cfg.net.Connect(gg.endnames[i][j], cfg.servername(gg.gid, j))
        cfg.net.Enable(gg.endnames[i][j], true)
    }

    // ==========================================
    // 2. 配置与控制面的通信网络 (Master network)
    // ==========================================
    // ShardKV 节点需要定期轮询 ShardMaster 获取最新 Config。
    // mends 就是这台服务器用来向 ShardMaster 发送 Query RPC 的网络通道。
    mends := make([]*labrpc.ClientEnd, cfg.nmasters)
    gg.mendnames[i] = make([]string, cfg.nmasters)
    for j := 0; j < cfg.nmasters; j++ {
        gg.mendnames[i][j] = randstring(20)
        mends[j] = cfg.net.MakeEnd(gg.mendnames[i][j])
        cfg.net.Connect(gg.mendnames[i][j], cfg.mastername(j))
        cfg.net.Enable(gg.mendnames[i][j], true)
    }

    // ==========================================
    // 3. 配置持久化存储 (Storage / Persister)
    // ==========================================
    // 模拟真实的硬盘。
    if gg.saved[i] != nil {
        // 如果 saved[i] 不为空，说明这是一个被 Shutdown 后又重新 Start 的节点（模拟宕机重启）。
        // 必须 Copy 一份旧的持久化数据（Raft日志、Term、快照），防止测试并发访问产生竞态。
        gg.saved[i] = gg.saved[i].Copy()
    } else {
        // 如果是全新启动的节点，给一块格式化后的新“硬盘”。
        gg.saved[i] = raft.MakePersister()
    }
    
    // 准备工作完成，解锁。注意：必须在调用真实的 StartServer 前解锁，防止死锁。
    cfg.mu.Unlock()

    // ==========================================
    // 4. 调用你的代码启动服务器！
    // ==========================================
    // 这是真正调用你写在 shardkv/server.go 里的 StartServer 函数。
    gg.servers[i] = StartServer(
        ends,             // 组内通信的 Raft peers
        i,                // 当前节点在组内的编号 (me)
        gg.saved[i],      // 持久化存储对象
        cfg.maxraftstate, // 快照阈值
        gg.gid,           // 当前节点所属的 GID（极其重要，用于判断自己是否负责某个分片）
        mends,            // 与 ShardMaster 通信的代理
        
        // 【核心注入】：动态跨组 RPC 创建函数 (make_end)
        // 在分片迁移时，你需要从 GID=100 向 GID=200 发送数据。
        // 由于你在启动时并不知道 GID=200 的机器地址，所以测试框架给你注入了这个闭包。
        // 你在需要时只需调用 make_end("ShardKV-200-0")，它就会立刻在底层为你拉好一条网线。
        func(servername string) *labrpc.ClientEnd {
            name := randstring(20)
            end := cfg.net.MakeEnd(name)
            cfg.net.Connect(name, servername)
            cfg.net.Enable(name, true)
            return end
        },
    )

    // ==========================================
    // 5. 挂载 RPC 服务接口
    // ==========================================
    // 把你的 ShardKV 实例和它内部的 Raft 实例，包装成可以接收网络调用的 RPC 服务。
    kvsvc := labrpc.MakeService(gg.servers[i]) 
    rfsvc := labrpc.MakeService(gg.servers[i].rf)
    
    srv := labrpc.MakeServer()
    srv.AddService(kvsvc) // 注册 ShardKV 接口 (处理 Get, Put, Append, 分片迁移等)
    srv.AddService(rfsvc) // 注册 Raft 接口 (处理 RequestVote, AppendEntries 等)
    
    // 最终：把这个大集成的服务器，以 "ShardKV-GID-i" 的名字插到模拟交换机 (cfg.net) 上，正式上线营业！
    cfg.net.AddServer(cfg.servername(gg.gid, i), srv)
}

func (cfg *config) StartGroup(gi int) {
	for i := 0; i < cfg.n; i++ {
		cfg.StartServer(gi, i)
	}
}

func (cfg *config) StartMasterServer(i int) {
	// ClientEnds to talk to other master replicas.
	ends := make([]*labrpc.ClientEnd, cfg.nmasters)
	for j := 0; j < cfg.nmasters; j++ {
		endname := randstring(20)
		ends[j] = cfg.net.MakeEnd(endname)
		cfg.net.Connect(endname, cfg.mastername(j))
		cfg.net.Enable(endname, true)
	}

	p := raft.MakePersister()

	cfg.masterservers[i] = shardmaster.StartServer(ends, i, p)

	msvc := labrpc.MakeService(cfg.masterservers[i])
	rfsvc := labrpc.MakeService(cfg.masterservers[i].Raft())
	srv := labrpc.MakeServer()
	srv.AddService(msvc)
	srv.AddService(rfsvc)
	cfg.net.AddServer(cfg.mastername(i), srv)
}

func (cfg *config) shardclerk() *shardmaster.Clerk {
	// ClientEnds to talk to master service.
	ends := make([]*labrpc.ClientEnd, cfg.nmasters)
	for j := 0; j < cfg.nmasters; j++ {
		name := randstring(20)
		ends[j] = cfg.net.MakeEnd(name)
		cfg.net.Connect(name, cfg.mastername(j))
		cfg.net.Enable(name, true)
	}

	return shardmaster.MakeClerk(ends)
}

// join 是一个简便的包装函数，用于向 ShardMaster 报告【单个】新副本组的加入。
// gi: 副本组在配置框架中的内部索引（不是 GID，是 cfg.groups 数组的下标，例如 0, 1, 2）
func (cfg *config) join(gi int) {
    // 将单个索引打包成切片，调用支持批量加入的 joinm 函数
    cfg.joinm([]int{gi})
}

// joinm 是真正的执行函数，用于向 ShardMaster 报告【多个】新副本组的批量加入。
// gis: 一个包含副本组内部索引（gi）的切片
func (cfg *config) joinm(gis []int) {
    // 1. 构造 Join RPC 所需的参数结构
    // ShardMaster 的 Join 接口期望接收一个 map：
    // Key 是新加入的 GID，Value 是这个 GID 组内所有服务器的名称（网络地址）列表
    m := make(map[int][]string, len(gis))
    
    // 2. 遍历每一个需要加入的副本组
    for _, g := range gis {
        // 从框架预先初始化的配置中，提取该组真实的全局唯一 GID (例如 100, 101)
        gid := cfg.groups[g].gid 
        
        // 准备一个切片，用于存放组内所有 n 台服务器的网络名称
        servernames := make([]string, cfg.n)
        
        // 遍历组内的所有服务器节点
        for i := 0; i < cfg.n; i++ {
            // cfg.servername(gid, i) 会生成一个类似于 "ShardKV-100-0" 的唯一字符串。
            // 在我们的模拟网络环境 (labrpc) 中，这个字符串就相当于真实世界里的 IP:Port 地址。
            servernames[i] = cfg.servername(gid, i)
        }
        
        // 将该组的 GID 和对应的服务器名称列表放入 map 中
        m[gid] = servernames
    }
    
    // 3. 发送 RPC 请求给 ShardMaster
    // cfg.mck 是一个特殊的、专属于管理员的 Clerk（客户端代理）。
    // 调用它的 Join 方法，实际上就是通过网络向 ShardMaster 集群发起了 Join RPC。
    // ShardMaster 收到请求后，会触发底层的 Raft 达成共识，生成新的 Config，
    // 并触发你之前在 Lab 4A 写的 rebalance 算法，把分片匀给这些新加入的组。
    cfg.mck.Join(m)
}

// tell the shardmaster that a group is leaving.
func (cfg *config) leave(gi int) {
	cfg.leavem([]int{gi})
}

func (cfg *config) leavem(gis []int) {
	gids := make([]int, 0, len(gis))
	for _, g := range gis {
		gids = append(gids, cfg.groups[g].gid)
	}
	cfg.mck.Leave(gids)
}

var ncpu_once sync.Once

// make_config 初始化一个 ShardKV 系统的测试环境（Config 对象）
// t: Go 的测试上下文对象
// n: 每个 ShardKV 副本组（Replica Group）中包含的服务器节点数量（通常是 3，构成 Raft 集群）
// unreliable: 是否开启不可靠网络模拟（开启后底层网络会随机丢包、延迟、乱序）
// maxraftstate: Raft 日志大小的软限制，用于测试 Snapshot（快照）功能，-1 表示不限制
func make_config(t *testing.T, n int, unreliable bool, maxraftstate int) *config {
    // 1. 并发环境检查与初始化
    // ncpu_once 保证这段闭包代码在整个测试运行期间只执行一次
    ncpu_once.Do(func() {
        if runtime.NumCPU() < 2 {
            // 警告：分布式系统的死锁和竞态条件（Race Condition）通常需要真实的物理并行才能暴露。
            // 如果只有 1 个 CPU，多协程只是分时复用，很多并发 Bug 可能会被隐藏。
            fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
        }
        rand.Seed(makeSeed()) // 初始化全局随机数种子
    })
    
    // 强行要求 Go 运行时至少分配 4 个系统线程来调度协程。
    // 这是为了最大限度地榨取多核性能，创造恶劣的并发环境，逼出你的死锁和并发 Bug。
    runtime.GOMAXPROCS(4)
    
    cfg := &config{}
    cfg.t = t
    cfg.maxraftstate = maxraftstate
    
    // 2. 创建模拟网络 (极其核心)
    // labrpc 是 6.824 官方提供的一个模拟网络层。
    // 测试框架通过控制这个 network，可以随时给某台机器“拔网线”，或者让 RPC 延迟到达。
    cfg.net = labrpc.MakeNetwork()
    cfg.start = time.Now()

    // ==========================================
    // 3. 搭建控制面：ShardMaster 集群
    // ==========================================
    cfg.nmasters = 3 // 规定系统中有 3 台 ShardMaster 服务器组成一个 Raft 组
    cfg.masterservers = make([]*shardmaster.ShardMaster, cfg.nmasters)
    for i := 0; i < cfg.nmasters; i++ {
        cfg.StartMasterServer(i) // 启动 ShardMaster 节点
    }
    // 创建一个特殊的 Clerk（客户端），专门供测试框架用来向 ShardMaster 发送 Join/Leave 等管理员命令
    cfg.mck = cfg.shardclerk()

    // ==========================================
    // 4. 搭建数据面：ShardKV 副本组 (Replica Groups)
    // ==========================================
    cfg.ngroups = 3 // 预设环境里总共会创建 3 个独立的 ShardKV 副本组
    cfg.groups = make([]*group, cfg.ngroups)
    cfg.n = n // 每个组内有 n 台机器（传参进来的，通常也是 3）
    
    for gi := 0; gi < cfg.ngroups; gi++ {
        gg := &group{}
        cfg.groups[gi] = gg
        // 分配全局唯一的 GID。这里硬编码为 100, 101, 102
        gg.gid = 100 + gi 
        
        // 为组内的 n 台服务器预先分配空间
        gg.servers = make([]*ShardKV, cfg.n)
        gg.saved = make([]*raft.Persister, cfg.n) // 模拟持久化存储（存 Raft 状态和快照）
        gg.endnames = make([][]string, cfg.n)     // 记录节点之间通信的 RPC 端点名称
        gg.mendnames = make([][]string, cfg.nmasters) // 记录与 ShardMaster 通信的端点名称
        
        // 依次启动这个组内的 n 台 ShardKV 服务器，底层它们会自动组成一个 Raft 集群
        for i := 0; i < cfg.n; i++ {
            cfg.StartServer(gi, i)
        }
    }

    // 5. 客户端跟踪器初始化
    cfg.clerks = make(map[*Clerk][]string)
    
    // 为客户端生成防重放的唯一 ClientId 设定初始基数。
    // 加 1000 是为了防止客户端的 ID 和服务器的 ID（0, 1, 2...）在日志里混淆，方便 debug。
    cfg.nextClientId = cfg.n + 1000 

    // 6. 设置网络可靠性
    // 根据传入的 unreliable 参数，告诉 labrpc 模拟网络是否需要随机丢包/延迟 RPC。
    cfg.net.Reliable(!unreliable)

    return cfg
}
