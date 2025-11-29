package kvraft

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
import "fmt"
import "time"
import "sync/atomic"

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

// 随机化服务器句柄顺序
// 这个函数将服务器端点数组进行随机洗牌，以避免客户端总是以相同的顺序尝试连接服务器
func random_handles(kvh []*labrpc.ClientEnd) []*labrpc.ClientEnd {
	// 创建一个新的数组，复制原始的服务器端点
	sa := make([]*labrpc.ClientEnd, len(kvh))
	copy(sa, kvh) // 复制原始数组，避免修改原数组
	
	// 使用Fisher-Yates洗牌算法随机打乱数组顺序
	for i := range sa {
		// 生成一个0到i之间的随机索引（包括0和i）
		j := rand.Intn(i + 1)
		// 交换位置i和位置j的元素
		sa[i], sa[j] = sa[j], sa[i]
	}
	
	// 返回随机化后的服务器端点数组
	return sa
}

type config struct {
	mu           sync.Mutex
	t            *testing.T
	net          *labrpc.Network
	n            int
	kvservers    []*KVServer
	saved        []*raft.Persister
	endnames     [][]string // names of each server's sending ClientEnds
	clerks       map[*Clerk][]string
	nextClientId int
	maxraftstate int
	start        time.Time // time at which make_config() was called
	// begin()/end() statistics
	t0    time.Time // time at which test_test.go called cfg.begin()
	rpcs0 int       // rpcTotal() at start of test
	ops   int32     // number of clerk get/put/append method calls
}

// func (cfg *config) one(cmd interface{}, expectedServers int, retry bool) int {
// 	// 记录开始时间，设置 10 秒超时
// 	t0 := time.Now()
// 	starts := 0

// 	// 主循环：在 10 秒内持续尝试提交命令
// 	for time.Since(t0).Seconds() < 10 {
// 		// 尝试向所有服务器提交命令，寻找当前的领导者
// 		index := -1

// 		// 遍历所有服务器节点
// 		for si := 0; si < cfg.n; si++ {
// 			// 轮询方式选择起始服务器，避免总是从同一台开始
// 			starts = (starts + 1) % cfg.n

// 			var rf *Raft
// 			// 获取服务器实例，只操作已连接的服务器
// 			cfg.mu.Lock()
// 			if cfg.connected[starts] {
// 				rf = cfg.rafts[starts]
// 			}
// 			cfg.mu.Unlock()

// 			// 如果服务器可用，尝试提交命令
// 			if rf != nil {
// 				// 调用 Start 方法提交命令
// 				index1, _, ok := rf.Start(cmd)
// 				if ok {
// 					// 提交成功，记录日志索引并跳出循环
// 					index = index1
// 					DPrintf("start日志添加成功，index为%d\n",index)
// 					break
// 				}
// 			}
// 		}

// 		// 如果成功提交了命令（找到了领导者）
// 		if index != -1 {
// 			// 等待命令在集群中达成共识，设置 2 秒超时
// 			t1 := time.Now()
// 			DPrintf("开始检查是否达到共识，index为%d\n",index)
// 			for time.Since(t1).Seconds() < 2 {
// 				// 检查指定索引位置的命令提交情况
// 				DPrintf("cfg.nCommitted(%d)开始执行\n",index)
// 				nd, cmd1 := cfg.nCommitted(index)
// 				DPrintf("cfg.nCommitted(%d)执行完成,nd: %d,cmd1: %d\n",index,nd, cmd1)

// 				// 如果有足够的服务器提交了该命令
// 				if nd > 0 && nd >= expectedServers {
// 					DPrintf("已经确定nd >= expectedServers\n")
// 					// 并且提交的命令内容正确
// 					if cmd1 == cmd {
// 						// 返回命令在日志中的索引
// 						return index
// 					}
// 				}
// 				// 短暂休眠后继续检查
// 				time.Sleep(20 * time.Millisecond)
// 			}

// 			// 如果不允许重试，直接失败
// 			if retry == false {
// 				cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
// 			}
// 		} else {
// 			// 如果没有找到领导者，短暂休眠后重试
// 			time.Sleep(50 * time.Millisecond)
// 		}
// 	}

// 	// 超时仍未达成共识，测试失败
// 	cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
// 	return -1
// }

func (cfg *config) checkTimeout() {
	// enforce a two minute real-time limit on each test
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}

func (cfg *config) cleanup() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for i := 0; i < len(cfg.kvservers); i++ {
		if cfg.kvservers[i] != nil {
			cfg.kvservers[i].Kill()
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

// Maximum snapshot size across all servers
func (cfg *config) SnapshotSize() int {
	snapshotsize := 0
	for i := 0; i < cfg.n; i++ {
		n := cfg.saved[i].SnapshotSize()
		if n > snapshotsize {
			snapshotsize = n
		}
	}
	return snapshotsize
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
	// 创建一个长度为服务器数量的整数切片
	all := make([]int, cfg.n)
	
	// 填充切片，使其包含从0到cfg.n-1的所有服务器ID
	for i := 0; i < cfg.n; i++ {
		all[i] = i // 服务器ID从0开始递增
	}
	
	// 返回包含所有服务器ID的切片
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

// 为特定客户端创建具有特定服务器名称的Clerk。
// 给它连接到所有服务器，但目前只启用到to[]中服务器的连接。
func (cfg *config) makeClient(to []int) *Clerk {
	cfg.mu.Lock()      // 加锁保护共享资源
	defer cfg.mu.Unlock() // 函数结束时解锁

	// 创建一组新的ClientEnds（客户端端点）
	ends := make([]*labrpc.ClientEnd, cfg.n) // 创建客户端端点数组，大小为服务器数量
	endnames := make([]string, cfg.n)        // 创建端点名称数组，大小为服务器数量
	
	// 为每个服务器创建客户端端点连接
	for j := 0; j < cfg.n; j++ {
		endnames[j] = randstring(20)          // 生成20位随机字符串作为端点名称
		ends[j] = cfg.net.MakeEnd(endnames[j]) // 在网络中创建客户端端点
		cfg.net.Connect(endnames[j], j)       // 将客户端端点连接到对应的服务器ID
	}

	// 创建新的客户端Clerk对象
	ck := MakeClerk(random_handles(ends)) // 使用随机处理的端点数组创建Clerk
	
	
	// 将新创建的客户端添加到配置的客户端映射中
	cfg.clerks[ck] = endnames             // 将客户端对象映射到其端点名称数组
	
	cfg.nextClientId++                    // 递增下一个客户端ID
	
	// 连接客户端到指定的服务器（在to数组中的服务器）
	cfg.ConnectClientUnlocked(ck, to)     // 连接客户端到指定的服务器列表
	
	return ck                            // 返回新创建的客户端对象
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
// 连接客户端到指定的服务器列表
// 该函数启用客户端到指定服务器的网络连接
func (cfg *config) ConnectClientUnlocked(ck *Clerk, to []int) {
	//DPrintf("ConnectClient %v to %v\n", ck, to) // 打印调试信息
	
	endnames := cfg.clerks[ck] // 获取客户端的端点名称数组（之前在makeClient中保存）
	
	// 遍历要连接的服务器列表
	for j := 0; j < len(to); j++ {
		s := endnames[to[j]]      // 获取到服务器to[j]的端点名称
		cfg.net.Enable(s, true)   // 启用该端点的网络连接（允许RPC通信）
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

	kv := cfg.kvservers[i]
	if kv != nil {
		cfg.mu.Unlock()
		kv.Kill()
		cfg.mu.Lock()
		cfg.kvservers[i] = nil
	}
}

// 如果重启服务器，首先调用ShutdownServer
// 原注释：如果要重启服务器，必须先调用ShutdownServer函数来正确关闭服务器
func (cfg *config) StartServer(i int) {
	cfg.mu.Lock() // 加锁保护共享资源

	//为多个服务器创建连接，这些是网络基础设施，提供服务器间的通信能力，是raft和kv数据库共同的底层服务器
	// 创建一组新的传出ClientEnd名称（用于与其他服务器通信）
	cfg.endnames[i] = make([]string, cfg.n) // 为服务器i创建与其他n个服务器通信的端点名称数组
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20) // 为每个连接生成20位随机字符串作为端点名称
	}

	// 创建一组新的ClientEnds（客户端端点）
	ends := make([]*labrpc.ClientEnd, cfg.n) // 创建客户端端点数组
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j]) // 在网络中创建客户端端点
		cfg.net.Connect(cfg.endnames[i][j], j)        // 将端点连接到服务器j
	}

	// 创建新的持久化器，防止旧实例覆盖新实例的持久化状态
	// 将旧持久化器的状态复制给新持久化器
	// 这样规范就是将最后持久化的状态传递给StartKVServer()
	if cfg.saved[i] != nil { // 如果之前存在持久化器
		cfg.saved[i] = cfg.saved[i].Copy() // 复制旧持久化器的状态到新持久化器
	} else {
		cfg.saved[i] = raft.MakePersister() // 创建新的持久化器
	}
	cfg.mu.Unlock() // 解锁

	// 启动KV服务器
	cfg.kvservers[i] = StartKVServer(ends, i, cfg.saved[i], cfg.maxraftstate)

	// 创建RPC服务
	kvsvc := labrpc.MakeService(cfg.kvservers[i])     // 为KV服务器创建服务
	rfsvc := labrpc.MakeService(cfg.kvservers[i].rf)  // 为底层Raft创建服务
	srv := labrpc.MakeServer()                        // 创建服务器实例
	srv.AddService(kvsvc)                             // 添加KV服务
	srv.AddService(rfsvc)                             // 添加Raft服务
	cfg.net.AddServer(i, srv)                         // 将服务器添加到网络中
}

func (cfg *config) Leader() (bool, int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	for i := 0; i < cfg.n; i++ {
		_, is_leader := cfg.kvservers[i].rf.GetState()
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

var ncpu_once sync.Once

func make_config(t *testing.T, n int, unreliable bool, maxraftstate int) *config {
	// 确保只执行一次CPU检查和随机种子初始化
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed()) // 设置随机种子
	})
	
	runtime.GOMAXPROCS(6) // 设置最大Goroutine执行核心数为4

	// 创建配置结构体
	cfg := &config{}
	cfg.t = t // 保存测试对象
	cfg.net = labrpc.MakeNetwork() // 创建RPC网络
	cfg.n = n // 服务器数量
	cfg.kvservers = make([]*KVServer, cfg.n) // 创建KV服务器数组
	cfg.saved = make([]*raft.Persister, cfg.n) // 创建持久化器数组
	cfg.endnames = make([][]string, cfg.n) // 创建服务器端点名称数组
	cfg.clerks = make(map[*Clerk][]string) // 创建客户端映射
	cfg.nextClientId = cfg.n + 1000 // 客户端ID从服务器ID+1000开始（避免冲突）
	cfg.maxraftstate = maxraftstate // 设置最大Raft状态大小限制
	cfg.start = time.Now() // 记录开始时间

	// 创建完整的KV服务器集合
	for i := 0; i < cfg.n; i++ {
		cfg.StartServer(i) // 启动第i个服务器
	}

	cfg.ConnectAll() // 连接所有服务器到网络

	cfg.net.Reliable(!unreliable) // 根据unreliable参数设置网络可靠性
	// 如果unreliable为true，网络不可靠（会丢包）
	// 如果unreliable为false，网络可靠（不会丢包）

	return cfg // 返回配置对象
}

func (cfg *config) rpcTotal() int {
	return cfg.net.GetTotalCount()
}

// start a Test.
// print the Test message.
// e.g. cfg.begin("Test (2B): RPC counts aren't too high")
func (cfg *config) begin(description string) {
	// 打印测试描述信息，显示测试开始
	fmt.Printf("%s ...\n", description)
	
	cfg.t0 = time.Now()       // 记录测试开始时间，用于后续计算测试持续时间
	
	cfg.rpcs0 = cfg.rpcTotal() // 记录测试开始时的RPC总数量，用于后续计算测试期间的RPC数量
	
	atomic.StoreInt32(&cfg.ops, 0) // 将操作计数器原子性地重置为0，用于统计测试期间的操作次数
}

func (cfg *config) op() {
	atomic.AddInt32(&cfg.ops, 1)
}

// end a Test -- the fact that we got here means there
// was no failure.
// print the Passed message,
// and some performance numbers.
func (cfg *config) end() {
	cfg.checkTimeout()
	if cfg.t.Failed() == false {
		t := time.Since(cfg.t0).Seconds()  // real time
		npeers := cfg.n                    // number of Raft peers
		nrpc := cfg.rpcTotal() - cfg.rpcs0 // number of RPC sends
		ops := atomic.LoadInt32(&cfg.ops)  //  number of clerk get/put/append calls

		fmt.Printf("  ... Passed --")
		fmt.Printf("  %4.1f  %d %5d %4d\n", t, npeers, nrpc, ops)
	}
}
