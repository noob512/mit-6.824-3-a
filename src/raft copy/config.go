package raft

//
// support for Raft tester.
//
// we will use the original config.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "../labrpc"
import "log"
import "sync"
import "testing"
import "runtime"
import "math/rand"
import crand "crypto/rand"
import "math/big"
import "encoding/base64"
import "time"
import "fmt"

// // 生成随机字符串（用于生成唯一端点名称）
func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

// // 生成随机种子（避免测试结果受固定随机影响）
func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

type config struct {
	mu        sync.Mutex            // 保护共享状态的互斥锁（多 goroutine 安全）
	t         *testing.T            // 测试框架的测试对象（用于报告错误）
	net       *labrpc.Network       // labrpc 网络实例（模拟节点间的网络通信）
	n         int                   // 集群节点总数
	rafts     []*Raft               // Raft 节点数组（存储集群中所有 Raft 实例）
	applyErr  []string              // 记录每个节点的 applyCh 错误（如日志不一致）
	connected []bool                // 标记每个节点是否连接到网络（模拟网络分区）
	saved     []*Persister          // 每个节点的持久化存储备份（模拟崩溃后恢复）
	endnames  [][]string            // 每个节点的 RPC 端点名称（用于网络连接管理）
	logs      []map[int]interface{} // 每个节点已提交日志的副本（用于验证一致性）
	start     time.Time             // 测试启动时间（用于超时控制）
	// 以下为测试统计信息
	t0        time.Time // 测试开始时间
	rpcs0     int       // 测试开始时的 RPC 总数
	cmds0     int       // 测试开始时的已提交命令数
	bytes0    int64     // 测试开始时的总字节数
	maxIndex  int       // 当+前已提交的最大日志索引
	maxIndex0 int       // 测试开始时的最大日志索引
}

var ncpu_once sync.Once

func make_config(t *testing.T, n int, unreliable bool) *config {
	//使用 sync.Once 确保匿名函数内的逻辑仅执行一次
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork() //创建 labrpc 网络实例（模拟节点间的网络通信，支持丢包、延迟等）。
	cfg.n = n                      //集群节点总数为 n
	cfg.applyErr = make([]string, cfg.n)
	cfg.rafts = make([]*Raft, cfg.n)
	cfg.connected = make([]bool, cfg.n)
	cfg.saved = make([]*Persister, cfg.n)
	cfg.endnames = make([][]string, cfg.n) //二维数组，存储每个节点的 RPC 端点名称（用于网络连接管理，确保节点间通信的唯一性）。
	cfg.logs = make([]map[int]interface{}, cfg.n)
	//数组，每个元素是一个 map（键为日志索引，值为命令），记录每个节点已提交的日志（用于验证一致性）。
	cfg.start = time.Now()

	cfg.setunreliable(unreliable) //调用 setunreliable 方法设置网络是否可靠

	cfg.net.LongDelays(true) //配置 labrpc 网络启用 “长延迟” 模式，模拟 RPC 传输中的随机延迟

	for i := 0; i < cfg.n; i++ {
		cfg.logs[i] = map[int]interface{}{} // 初始化节点 i 的提交日志 map
		cfg.start1(i)                       // 启动节点 i（创建 Raft 实例）
	}

	// connect everyone
	for i := 0; i < cfg.n; i++ {
		cfg.connect(i)
	}

	return cfg
}

// 关闭一个 Raft 服务器，但保存其持久化状态
func (cfg *config) crash1(i int) {
	// 1. 断开节点 i 与网络的连接（禁用所有进出 RPC）
	cfg.disconnect(i)

	// 2. 从网络中删除节点 i 的服务端注册，禁止新连接建立
	cfg.net.DeleteServer(i) // 禁用客户端与该服务器的连接

	// 3. 加锁保护共享状态操作（线程安全）
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// 4. 备份持久化状态，防止旧实例继续修改
	// （创建副本以隔离旧实例的后续操作，但保留最新状态）
	// 新创建一个持久化存储实例，以防旧的 Raft 实例
	// 继续更新原有的持久化存储。
	// 但要复制旧持久化存储中的内容，这样我们就能始终
	// 向 Make () 函数传递最新的持久化状态。
	if cfg.saved[i] != nil {
		// 复制当前持久化存储的内容到新实例
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	// 5. 终止 Raft 实例
	// 获取节点 i 的 Raft 实例
	rf := cfg.rafts[i]
	if rf != nil {
		// 临时解锁（避免 Kill() 内部操作导致死锁）
		cfg.mu.Unlock()
		// 调用 Raft 的 Kill() 方法终止其内部逻辑（如 goroutine）
		rf.Kill()
		// 重新加锁继续操作
		cfg.mu.Lock()
		// 标记该节点的 Raft 实例已销毁
		cfg.rafts[i] = nil
	}

	// 6. 刷新持久化存储，确保状态是崩溃瞬间的快照
	if cfg.saved[i] != nil {
		// 读取当前持久化的状态数据（任期、日志等）
		raftlog := cfg.saved[i].ReadRaftState()
		// 创建全新的 Persister 实例
		cfg.saved[i] = &Persister{}
		// 将读取到的状态重新存入新的 Persister
		cfg.saved[i].SaveRaftState(raftlog)
	}
}

// start or re-start a Raft.
// if one already exists, "kill" it first.
// allocate new outgoing port file names, and a new
// state persister, to isolate previous instance of
// this server. since we cannot really kill it.
//
// 启动或重启一个 Raft 节点。
// 若该节点已存在 Raft 实例，先“终止”旧实例。
// 分配新的输出端口文件名和新的状态持久化存储，
// 以隔离该节点的先前实例（因为无法真正彻底“杀死”旧实例的所有残留进程）。
func (cfg *config) start1(i int) {
	cfg.crash1(i) //先终止旧实例（确保环境干净）

	// a fresh set of outgoing ClientEnd names.
	// so that old crashed instance's ClientEnds can't send.
	//// 生成新的输出 ClientEnd 名称，避免旧实例的 ClientEnd 继续发送请求
	cfg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	// 创建新的 ClientEnd 数组（节点 i 对外通信的客户端）
	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j]) // 基于新端点名创建 ClientEnd
		cfg.net.Connect(cfg.endnames[i][j], j)        // 连接到目标节点 j
	}

	cfg.mu.Lock()

	// 新创建持久化存储实例，避免旧实例覆盖新实例的状态
	// 但复制旧存储的内容，确保新实例启动时能加载最新持久化状态
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy() // 复制旧存储内容到新实例
	} else {
		cfg.saved[i] = MakePersister() // 若旧存储不存在，创建新的
	}

	cfg.mu.Unlock()

	// 创建用于接收 Raft 提交日志的通道
	applyCh := make(chan ApplyMsg)
	// 启动 goroutine 监听 applyCh，验证提交日志的一致性
	go func() {
		for m := range applyCh { // 循环读取 Raft 提交的日志消息
			err_msg := ""
			if m.CommandValid == false {
				// 忽略非日志提交的消息（如快照）
			} else {
				v := m.Command // 当前节点提交的命令
				cfg.mu.Lock()
				// 检查所有节点的日志：同一索引是否提交了不同命令
				for j := 0; j < len(cfg.logs); j++ {
					if old, oldok := cfg.logs[j][m.CommandIndex]; oldok && old != v {
						// 发现不一致：不同节点在同一索引提交了不同命令
						err_msg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v",
							m.CommandIndex, i, m.Command, j, old)
					}
				}
				// 检查提交顺序：当前索引是否比前一个索引大（避免乱序）
				_, prevok := cfg.logs[i][m.CommandIndex-1]
				cfg.logs[i][m.CommandIndex] = v // 记录当前节点提交的日志
				if m.CommandIndex > cfg.maxIndex {
					cfg.maxIndex = m.CommandIndex // 更新全局最大提交索引
				}
				cfg.mu.Unlock()

				// 若当前索引>1但前一个索引未提交，说明提交顺序错乱
				if m.CommandIndex > 1 && prevok == false {
					err_msg = fmt.Sprintf("server %v apply out of order %v", i, m.CommandIndex)
				}
			}

			// 若有错误，记录并报错（但继续读取以避免 Raft 阻塞）
			if err_msg != "" {
				log.Fatalf("apply error: %v\n", err_msg)
				cfg.applyErr[i] = err_msg
				// 即使出错也继续读取，防止 Raft 因通道阻塞而死锁
			}
		}
	}()

	// 创建 Raft 实例（调用用户实现的 Make 函数）
	rf := Make(ends, i, cfg.saved[i], applyCh)

	// 将新实例保存到 config 中
	cfg.mu.Lock()
	cfg.rafts[i] = rf
	cfg.mu.Unlock()

	// 注册 Raft 实例为 RPC 服务，使其能接收其他节点的 RPC 请求
	svc := labrpc.MakeService(rf) // 将 Raft 实例包装为 RPC 服务
	srv := labrpc.MakeServer()    // 创建 RPC 服务器
	srv.AddService(svc)           // 向服务器注册服务
	cfg.net.AddServer(i, srv)     // 将服务器添加到网络，节点 i 可接收 RPC

}

func (cfg *config) checkTimeout() {
	// enforce a two minute real-time limit on each test
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}

func (cfg *config) cleanup() {
	for i := 0; i < len(cfg.rafts); i++ {
		if cfg.rafts[i] != nil {
			cfg.rafts[i].Kill()
		}
	}
	cfg.net.Cleanup()
	cfg.checkTimeout()
}

// attach server i to the net.
func (cfg *config) connect(i int) {
	fmt.Printf("connect(%d)\n", i)  // 调试用：打印连接信息

	// 标记节点 i 为"已连接"状态
	cfg.connected[i] = true

	// 启用节点 i 的" outgoing ClientEnds "（节点 i 向其他节点发送 RPC 的通道）
	for j := 0; j < cfg.n; j++ {
		// 仅当目标节点 j 也处于连接状态时，才启用通道
		if cfg.connected[j] {
			endname := cfg.endnames[i][j] // 获取节点 i 到 j 的 RPC 端点名称
			cfg.net.Enable(endname, true) // 启用该端点的通信（允许发送 RPC）
		}
	}

	// 启用节点 i 的" incoming ClientEnds "（其他节点向 i 发送 RPC 的通道）
	for j := 0; j < cfg.n; j++ {
		// 仅当源节点 j 处于连接状态时，才启用通道
		if cfg.connected[j] {
			endname := cfg.endnames[j][i] // 获取节点 j 到 i 的 RPC 端点名称
			cfg.net.Enable(endname, true) // 启用该端点的通信（允许接收 RPC）
		}
	}
}

// detach server i from the net.
func (cfg *config) disconnect(i int) {
	DPrintf("disconnect(%d)\n", i) // 调试用：打印断开信息

	// 标记节点 i 为"已断开"状态
	cfg.connected[i] = false

	// 禁用节点 i 的" outgoing ClientEnds "（停止向其他节点发送 RPC）
	for j := 0; j < cfg.n; j++ {
		// 若节点 i 有有效的端点配置
		if cfg.endnames[i] != nil {
			endname := cfg.endnames[i][j]  // 获取节点 i 到 j 的 RPC 端点名称
			cfg.net.Enable(endname, false) // 禁用该端点的通信（禁止发送 RPC）
		}
	}

	// 禁用节点 i 的" incoming ClientEnds "（停止接收其他节点的 RPC）
	for j := 0; j < cfg.n; j++ {
		// 若节点 j 有有效的端点配置
		if cfg.endnames[j] != nil {
			endname := cfg.endnames[j][i]  // 获取节点 j 到 i 的 RPC 端点名称
			cfg.net.Enable(endname, false) // 禁用该端点的通信（禁止接收 RPC）
		}
	}
}

func (cfg *config) rpcCount(server int) int {
	return cfg.net.GetCount(server)
} //作用：统计指定节点（server）接收到的 RPC 请求总数。

func (cfg *config) rpcTotal() int {
	return cfg.net.GetTotalCount()
} //作用：统计整个集群中所有节点接收到的 RPC 总数量。

func (cfg *config) setunreliable(unrel bool) {
	cfg.net.Reliable(!unrel)
} //作用：设置网络是否为 “不可靠模式”（模拟丢包、延迟等真实网络特性）。

func (cfg *config) bytesTotal() int64 {
	return cfg.net.GetTotalBytes()
} //作用：统计整个集群中所有 RPC 通信的总字节数（包括请求和响应）。

func (cfg *config) setlongreordering(longrel bool) {
	cfg.net.LongReordering(longrel)
} //作用：设置是否启用 “长延迟重排序” 模式（模拟 RPC 乱序到达）。

// check that there's exactly one leader.
// try a few times in case re-elections are needed.
func (cfg *config) checkOneLeader() int {
	for iters := 0; iters < 10; iters++ {
		DPrintf("now is iter:%d\n", iters)
		ms := 450 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		leaders := make(map[int][]int)
		//创建 leaders 映射：key 为 “term 编号”，value 为 “该 term 下声称是 leader 的节点索引列表”。
		for i := 0; i < cfg.n; i++ {
			if cfg.connected[i] {
				if term, leader := cfg.rafts[i].GetState(); leader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}
		//遍历所有节点，收集 leader 信息：
		//if cfg.connected[i]：仅考虑已连接到网络的节点（离线节点无需参与检查）。
		//cfg.rafts[i].GetState()：调用 Raft 实例的 GetState 方法，获取节点的当前 term 和 “是否认为自己是 leader”。
		//若节点 i 认为自己是 leader，则将其索引添加到 leaders[term] 列表中（按 term 分组）

		lastTermWithLeader := -1
		for term, leaders := range leaders {
			if len(leaders) > 1 {
				cfg.t.Fatalf("term %d has %d (>1) leaders", term, len(leaders))
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}
		//验证每个 term 的 leader 唯一性，并找到最新 term：
		//遍历 leaders 映射，若某 term 对应的 leaders 列表长度 >1（同一 term 有多个节点声称是 leader），则调用 cfg.t.Fatalf 报错（违反 Raft 协议：每个 term 最多一个 leader）。
		//记录 lastTermWithLeader 为最大的 term 编号（最新 term 的 leader 才是当前有效的 leader，旧 term 的 leader 已过期）。

		if len(leaders) != 0 {
			return leaders[lastTermWithLeader][0]
		}
	}
	cfg.t.Fatalf("expected one leader, got none")
	return -1
}

// 检查所有节点是否对当前任期（term）达成一致。
func (cfg *config) checkTerms() int {
	term := -1 // 用于存储首个节点的任期，初始化为-1（无效值）
	// 遍历集群中所有节点
	for i := 0; i < cfg.n; i++ {
		// 只检查处于连接状态的节点（排除已崩溃或网络隔离的节点）
		if cfg.connected[i] {
			// 调用节点i的GetState()方法，获取其当前任期（忽略是否为领导者的返回值）
			xterm, _ := cfg.rafts[i].GetState()
			// 若尚未记录任期（首次检查），将当前节点的任期设为基准
			if term == -1 {
				term = xterm
			} else if term != xterm { // 若后续节点的任期与基准不一致
				// 终止测试并报错：节点间任期不统一
				cfg.t.Fatalf("servers disagree on term")
			}
		}
	}
	// 返回所有节点一致的任期（若有节点连接，则为有效的term；否则为初始值-1）
	return term
}

// check that there's no leader
// 遍历所有节点后，若未发现任何节点声称是 leader，则函数正常结束（测试通过）
func (cfg *config) checkNoLeader() {
	for i := 0; i < cfg.n; i++ {
		if cfg.connected[i] {
			_, is_leader := cfg.rafts[i].GetState()
			if is_leader {
				cfg.t.Fatalf("expected no leader, but %v claims to be leader", i)
			}
		}
	}
}

// 统计有多少个服务器认为某条日志条目已被提交
func (cfg *config) nCommitted(index int) (int, interface{}) {
	// 初始化计数器，记录已提交该日志的节点数量
	count := 0
	// 用于存储日志索引对应的命令（确保所有节点提交的命令一致）
	var cmd interface{} = nil

	// 遍历集群中所有节点
	for i := 0; i < len(cfg.rafts); i++ {
		// 检查当前节点是否存在日志提交错误（如乱序提交）
		if cfg.applyErr[i] != "" {
			DPrintf("主机：%d出现了问题", i)
			// 若有错误，终止测试并输出错误信息
			cfg.t.Fatal(cfg.applyErr[i])
		}

		// 加锁保护共享资源 cfg.logs[i]（避免并发读写冲突）
		cfg.mu.Lock()
		// 从节点 i 的日志中获取索引为 index 的命令
		// cmd1: 该索引对应的命令；ok: 标记该节点是否已提交该索引的日志
		cmd1, ok := cfg.logs[i][index]
		// 解锁
		cfg.mu.Unlock()

		// 若节点 i 已提交该索引的日志（ok 为 true）
		if ok {
			// 检查当前节点的命令与已记录的命令是否一致
			// 仅在已有其他节点提交过日志（count > 0）时需要检查
			if count > 0 && cmd != cmd1 {
				// 若命令不一致，终止测试并报告错误
				cfg.t.Fatalf("committed values do not match: index %v, %v, %v\n",
					index, cmd, cmd1)
			}
			// 增加已提交节点的计数
			count += 1
			// 记录当前命令（用于后续节点的一致性检查）
			cmd = cmd1
		}
	}

	// 返回统计结果：已提交该日志的节点数量 + 对应的命令（若存在）
	return count, cmd
}

// 等待至少 n 个服务器提交提交指定索引的日志条目。
// 但不会无限期等待。
func (cfg *config) wait(index int, n int, startTerm int) interface{} {
	// 初始等待时间为 10 毫秒
	to := 10 * time.Millisecond
	// 最多尝试 30 次（总等待时间约 10ms + 20ms + ... + 1s，共约 2 秒）
	for iters := 0; iters < 30; iters++ {
		// 检查当前已提交该索引日志的节点数量（忽略命令值）
		nd, _ := cfg.nCommitted(index)
		// 若已满足至少 n 个节点提交，退出循环
		if nd >= n {
			break
		}
		// 等待一段时间后重试
		time.Sleep(to)
		// 指数退避：等待时间翻倍（最大不超过 1 秒）
		if to < time.Second {
			to *= 2
		}
		// 若指定了起始任期（startTerm > -1），检查是否有节点任期已推进
		if startTerm > -1 {
			for _, r := range cfg.rafts {
				// 获取节点当前任期
				t, _ := r.GetState()
				// 若有节点任期超过 startTerm，说明集群已进入新任期
				if t > startTerm {
					// 无法再保证当前操作能成功（可能被新 leader 覆盖）
					return -1
				}
			}
		}
	}
	// 循环结束后，最终检查已提交的节点数量和对应命令
	nd, cmd := cfg.nCommitted(index)
	// 若仍未达到 n 个节点提交，终止测试并报错
	if nd < n {
		cfg.t.Fatalf("only %d decided for index %d; wanted %d\n",
			nd, index, n)
	}
	// 返回该索引对应的日志命令（所有已提交节点的命令已通过 nCommitted 验证一致）
	return cmd
}

// 完成一次完整的日志共识过程。
// 初始可能会选错 leader，
// 若失败则需放弃后重新提交。
// 约 10 秒后完全放弃。
// 间接验证所有服务器对同一值达成共识 ——
// 因为 nCommitted () 会检查这一点，
// 从 applyCh 读取日志的线程也会做同样的检查。
// 返回日志索引。
// 若 retry==true，可能会多次提交命令 ——
// 以防 leader 在调用 Start () 后立即崩溃。
// 若 retry==false，仅调用 Start () 一次 ——
// 目的是简化 Lab 2B 前期的测试。
// one 向 Raft 集群提交一个命令并等待达成共识
// 该函数用于测试 Raft 集群的一致性，确保命令被正确复制到足够多的服务器上
func (cfg *config) one(cmd interface{}, expectedServers int, retry bool) int {
	// 记录开始时间，设置 10 秒超时
	t0 := time.Now()
	starts := 0

	// 主循环：在 10 秒内持续尝试提交命令
	for time.Since(t0).Seconds() < 10 {
		// 尝试向所有服务器提交命令，寻找当前的领导者
		index := -1

		// 遍历所有服务器节点
		for si := 0; si < cfg.n; si++ {
			// 轮询方式选择起始服务器，避免总是从同一台开始
			starts = (starts + 1) % cfg.n

			var rf *Raft
			// 获取服务器实例，只操作已连接的服务器
			cfg.mu.Lock()
			if cfg.connected[starts] {
				rf = cfg.rafts[starts]
			}
			cfg.mu.Unlock()

			// 如果服务器可用，尝试提交命令
			if rf != nil {
				// 调用 Start 方法提交命令
				index1, _, ok := rf.Start(cmd)
				if ok {
					// 提交成功，记录日志索引并跳出循环
					index = index1
					DPrintf("start日志添加成功，index为%d\n",index)
					break
				}
			}
		}

		// 如果成功提交了命令（找到了领导者）
		if index != -1 {
			// 等待命令在集群中达成共识，设置 2 秒超时
			t1 := time.Now()
			DPrintf("开始检查是否达到共识，index为%d\n",index)
			for time.Since(t1).Seconds() < 2 {
				// 检查指定索引位置的命令提交情况
				DPrintf("cfg.nCommitted(%d)开始执行\n",index)
				nd, cmd1 := cfg.nCommitted(index)
				DPrintf("cfg.nCommitted(%d)执行完成,nd: %d,cmd1: %d\n",index,nd, cmd1)

				// 如果有足够的服务器提交了该命令
				if nd > 0 && nd >= expectedServers {
					DPrintf("已经确定nd >= expectedServers\n")
					// 并且提交的命令内容正确
					if cmd1 == cmd {
						// 返回命令在日志中的索引
						return index
					}
				}
				// 短暂休眠后继续检查
				time.Sleep(20 * time.Millisecond)
			}

			// 如果不允许重试，直接失败
			if retry == false {
				cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			// 如果没有找到领导者，短暂休眠后重试
			time.Sleep(50 * time.Millisecond)
		}
	}

	// 超时仍未达成共识，测试失败
	cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
	return -1
}

// start a Test.
// print the Test message.
// e.g. cfg.begin("Test (2B): RPC counts aren't too high")
func (cfg *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.rpcTotal()
	cfg.bytes0 = cfg.bytesTotal()
	cfg.cmds0 = 0
	cfg.maxIndex0 = cfg.maxIndex
}

// end a Test -- the fact that we got here means there
// was no failure.
// print the Passed message,
// and some performance numbers.
func (cfg *config) end() {
	cfg.checkTimeout()
	if cfg.t.Failed() == false {
		cfg.mu.Lock()
		t := time.Since(cfg.t0).Seconds()       // real time
		npeers := cfg.n                         // number of Raft peers
		nrpc := cfg.rpcTotal() - cfg.rpcs0      // number of RPC sends
		nbytes := cfg.bytesTotal() - cfg.bytes0 // number of bytes
		ncmds := cfg.maxIndex - cfg.maxIndex0   // number of Raft agreements reported
		cfg.mu.Unlock()

		fmt.Printf("  ... Passed --")
		fmt.Printf("  %4.1f  %d %4d %7d %4d\n", t, npeers, nrpc, nbytes, ncmds)
	}
}
