package raft

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "testing"
 import "fmt"
import "time"
import "math/rand"
// import "sync/atomic"
 import "sync"

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
// const RaftElectionTimeout = 1000 * time.Millisecond

func Test2A(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	DPrintf("make_config完成\n")
	defer cfg.cleanup()

	cfg.begin("Test (2A): election after network failure")
	DPrintf("checkOneLeader启动-1\n")
	leader1 := cfg.checkOneLeader()// 记录初始leader
	DPrintf("checkOneLeader完成-1\n")
	// if the leader disconnects, a new one should be elected.
	// 断开初始leader的连接，检查是否选举出新leader
	cfg.disconnect(leader1)
	DPrintf("disConnect完成-1\n")
	DPrintf("checkOneLeader启动-2\n")
	cfg.checkOneLeader()
	DPrintf("checkOneLeader完成-2\n")

	// if the old leader rejoins, that shouldn't
	// disturb the new leader.
	// 重新连接旧leader，检查新leader是否仍稳定（旧leader不应抢占）
	cfg.connect(leader1)
	DPrintf("conNect完成-1\n")
	DPrintf("checkOneLeader启动-3\n")
	leader2 := cfg.checkOneLeader()
	DPrintf("checkOneLeader完成-3\n")

	// if there's no quorum, no leader should
	// be elected.
	// 断开2个节点（仅剩1个节点，无多数派），检查是否无leader
	cfg.disconnect(leader2)
	DPrintf("disConnect完成-2\n")
	cfg.disconnect((leader2 + 1) % servers)
	DPrintf("disConnect完成-2\n")
	time.Sleep(2 * RaftElectionTimeout)
	DPrintf("checkNoLeader启动-4\n")
	cfg.checkNoLeader()
	DPrintf("checkNoLeader完成-4\n")

	// if a quorum arises, it should elect a leader.
	// 重新连接1个节点（恢复多数派），检查是否重新选举出leader
	cfg.connect((leader2 + 1) % servers)
	DPrintf("Connect完成-3\n")
	DPrintf("checkOneLeader启动-5\n")
	cfg.checkOneLeader()
	DPrintf("checkOneLeader完成-5\n")

	// re-join of last node shouldn't prevent leader from existing.
	// 重新连接最后1个节点，检查leader是否仍存在
	cfg.connect(leader2)
	DPrintf("Connect完成-4\n")
	DPrintf("checkOneLeader启动-6\n")
	cfg.checkOneLeader()
	DPrintf("checkOneLeader完成-6\n")

	cfg.end()
}

func Test1_2A(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)// 创建3个节点的测试集群
	defer cfg.cleanup()// 测试结束后清理资源

	// 检查是否选举出唯一leader
	cfg.begin("Test (2A): initial election")
	// is a leader elected?
	cfg.checkOneLeader()

	// 等待50ms让follower同步term，再检查所有节点term是否一致
	// sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the term.
	time.Sleep(50 * time.Millisecond)
	term1 := cfg.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %v, but should be at least 1", term1)
	}

	// does the leader+term stay the same if there is no network failure?
	// 等待2个选举超时，检查leader是否保持（无故障时leader不应变更）
	time.Sleep(2 * RaftElectionTimeout)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	// there should still be a leader.
	// 确认仍有leader
	cfg.checkOneLeader()

	cfg.end()
}

func TestBasicAgree2B_my(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	DPrintf("Test (2B): make_config完成")
	defer cfg.cleanup()

	cfg.begin("Test (2B): basic agreement")
	DPrintf("Test (2B): begin完成")
	iters := 3
	for index := 1; index < iters+1; index++ {
		DPrintf("Test (2B): 现在是第%v条日志", index)
		nd, _ := cfg.nCommitted(index) //index代表第index条日志
		if nd > 0 {
			t.Fatalf("some have committed before Start()")
		}
		DPrintf("Test (2B): 现在是第%v条日志，日志内容为%v", index, index*100)
		xindex := cfg.one(index*100, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}

	cfg.end()
}
// TestRPCBytes2B 测试RPC字节数统计
// 验证每个命令只发送给每个peer一次，通过统计RPC传输的字节数来验证
func TestBytes2B(t *testing.T) {
	// 设置测试环境：3个服务器节点，不启用随机化
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup() // 测试结束后清理资源

	// 开始测试：记录测试开始标记
	cfg.begin("Test (2B): RPC byte count")

	// 执行一次命令作为初始化，记录初始字节数
	cfg.one(99, servers, false)
	bytes0 := cfg.bytesTotal() // 获取初始传输的总字节数

	// 进行多次迭代测试
	iters := 10
	var sent int64 = 0 // 记录发送的命令总字节数

	// 从索引2开始执行10次命令提交
	for index := 2; index < iters+2; index++ {
		// 生成5000字节长度的随机字符串命令
		cmd := randstring(5000)

		// 提交命令并等待大多数节点应用，返回提交的索引
		xindex := cfg.one(cmd, servers, false)

		// 验证返回的索引是否正确
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}

		// 累计发送的字节数
		sent += int64(len(cmd))
	}

	// 计算测试期间新增的字节数
	bytes1 := cfg.bytesTotal()
	got := bytes1 - bytes0 // 实际传输的字节数

	// 计算期望的字节数：节点数 × 命令总字节数
	// 在Raft中，每个命令应该只发送给每个peer一次
	expected := int64(servers) * sent

	// 验证实际传输字节数不超过期望值+50000字节的容差
	// 50000字节的容差考虑了RPC头部和其他元数据的开销
	if got > expected+50000 {
		t.Fatalf("too many RPC bytes; got %v, expected %v", got, expected)
	}
	// 结束测试
	cfg.end()
}

// TestFailAgree2B 测试在网络分区情况下Raft集群仍能达成一致
// 验证当一个follower断开连接时，剩余的节点仍能正常工作并达成共识
func TestFailagree2B(t *testing.T) {
	// 设置测试环境：3个服务器节点，不启用随机化
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup() // 测试结束后清理资源

	// 开始测试：记录测试开始标记
	// 测试目标：验证即使有follower断开连接，集群仍能达成一致
	cfg.begin("Test (2B): agreement despite follower disconnection")

	// 执行一次初始化命令，确保集群正常启动
	// 所有3个节点都应该应用这个命令
	cfg.one(101, servers, false)

	// 断开一个follower的网络连接，模拟网络分区
	leader := cfg.checkOneLeader() // 首先确定当前的leader
	// 断开leader下一个节点的连接（循环方式选择follower）
	cfg.disconnect((leader + 1) % servers)

	// 验证剩余节点仍能达成一致
	// 现在只有2个节点可以通信（leader和一个follower）
	// 在3节点集群中，2个节点仍能形成多数派（quorum）
	cfg.one(102, servers-1, false)  // 只需要2个节点同意
	cfg.one(103, servers-1, false)  // 继续提交更多命令
	time.Sleep(RaftElectionTimeout) // 等待一个选举超时周期
	cfg.one(104, servers-1, false)  // 验证超时后仍能正常工作
	cfg.one(105, servers-1, false)  // 继续验证一致性

	// 恢复网络连接
	// 重新连接之前断开的follower
	cfg.connect((leader + 1) % servers)

	// 验证完整集群的一致性和新命令处理能力
	// 现在所有3个节点都在线，应该能达成3节点的一致
	cfg.one(106, servers, true)     // 需要所有3个节点同意
	time.Sleep(RaftElectionTimeout) // 等待选举超时，确保集群稳定
	cfg.one(107, servers, true)     // 继续验证完整集群的功能

	// 结束测试
	cfg.end()
}

// TestFailNoAgree2B 测试在网络分区情况下无法达成一致的场景
func TestFailnoagree2B(t *testing.T) {
	// 创建5个服务器节点的集群配置
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup() // 测试结束后清理资源

	cfg.begin("Test (2B): no agreement if too many followers disconnect")

	// 首先让集群达成一致，提交一个值10
	cfg.one(10, servers, false)

	// 找到当前的leader节点
	leader := cfg.checkOneLeader()
	
	// 断开3个follower节点的连接（总共5个节点，1个leader+3个断开=4个节点断开）
	// 这样只有leader自己能正常工作，无法形成多数派（3/5）
	cfg.disconnect((leader + 1) % servers)  // 断开第1个follower
	cfg.disconnect((leader + 2) % servers)  // 断开第2个follower
	cfg.disconnect((leader + 3) % servers)  // 断开第3个follower

	// leader尝试提交新值20（此时应该失败，因为没有多数派确认）
	index, _, ok := cfg.rafts[leader].Start(20)
	if ok != true {
		t.Fatalf("leader rejected Start()")  // leader应该接受这个请求
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %v", index)  // 期望日志索引为2
	}

	// 等待足够长时间，让可能的选举超时发生
	time.Sleep(2 * RaftElectionTimeout)

	// 检查索引2的日志条目是否被提交
	n, _ := cfg.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)  // 不应该有任何节点提交这个条目
	}

	// 修复网络分区：重新连接之前断开的3个节点
	cfg.connect((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.connect((leader + 3) % servers)

	// 网络恢复后，断开的节点多数可能已经选出了新的leader
	// 这个新leader可能不知道原来的索引2的条目
	leader2 := cfg.checkOneLeader()
	
	// 新leader尝试提交值30
	index2, _, ok2 := cfg.rafts[leader2].Start(30)
	if ok2 == false {
		t.Fatalf("leader2 rejected Start()")  // 新leader应该接受请求
	}
	// 由于可能的日志覆盖，新条目的索引可能是2或3
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %v", index2)
	}

	// 最终提交一个新值1000，确保所有节点都能达成一致
	cfg.one(1000, servers, true)

	cfg.end()  // 结束测试
}
func TestConcurrentstarts2B(t *testing.T) {
	// 创建3个服务器节点的集群配置
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup() // 测试结束后清理资源

	cfg.begin("Test (2B): concurrent Start()s")

	var success bool
	
// 外层循环：最多尝试5次测试
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// 给系统一些时间稳定
			time.Sleep(3 * time.Second)
		}

		// 找到当前的leader节点
		leader := cfg.checkOneLeader()
		
		// leader尝试提交第一个值1，获取当前任期
		_, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			// leader可能很快发生了变化，重新尝试
			continue
		}

		// 并发执行5次Start()操作
		iters := 5
		var wg sync.WaitGroup  // 等待组，用于等待所有goroutine完成
		is := make(chan int, iters)  // 通道，用于收集成功提交的日志索引
		
		// 启动5个并发的goroutine，每个提交不同的值
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)  // 增加等待计数
			go func(i int) {
				defer wg.Done()  // goroutine结束时减少等待计数
				// 每个goroutine尝试提交值(100+i)
				i, term1, ok := cfg.rafts[leader].Start(100 + i)
				if term1 != term {
					// 任期发生变化，说明leader可能已经变更
					return
				}
				if ok != true {
					// Start操作失败
					return
				}
				// 将成功提交的日志索引发送到通道
				is <- i
			}(ii)
		}

		// 等待所有并发操作完成
		wg.Wait()
		close(is)  // 关闭通道，表示不再发送数据

		// 检查所有节点的任期是否一致
		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// 任期发生变化，无法期望低RPC计数，重新开始测试
				continue loop
			}
		}

		// 检查并发提交的结果
		failed := false
		cmds := []int{}  // 存储所有成功提交的命令值
		
		// 收集所有成功提交的命令
		for index := range is {
			cmd := cfg.wait(index, servers, term)  // 等待指定索引的日志在多数节点上提交
			if ix, ok := cmd.(int); ok {
				if ix == -1 {
					// 节点已经进入新的任期，无法期望所有Start()都成功
					failed = true
					break
				}
				cmds = append(cmds, ix)  // 将命令值添加到列表
			} else {
				t.Fatalf("value %v is not an int", cmd)  // 类型断言失败
			}
		}

		// 如果测试失败，启动goroutine清理通道中的剩余数据，避免goroutine泄漏
		if failed {
			// avoid leaking goroutines
			go func() {
				for range is {
				}
			}()
			continue  // 重新尝试测试
		}

		// 验证所有预期的命令都已成功提交
		for ii := 0; ii < iters; ii++ {
			x := 100 + ii  // 期望的命令值
			ok := false
			// 检查命令值是否存在于提交的命令列表中
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true  // 找到该命令值
				}
			}
			if ok == false {
				t.Fatalf("cmd %v missing in %v", x, cmds)  // 命令缺失，测试失败
			}
		}

		// 所有验证通过，测试成功
		success = true
		break
	}

	// 如果5次尝试都失败，说明任期变化太频繁
	if !success {
		t.Fatalf("term changed too often")
	}

	cfg.end()  // 结束测试
}

// TestRejoin2B 测试分区leader重新加入集群的场景
func TestRejoin2b(t *testing.T) {
	// 创建3个服务器节点的集群配置
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup() // 测试结束后清理资源

	cfg.begin("Test (2B): rejoin of partitioned leader")

	// 首先让所有节点达成一致，提交值101
	cfg.one(101, servers, true)

	// 模拟leader网络故障：断开当前leader的连接
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)  // 断开leader1的网络连接

	// 失联的旧leader仍然尝试提交一些条目（这些提交会失败）
	cfg.rafts[leader1].Start(102)  // 旧leader尝试提交102
	cfg.rafts[leader1].Start(103)  // 旧leader尝试提交103
	cfg.rafts[leader1].Start(104)  // 旧leader尝试提交104

	// 剩余节点选出新leader并提交值103（索引为2）
	cfg.one(103, 2, true)  // 在2个节点上达成一致

	// 模拟新leader也发生网络故障：断开新leader的连接
	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)  // 断开leader2的网络连接

	// 重新连接旧leader（leader1）
	cfg.connect(leader1)  // 重新连接旧leader

	// 此时只有leader1和一个follower在线，提交值104
	cfg.one(104, 2, true)  // 在2个节点上达成一致

	// 最后重新连接所有节点
	cfg.connect(leader2)  // 重新连接之前断开的leader2

	// 所有节点都在线，提交最终值105
	cfg.one(105, servers, true)  // 在所有节点上达成一致

	cfg.end()  // 结束测试
}

// TestCount2B 测试 Raft 实现中 RPC 调用数量是否在合理范围内
func TestCount2b(t *testing.T) {
	// 启动 3 个 Raft 服务器节点
	servers := 3
	cfg := make_config(t, servers, false) // 创建测试配置
	defer cfg.cleanup()                   // 测试结束时清理资源

	// 开始测试阶段标记
	cfg.begin("Test (2B): RPC counts aren't too high")

	// 定义一个匿名函数，用来统计所有节点的 RPC 调用总数
	rpcs := func() (n int) {
		for j := 0; j < servers; j++ {
			n += cfg.rpcCount(j) // 累加每个节点的 RPC 数量
		}
		return
	}

	// 检查当前集群中是否只有一个 Leader
	leader := cfg.checkOneLeader()

	// 获取初始 RPC 总数
	total1 := rpcs()

	// 初始选举阶段的 RPC 数量应在 [1, 30] 区间内
	if total1 > 30 || total1 < 1 {
		t.Fatalf("too many or few RPCs (%v) to elect initial leader\n", total1)
	}

	var total2 int
	var success bool

	// 最多尝试 5 次执行日志提交流程
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// 如果不是第一次尝试，等待一段时间让系统稳定
			time.Sleep(3 * time.Second)
		}

		// 再次确认当前只有一个 Leader
		leader = cfg.checkOneLeader()
		total1 = rpcs() // 更新 RPC 总数

		// 准备提交 10 条命令
		iters := 10
		starti, term, ok := cfg.rafts[leader].Start(1) // 启动第一条命令
		if !ok {
			// 如果 Start 失败（比如不再是 Leader），跳过此次尝试
			continue
		}

		cmds := []int{} // 保存将要提交的命令值
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31()) // 随机生成一个整数命令
			cmds = append(cmds, x)

			// 提交命令
			index1, term1, ok := cfg.rafts[leader].Start(x)
			if term1 != term {
				// 如果任期发生变化，说明 Leader 可能变更，跳过此次尝试
				continue loop
			}
			if !ok {
				// 不再是 Leader，任期变化，跳过
				continue loop
			}
			if starti+i != index1 {
				// 命令索引不连续，出错
				t.Fatalf("Start() failed")
			}
		}

		// 等待这些命令被提交并验证它们的值是否正确
		for i := 1; i < iters+1; i++ {
			cmd := cfg.wait(starti+i, servers, term) // 等待指定索引的日志被提交
			if ix, ok := cmd.(int); ok == false || ix != cmds[i-1] {
				if ix == -1 {
					// 如果返回 -1，表示任期变化，需要重新尝试
					continue loop
				}
				// 命令值不匹配，报错
				t.Fatalf("wrong value %v committed for index %v; expected %v\n", cmd, starti+i, cmds)
			}
		}

		// 检查所有节点的任期是否一致，如果不一致则跳过
		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// 任期变化，不能保证低 RPC 数量，标记为失败
				failed = true
			}
			total2 += cfg.rpcCount(j) // 累加当前所有节点的 RPC 数量
		}

		if failed {
			continue loop
		}

		// 检查新增的 RPC 数量是否在合理范围内
		// (iters+1+3)*3 表示每次提交最多允许 3 倍的 RPC（包括心跳、AppendEntries 等）
		if total2-total1 > (iters+1+3)*3 {
			t.Fatalf("too many RPCs (%v) for %v entries\n", total2-total1, iters)
		}

		// 成功完成测试，跳出循环
		success = true
		break
	}

	// 如果多次尝试后仍未成功，说明任期频繁变更，报错
	if !success {
		t.Fatalf("term changed too often")
	}

	// 等待一段时间（模拟空闲状态）
	time.Sleep(RaftElectionTimeout)

	// 统计空闲期间的 RPC 数量
	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += cfg.rpcCount(j)
	}

	// 空闲期间的 RPC 数量应小于 3*20（即每个节点最多 20 次）
	if total3-total2 > 3*20 {
		t.Fatalf("too many RPCs (%v) for 1 second of idleness\n", total3-total2)
	}

	// 结束测试阶段
	cfg.end()
}

//TestBackup2B 测试leader如何快速回退以处理follower的错误日志
func TestBackUp2B(t *testing.T) {
	// 创建5个服务器节点的集群配置
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup() // 测试结束后清理资源

	cfg.begin("Test (2B): leader backs up quickly over incorrect follower logs")

	// 首先提交一个随机值，确保集群正常工作
	cfg.one(rand.Int(), servers, true)

	// 创建第一个分区：只有leader1和一个follower在线（2个节点）
	leader1 := cfg.checkOneLeader()
	cfg.disconnect((leader1 + 2) % servers)  // 断开3个节点
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	// leader1提交大量命令（这些命令无法提交，因为没有多数派）
	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Start(rand.Int())  // 这些命令会堆积在leader1的日志中
	}

	// 等待一段时间（半个选举超时）
	time.Sleep(RaftElectionTimeout / 2)

	// 断开原来的leader1和其follower，形成新的分区
	cfg.disconnect((leader1 + 0) % servers)  // 断开leader1
	cfg.disconnect((leader1 + 1) % servers)  // 断开其follower

	// 恢复另外3个节点的连接，让它们形成新的多数派（3个节点）
	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)

	// 新的3节点集群提交大量命令（这些都能成功提交）
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)  // 在3个节点上达成一致
	}

	// 现在创建第二个分区：新的leader2和一个follower在线
	leader2 := cfg.checkOneLeader()  // 找到新的leader
	other := (leader1 + 2) % servers  // 选择一个节点
	if leader2 == other {
		other = (leader2 + 1) % servers  // 避免选择leader自己
	}
	cfg.disconnect(other)  // 断开一个节点

	// leader2提交大量命令（这些也无法提交，因为只有2个节点）
	for i := 0; i < 50; i++ {
		cfg.rafts[leader2].Start(rand.Int())  // 这些命令会堆积在leader2的日志中
	}

	// 等待一段时间
	time.Sleep(RaftElectionTimeout / 2)

	// 重新激活原始的leader1分区
	// 首先断开所有节点连接
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}
	// 然后连接原始leader1分区的3个节点
	cfg.connect((leader1 + 0) % servers)  // 连接原始leader1
	cfg.connect((leader1 + 1) % servers)  // 连接其follower
	cfg.connect(other)                   // 连接之前断开的节点

	// 这个3节点集群提交大量命令（这些都能成功提交）
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)  // 在3个节点上达成一致
	}

	// 最后连接所有节点，恢复完整集群
	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}
	
	// 提交最终命令，确保所有节点达成一致
	cfg.one(rand.Int(), servers, true)

	cfg.end()  // 结束测试
}