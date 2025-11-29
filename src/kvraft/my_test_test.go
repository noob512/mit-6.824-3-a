package kvraft

//import "../porcupine"
//import "../models"
import "testing"
import "strconv"
import "time"
import "math/rand"
import "log"
import "strings"
//import "sync"
import "sync/atomic"
//import "fmt"
//import "io/ioutil"

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const electionTimeout = 1 * time.Second

const linearizabilityCheckTimeout = 1 * time.Second

// get/put/putappend that keep counts
func Get(cfg *config, ck *Clerk, key string) string {
	v := ck.Get(key)
	cfg.op()
	return v
}

func Put(cfg *config, ck *Clerk, key string, value string) {
	ck.Put(key, value) // 通过客户端代理向分布式KV系统发送Put请求
	cfg.op()          // 记录一次操作，用于统计操作数量
}

func Append(cfg *config, ck *Clerk, key string, value string) {
	ck.Append(key, value)
	cfg.op()
}

func check(cfg *config, t *testing.T, ck *Clerk, key string, value string) {
	v := Get(cfg, ck, key)
	if v != value {
		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
	}
}

func run_client(t *testing.T, cfg *config, me int, ca chan bool, fn func(me int, ck *Clerk, t *testing.T)) {
	ok := false
	defer func() { ca <- ok }()  // defer语句，最后执行
	
	ck := cfg.makeClient(cfg.All())  // 创建客户端
	ck.pos=me
	
	fn(me, ck, t)  // 执行fn函数 ← 这里是执行fn的地方
	
	ok = true      // fn执行完成后才执行这行
	
	cfg.deleteClient(ck)  // fn执行完成后才执行清理 ← 这里执行清理
}

// spawn_clients_and_wait 启动ncli个客户端并等待它们全部完成
// t: 测试对象，用于错误报告
// cfg: 配置对象，包含服务器集群信息
// ncli: 客户端数量
// fn: 每个客户端执行的函数，参数为(客户端编号, 客户端对象, 测试对象)
func spawn_clients_and_wait(t *testing.T, cfg *config, ncli int, fn func(me int, ck *Clerk, t *testing.T)) {
	ca := make([]chan bool, ncli)  // 创建结果通道数组，用于接收每个客户端的执行结果
	
	// 启动ncli个客户端goroutine
	for cli := 0; cli < ncli; cli++ {
		ca[cli] = make(chan bool)  // 为每个客户端创建结果通道
		go run_client(t, cfg, cli, ca[cli], fn)  // 并发运行客户端
	}
	
	// 等待所有客户端完成
	for cli := 0; cli < ncli; cli++ {
		ok := <-ca[cli]  // 接收客户端执行结果
		if ok == false { // 如果客户端执行失败
			t.Fatalf("failure")  // 测试失败
		}
	}
}

// predict effect of Append(k, val) if old value is prev.
func NextValue(prev string, val string) string {
	return prev + val
}

// check that for a specific client all known appends are present in a value,
// and in order
// 验证客户端追加操作的结果是否正确
// 参数：t - 测试对象，clnt - 客户端ID，v - 从服务器获取的完整字符串值，count - 追加操作的次数
func checkClntAppends(t *testing.T, clnt int, v string, count int) {
	lastoff := -1 // 记录上一个找到的元素在字符串中的位置，用于验证顺序
	
	// 遍历期望的每个追加元素
	for j := 0; j < count; j++ {
		// 构造期望的追加值格式："x 客户端ID j y"
		wanted := "x " + strconv.Itoa(clnt) + " " + strconv.Itoa(j) + " y"
		
		// 查找当前期望值在结果字符串中的位置
		off := strings.Index(v, wanted)
		if off < 0 { // 如果没有找到期望的值
			t.Fatalf("%v missing element %v in Append result %v", clnt, wanted, v)
		}
		
		// 检查是否存在重复元素（通过查找最后一次出现的位置）
		off1 := strings.LastIndex(v, wanted)
		if off1 != off { // 如果第一次出现和最后一次出现位置不同，说明有重复
			t.Fatalf("duplicate element %v in Append result", wanted)
		}
		
		// 验证元素的顺序是否正确（当前元素的位置应该比上一个元素的位置大）
		if off <= lastoff { // 如果当前元素位置小于等于上一个元素位置，说明顺序错误
			t.Fatalf("wrong order for element %v in Append result", wanted)
		}
		
		lastoff = off // 更新上一个元素的位置
	}
}

// check that all known appends are present in a value,
// and are in order for each concurrent client.
func checkConcurrentAppends(t *testing.T, v string, counts []int) {
	nclients := len(counts)
	for i := 0; i < nclients; i++ {
		lastoff := -1
		for j := 0; j < counts[i]; j++ {
			wanted := "x " + strconv.Itoa(i) + " " + strconv.Itoa(j) + " y"
			off := strings.Index(v, wanted)
			if off < 0 {
				t.Fatalf("%v missing element %v in Append result %v", i, wanted, v)
			}
			off1 := strings.LastIndex(v, wanted)
			if off1 != off {
				t.Fatalf("duplicate element %v in Append result", wanted)
			}
			if off <= lastoff {
				t.Fatalf("wrong order for element %v in Append result", wanted)
			}
			lastoff = off
		}
	}
}

// repartition the servers periodically
func partitioner(t *testing.T, cfg *config, ch chan bool, done *int32) {
	defer func() { ch <- true }()
	for atomic.LoadInt32(done) == 0 {
		a := make([]int, cfg.n)
		for i := 0; i < cfg.n; i++ {
			a[i] = (rand.Int() % 2)
		}
		pa := make([][]int, 2)
		for i := 0; i < 2; i++ {
			pa[i] = make([]int, 0)
			for j := 0; j < cfg.n; j++ {
				if a[j] == i {
					pa[i] = append(pa[i], j)
				}
			}
		}
		cfg.partition(pa[0], pa[1])
		time.Sleep(electionTimeout + time.Duration(rand.Int63()%200)*time.Millisecond)
	}
}

// 基本测试如下：一个或多个客户端在一段时间内向服务器组提交Append/Get操作。
// 一段时间结束后，测试检查所有追加的值是否按特定键存在且有序。
// 如果unreliable为true，RPC可能会失败。如果crash为true，则服务器在一段时间后崩溃并重启。
// 如果partitions为true，则测试在网络重新分区的同时运行客户端和服务器。
// 如果maxraftstate为正数，则Raft的状态大小（即日志大小）不应超过8*maxraftstate。
// 如果maxraftstate为负数，则不应使用快照。
func GenericTest(t *testing.T, part string, nclients int, unreliable bool, crash bool, partitions bool, maxraftstate int) {

	// 构建测试标题，根据不同的测试条件添加描述
	title := "Test: "
	if unreliable {
		// 网络会丢弃RPC请求和回复
		title = title + "unreliable net, "
	}
	if crash {
		// 节点重新启动，因此持久化必须有效
		title = title + "restarts, "
	}
	if partitions {
		// 网络可能分区
		title = title + "partitions, "
	}
	if maxraftstate != -1 {
		title = title + "snapshots, " // 如果maxraftstate不是-1，则启用快照
	}
	if nclients > 1 {
		title = title + "many clients" // 多客户端测试
	} else {
		title = title + "one client"   // 单客户端测试
	}
	title = title + " (" + part + ")" // 3A or 3B

	const nservers = 5 // 服务器数量固定为5
	cfg := make_config(t, nservers, unreliable, maxraftstate) // 创建测试配置
	DPrintf("make_config完成")
	defer cfg.cleanup() // 确保测试结束后清理资源

	cfg.begin(title) // 开始测试

	ck := cfg.makeClient(cfg.All()) // 创建客户端
	DPrintf("makeClient完成")
	// 控制分隔器和客户端的退出标志
	done_partitioner := int32(0) // 分隔器退出标志
	done_clients := int32(0)     // 客户端退出标志
	ch_partitioner := make(chan bool) // 分隔器完成通知通道
	clnts := make([]chan int, nclients) // 每个客户端的计数通道
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int) // 为每个客户端创建计数通道
	}
	// 运行3轮测试
	for i := 0; i < 3; i++ {
		DPrintf("第%v轮测试开始",i)
		// log.Printf("Iteration %v\n", i)
		atomic.StoreInt32(&done_clients, 0)   // 重置客户端退出标志
		atomic.StoreInt32(&done_partitioner, 0) // 重置分隔器退出标志
		
		// 启动客户端goroutine
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0 // 操作计数器
			get_num:=0
			defer func() {
				DPrintf("spawn_clients_and_wait完成/n")
				clnts[cli] <- j // 将操作计数发送到对应通道
			}()
			
			last := "" // 记录最后的值
			key := strconv.Itoa(cli) // 使用客户端ID作为键
			Put(cfg, myck, key, last) // 初始化键值对
			DPrintf("Put完成/n")
			// 当客户端未被要求退出时继续操作
			for atomic.LoadInt32(&done_clients) == 0 {
				if (rand.Int() % 1000) < 500{ // 50%概率执行Append操作
					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y" // 生成新的值
					// log.Printf("%d: client new append %v\n", cli, nv)
					Append(cfg, myck, key, nv) // 执行追加操作
					last = NextValue(last, nv) // 更新期望的最后值
					j++ // 操作计数递增
					DPrintf("Append完成/n")
				} else { // 50%概率执行Get操作
					// log.Printf("%d: client new get %v\n", cli, key)
					v := Get(cfg, myck, key) // 获取当前值
					get_num++
					DPrintf("Get完成/n")
					if v != last { // 验证获取的值是否正确
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
			}
			DPrintf("该协程运行结束即将退出\n")
		})

		// 如果启用网络分区，则启动分隔器
		if partitions {
			// 允许客户端在没有干扰的情况下执行一些操作
			time.Sleep(1 * time.Second)
			go partitioner(t, cfg, ch_partitioner, &done_partitioner) // 启动网络分隔器
		}
		
		// 让测试运行5秒
		
		time.Sleep(5 * time.Second)

		// 设置退出标志，通知客户端和分隔器退出
		atomic.StoreInt32(&done_clients, 1)     // 通知客户端退出
		atomic.StoreInt32(&done_partitioner, 1) // 通知分隔器退出
		DPrintf("已经设定退出标志")

		// 如果启用了网络分区，处理分隔器的清理
		if partitions {
			// log.Printf("wait for partitioner\n")
			<-ch_partitioner // 等待分隔器完成
			// 重新连接网络并提交请求。客户端可能在少数派中提交了请求，
			// 该请求在服务器发现新任期开始之前不会返回。
			cfg.ConnectAll() // 重新连接所有网络
			// 等待一段时间以确保出现新任期
			time.Sleep(electionTimeout)
		}

		// 如果启用了崩溃测试，执行服务器崩溃和重启
		if crash {
			// log.Printf("shutdown servers\n")
			for i := 0; i < nservers; i++ {
				cfg.ShutdownServer(i) // 关闭每个服务器
			}
			// 等待一段时间让服务器关闭，因为关闭不是真正的崩溃，也不是即时的
			time.Sleep(electionTimeout)
			// log.Printf("restart servers\n")
			// 崩溃并重新启动所有服务器
			for i := 0; i < nservers; i++ {
				cfg.StartServer(i) // 重新启动每个服务器
			}
			cfg.ConnectAll() // 重新连接所有网络
		}

		// 等待所有客户端完成并验证结果
		DPrintf("wait for clients\n")
		//time.Sleep(20 * time.Second)
		for i := 0; i < nclients; i++ {
			// log.Printf("read from clients %d\n", i)
			DPrintf("从通道获取客户端操作计数卡住\n")
			j := <-clnts[i] // 从通道获取客户端操作计数
			DPrintf("从通道获取客户端操作计数完成\n")
			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			key := strconv.Itoa(i) // 构建键
			// log.Printf("Check %v for client %d\n", j, i)
			DPrintf("Check-key %v \n", key)
			v := Get(cfg, ck, key) // 获取最终值
			checkClntAppends(t, i, v, j) // 验证客户端追加操作的结果
		}
		DPrintf("wait for clients 完成\n")

		// 检查日志大小限制（如果启用了快照）
		if maxraftstate > 0 {
			// 在服务器处理完所有客户端请求并有时间进行检查点后检查最大大小
			sz := cfg.LogSize() // 获取日志大小
			if sz > 8*maxraftstate { // 检查是否超过限制
				t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
			}
		}
		
		// 检查快照是否未被使用（如果maxraftstate为负数）
		if maxraftstate < 0 {
			// 检查快照是否未被使用
			ssz := cfg.SnapshotSize() // 获取快照大小
			if ssz > 0 { // 如果快照被使用了（大小大于0），则失败
				t.Fatalf("snapshot too large (%v), should not be used when maxraftstate = %d", ssz, maxraftstate)
			}
		}
	}

	cfg.end() // 结束测试
}

// similar to GenericTest, but with clients doing random operations (and using a
// linearizability checker)
// func GenericTestLinearizability(t *testing.T, part string, nclients int, nservers int, unreliable bool, crash bool, partitions bool, maxraftstate int) {

// 	title := "Test: "
// 	if unreliable {
// 		// the network drops RPC requests and replies.
// 		title = title + "unreliable net, "
// 	}
// 	if crash {
// 		// peers re-start, and thus persistence must work.
// 		title = title + "restarts, "
// 	}
// 	if partitions {
// 		// the network may partition
// 		title = title + "partitions, "
// 	}
// 	if maxraftstate != -1 {
// 		title = title + "snapshots, "
// 	}
// 	if nclients > 1 {
// 		title = title + "many clients"
// 	} else {
// 		title = title + "one client"
// 	}
// 	title = title + ", linearizability checks (" + part + ")" // 3A or 3B

// 	cfg := make_config(t, nservers, unreliable, maxraftstate)
// 	defer cfg.cleanup()

// 	cfg.begin(title)

// 	begin := time.Now()
// 	var operations []porcupine.Operation
// 	var opMu sync.Mutex

// 	done_partitioner := int32(0)
// 	done_clients := int32(0)
// 	ch_partitioner := make(chan bool)
// 	clnts := make([]chan int, nclients)
// 	for i := 0; i < nclients; i++ {
// 		clnts[i] = make(chan int)
// 	}
// 	for i := 0; i < 3; i++ {
// 		// log.Printf("Iteration %v\n", i)
// 		atomic.StoreInt32(&done_clients, 0)
// 		atomic.StoreInt32(&done_partitioner, 0)
// 		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
// 			j := 0
// 			defer func() {
// 				clnts[cli] <- j
// 			}()
// 			for atomic.LoadInt32(&done_clients) == 0 {
// 				key := strconv.Itoa(rand.Int() % nclients)
// 				nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
// 				var inp models.KvInput
// 				var out models.KvOutput
// 				start := int64(time.Since(begin))
// 				if (rand.Int() % 1000) < 500 {
// 					Append(cfg, myck, key, nv)
// 					inp = models.KvInput{Op: 2, Key: key, Value: nv}
// 					j++
// 				} else if (rand.Int() % 1000) < 100 {
// 					Put(cfg, myck, key, nv)
// 					inp = models.KvInput{Op: 1, Key: key, Value: nv}
// 					j++
// 				} else {
// 					v := Get(cfg, myck, key)
// 					inp = models.KvInput{Op: 0, Key: key}
// 					out = models.KvOutput{Value: v}
// 				}
// 				end := int64(time.Since(begin))
// 				op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: cli}
// 				opMu.Lock()
// 				operations = append(operations, op)
// 				opMu.Unlock()
// 			}
// 		})

// 		if partitions {
// 			// Allow the clients to perform some operations without interruption
// 			time.Sleep(1 * time.Second)
// 			go partitioner(t, cfg, ch_partitioner, &done_partitioner)
// 		}
// 		time.Sleep(5 * time.Second)

// 		atomic.StoreInt32(&done_clients, 1)     // tell clients to quit
// 		atomic.StoreInt32(&done_partitioner, 1) // tell partitioner to quit

// 		if partitions {
// 			// log.Printf("wait for partitioner\n")
// 			<-ch_partitioner
// 			// reconnect network and submit a request. A client may
// 			// have submitted a request in a minority.  That request
// 			// won't return until that server discovers a new term
// 			// has started.
// 			cfg.ConnectAll()
// 			// wait for a while so that we have a new term
// 			time.Sleep(electionTimeout)
// 		}

// 		if crash {
// 			// log.Printf("shutdown servers\n")
// 			for i := 0; i < nservers; i++ {
// 				cfg.ShutdownServer(i)
// 			}
// 			// Wait for a while for servers to shutdown, since
// 			// shutdown isn't a real crash and isn't instantaneous
// 			time.Sleep(electionTimeout)
// 			// log.Printf("restart servers\n")
// 			// crash and re-start all
// 			for i := 0; i < nservers; i++ {
// 				cfg.StartServer(i)
// 			}
// 			cfg.ConnectAll()
// 		}

// 		// wait for clients.
// 		for i := 0; i < nclients; i++ {
// 			<-clnts[i]
// 		}

// 		if maxraftstate > 0 {
// 			// Check maximum after the servers have processed all client
// 			// requests and had time to checkpoint.
// 			sz := cfg.LogSize()
// 			if sz > 8*maxraftstate {
// 				t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
// 			}
// 		}
// 	}

// 	cfg.end()

// 	res, info := porcupine.CheckOperationsVerbose(models.KvModel, operations, linearizabilityCheckTimeout)
// 	if res == porcupine.Illegal {
// 		file, err := ioutil.TempFile("", "*.html")
// 		if err != nil {
// 			fmt.Printf("info: failed to create temp file for visualization")
// 		} else {
// 			err = porcupine.Visualize(models.KvModel, info, file)
// 			if err != nil {
// 				fmt.Printf("info: failed to write history visualization to %s\n", file.Name())
// 			} else {
// 				fmt.Printf("info: wrote history visualization to %s\n", file.Name())
// 			}
// 		}
// 		t.Fatal("history is not linearizable")
// 		t.Fatal("history is not linearizable")
// 	} else if res == porcupine.Unknown {
// 		fmt.Println("info: linearizability check timed out, assuming history is ok")
// 	}
// }

// func TestBasic3A(t *testing.T) {
// 	// Test: one client (3A) ...
// 	GenericTest(t, "3A", 1, false, false, false, -1)
// }

// func TestConcurrent3A(t *testing.T) {
// 	// Test: many clients (3A) ...
// 	GenericTest(t, "3A", 5, false, false, false, -1)
// }

// func TestUnreliable3A(t *testing.T) {
// 	// Test: unreliable net, many clients (3A) ...
// 	GenericTest(t, "3A", 5, true, false, false, -1)
// }

// TestUnreliableOneKey3A 测试在不可靠网络环境下，
// 多个客户端并发地对同一个键（key）执行 Append 操作的正确性（属于 Lab 3A 范畴）。
// 该测试验证 KV 服务在 leader 更换、网络丢包等异常情况下，
// 仍能保证操作的线性一致性（linearizability）和无重复执行（exactly-once 语义）。
// func TestUnreliableOneKey3A(t *testing.T) {
// 	// 定义集群中 Raft 节点数量为 3（最小多数派：2/3）
// 	const nservers = 3

// 	// 创建一个测试配置（config）：
// 	// - 启动 nservers 个 Raft 节点
// 	// - 启用不可靠网络（模拟丢包、延迟）
// 	// - -1 表示不启用日志快照（log 不会被截断）
// 	cfg := make_config(t, nservers, true, -1)
// 	// 测试结束后自动清理资源（停止所有节点、客户端等）
// 	defer cfg.cleanup()

// 	// 创建一个客户端 ck，该客户端可连接到集群中任意节点
// 	ck := cfg.makeClient(cfg.All())

// 	// 记录测试开始日志，便于调试和追踪
// 	cfg.begin("Test: concurrent append to same key, unreliable (3A)")

// 	// 初始化键 "k" 的值为空字符串，确保测试从干净状态开始
// 	Put(cfg, ck, "k", "")

// 	// 定义并发客户端数量和每个客户端的 Append 操作次数
// 	const nclient = 5   // 启动 5 个并发客户端
// 	const upto = 10     // 每个客户端执行 10 次 Append

// 	// 启动 nclient 个并发客户端 goroutine，并等待它们全部完成
// 	spawn_clients_and_wait(t, cfg, nclient, func(me int, myck *Clerk, t *testing.T) {
// 		n := 0
// 		for n < upto {
// 			// 每个客户端向同一个键 "k" 追加唯一标识的字符串：
// 			// 格式为 "x <client_id> <operation_index> y"
// 			// 例如："x 2 5 y" 表示客户端 2 的第 5 次操作
// 			Append(cfg, myck, "k", "x "+strconv.Itoa(me)+" "+strconv.Itoa(n)+" y")
// 			n++
// 		}
// 	})

// 	// 构建期望的每客户端操作次数列表（用于后续验证）
// 	// 因为每个客户端都执行了 `upto` 次 Append，所以 counts = [10, 10, 10, 10, 10]
// 	var counts []int
// 	for i := 0; i < nclient; i++ {
// 		counts = append(counts, upto)
// 	}

// 	// 所有客户端操作完成后，读取最终的键 "k" 的值
// 	vx := Get(cfg, ck, "k")

// 	// 验证最终值是否包含每个客户端恰好 `upto` 次 Append 的内容，
// 	// 且操作顺序满足线性一致性（不允许丢失、重复或乱序到违反因果）
// 	// checkConcurrentAppends 会解析 vx 字符串，统计各客户端操作出现次数
// 	checkConcurrentAppends(t, vx, counts)

// 	// 记录测试结束日志
// 	cfg.end()
// }

// Submit a request in the minority partition and check that the requests
// doesn't go through until the partition heals.  The leader in the original
// network ends up in the minority partition.
// TestOnePartition3A 测试在网络分区（network partition）场景下，
// Raft 集群的多数派（majority）能否继续提供服务，而少数派（minority）是否被正确阻塞，
// 以及在分区恢复后系统能否恢复正常并保证一致性（属于 Lab 3A 的核心容错测试）。
func TestOnePartition3A(t *testing.T) {
	// 启动一个包含 5 个节点的 Raft 集群（奇数节点便于划分为多数/少数）
	const nservers = 5
	cfg := make_config(t, nservers, false, -1) // 不启用不可靠网络（unreliable=false），不启用快照
	defer cfg.cleanup()                         // 测试结束自动清理资源

	// 创建一个全局客户端，可连接任意节点
	ck := cfg.makeClient(cfg.All())

	// 初始化键 "1" 的值为 "13"
	Put(cfg, ck, "1", "13")

	// =============== 第一阶段：验证多数派能正常推进 ===============
	cfg.begin("Test: progress in majority (3A)")

	// 将 5 个节点划分为两个分区：
	// - p1: 包含 3 个节点（多数派，可形成 quorum）
	// - p2: 包含 2 个节点（少数派，无法选举 leader 或提交日志）
	p1, p2 := cfg.make_partition()
	cfg.partition(p1, p2) // 实施网络隔离，p1 和 p2 之间无法通信

	// 为每个分区创建专用客户端：
	ckp1 := cfg.makeClient(p1)   // 仅连接 p1（多数派）
	ckp2a := cfg.makeClient(p2)  // 仅连接 p2（少数派）
	ckp2b := cfg.makeClient(p2)  // 另一个连接 p2 的客户端（用于并发测试）

	// 通过多数派客户端写入新值 "14"
	Put(cfg, ckp1, "1", "14")
	// 验证读取结果为 "14" —— 说明多数派仍可正常处理请求
	check(cfg, t, ckp1, "1", "14")

	cfg.end()

	// =============== 第二阶段：验证少数派无法推进 ===============
	cfg.begin("Test: no progress in minority (3A)")

	// 启动两个并发 goroutine，尝试在少数派 p2 上执行操作：
	done0 := make(chan bool)
	done1 := make(chan bool)

	// goroutine 1: 尝试在 p2 上 Put("1", "15")
	go func() {
		Put(cfg, ckp2a, "1", "15")
		done0 <- true
	}()

	// goroutine 2: 尝试在 p2 上 Get("1")
	go func() {
		Get(cfg, ckp2b, "1")
		done1 <- true
	}()

	// 等待最多 1 秒：
	// - 如果任一操作完成，说明少数派错误地提供了服务（违反 Raft 安全性），测试失败
	select {
	case <-done0:
		t.Fatalf("Put in minority completed") // 少数派不应完成写入
	case <-done1:
		t.Fatalf("Get in minority completed") // 少数派也不应完成读取（因无有效 leader）
	case <-time.After(time.Second):
		// 预期行为：两个操作都因无法形成 quorum 而阻塞（超时未完成）
	}

	// 再次验证多数派状态未受影响，值仍为 "14"
	check(cfg, t, ckp1, "1", "14")
	// 并在多数派上继续写入 "16"
	Put(cfg, ckp1, "1", "16")
	check(cfg, t, ckp1, "1", "16")

	cfg.end()

	// =============== 第三阶段：验证分区恢复后系统一致性 ===============
	cfg.begin("Test: completion after heal (3A)")

	// 恢复全网连通性：所有节点重新互相通信
	cfg.ConnectAll()
	// 更新客户端连接，使其可访问所有节点
	cfg.ConnectClient(ckp2a, cfg.All())
	cfg.ConnectClient(ckp2b, cfg.All())

	// 等待足够时间让新 leader 选举完成（至少一个 election timeout）
	time.Sleep(electionTimeout)

	// 此时，之前在 minority 中阻塞的 Put("1", "15") 应该能够完成（因为集群已恢复）
	select {
	case <-done0:
		// 预期：Put 完成
	case <-time.After(30 * 100 * time.Millisecond): // 3 秒
		t.Fatalf("Put did not complete")
	}

	// 同样，之前阻塞的 Get 也应完成
	select {
	case <-done1:
	case <-time.After(30 * 100 * time.Millisecond):
		t.Fatalf("Get did not complete")
	default:
	}

	// 关键验证：最终值应为 "15"
	// 为什么不是 "16"？
	// 注意：Put("1", "15") 是在 Put("1", "16") **之前**发起的（虽然被阻塞），
	// 但在分区恢复后，它可能被新 leader 接收并提交。
	// 然而，**更关键的是：该测试假设 Put("1", "15") 最终被提交**，
	// 而之前的 "16" 可能因 leader 切换、日志冲突等原因被覆盖或未 commit。
	//
	// 实际上，MIT 6.824 的测试框架中，**在分区期间未完成的请求，
	// 在恢复后重试时会被视为新请求**。但此处 `ckp2a` 是同一个 Clerk，
	// 会携带相同的 ClientID 和 SeqNum，因此 Put("15") 会被去重或按序执行。
	//
	// 不过，根据原始 6.824 测试逻辑，此处 **预期最终值为 "15"**，
	// 意味着 Put("15") 在恢复后成功提交，且覆盖了 "16" —— 
	// 这通常是因为 Put("16") 虽然在多数派成功，但在后续 leader 切换中，
	// 若新 leader 来自包含 Put("15") 请求的路径（且其日志 term 更高），
	// 可能导致 "16" 被回滚（这是 Raft 允许的，只要未提交到状态机）。
	//
	// 但更合理的解释是：**Put("16") 已提交（applied），所以不应被覆盖**。
	// 实际上，官方测试中此处的预期值应为 "16"，但原测试代码写的是 "15"，
	// 这是一个已知的“陷阱”—— 它依赖于 Put("15") 在恢复后**重新提交并覆盖**。
	//
	// ⚠️ 注意：正确实现应保证 **一旦 Put("16") 被客户端确认，就不能被回滚**。
	// 因此，如果你的实现最终值是 "16"，但测试期望 "15"，可能会失败。
	// 然而，在 MIT 6.824 官方 Lab 3 中，此测试的最终 check 确实是 "15"，
	// 其隐含前提是：Put("16") **并未真正提交到状态机**（可能因 leader 变更未 commit），
	// 或 Put("15") 在恢复后以更高 term 提交并成为最终值。
	//
	// 实际上，更安全的理解是：**该测试要求你的系统在恢复后，
	// 能正确处理之前挂起的请求，并保证全局顺序一致**。
	// 如果你的实现通过了，说明一致性模型正确。
	check(cfg, t, ck, "1", "15")

	cfg.end()
}