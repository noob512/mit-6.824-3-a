package kvraft

// import "../porcupine"
// import "../models"
// import "testing"
// import "strconv"
// import "time"
// import "math/rand"
// import "log"
// import "strings"
// import "sync"
// import "sync/atomic"
// import "fmt"
// import "io/ioutil"

// // The tester generously allows solutions to complete elections in one second
// // (much more than the paper's range of timeouts).
// const electionTimeout = 1 * time.Second

// const linearizabilityCheckTimeout = 1 * time.Second

// // get/put/putappend that keep counts
// func Get(cfg *config, ck *Clerk, key string) string {
// 	v := ck.Get(key)
// 	cfg.op()
// 	return v
// }

// func Put(cfg *config, ck *Clerk, key string, value string) {
// 	ck.Put(key, value) // 通过客户端代理向分布式KV系统发送Put请求
// 	cfg.op()          // 记录一次操作，用于统计操作数量
// }

// func Append(cfg *config, ck *Clerk, key string, value string) {
// 	ck.Append(key, value)
// 	cfg.op()
// }

// func check(cfg *config, t *testing.T, ck *Clerk, key string, value string) {
// 	v := Get(cfg, ck, key)
// 	if v != value {
// 		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
// 	}
// }

// // a client runs the function f and then signals it is done
// func run_client(t *testing.T, cfg *config, me int, ca chan bool, fn func(me int, ck *Clerk, t *testing.T)) {
// 	ok := false
// 	defer func() { ca <- ok }()
// 	ck := cfg.makeClient(cfg.All())
// 	fn(me, ck, t)
// 	ok = true
// 	cfg.deleteClient(ck)
// }

// // spawn_clients_and_wait 启动ncli个客户端并等待它们全部完成
// // t: 测试对象，用于错误报告
// // cfg: 配置对象，包含服务器集群信息
// // ncli: 客户端数量
// // fn: 每个客户端执行的函数，参数为(客户端编号, 客户端对象, 测试对象)
// func spawn_clients_and_wait(t *testing.T, cfg *config, ncli int, fn func(me int, ck *Clerk, t *testing.T)) {
// 	ca := make([]chan bool, ncli)  // 创建结果通道数组，用于接收每个客户端的执行结果
	
// 	// 启动ncli个客户端goroutine
// 	for cli := 0; cli < ncli; cli++ {
// 		ca[cli] = make(chan bool)  // 为每个客户端创建结果通道
// 		go run_client(t, cfg, cli, ca[cli], fn)  // 并发运行客户端
// 	}
	
// 	// 等待所有客户端完成
// 	for cli := 0; cli < ncli; cli++ {
// 		ok := <-ca[cli]  // 接收客户端执行结果
// 		if ok == false { // 如果客户端执行失败
// 			t.Fatalf("failure")  // 测试失败
// 		}
// 	}
// }

// // predict effect of Append(k, val) if old value is prev.
// func NextValue(prev string, val string) string {
// 	return prev + val
// }

// // check that for a specific client all known appends are present in a value,
// // and in order
// // 验证客户端追加操作的结果是否正确
// // 参数：t - 测试对象，clnt - 客户端ID，v - 从服务器获取的完整字符串值，count - 追加操作的次数
// func checkClntAppends(t *testing.T, clnt int, v string, count int) {
// 	lastoff := -1 // 记录上一个找到的元素在字符串中的位置，用于验证顺序
	
// 	// 遍历期望的每个追加元素
// 	for j := 0; j < count; j++ {
// 		// 构造期望的追加值格式："x 客户端ID j y"
// 		wanted := "x " + strconv.Itoa(clnt) + " " + strconv.Itoa(j) + " y"
		
// 		// 查找当前期望值在结果字符串中的位置
// 		off := strings.Index(v, wanted)
// 		if off < 0 { // 如果没有找到期望的值
// 			t.Fatalf("%v missing element %v in Append result %v", clnt, wanted, v)
// 		}
		
// 		// 检查是否存在重复元素（通过查找最后一次出现的位置）
// 		off1 := strings.LastIndex(v, wanted)
// 		if off1 != off { // 如果第一次出现和最后一次出现位置不同，说明有重复
// 			t.Fatalf("duplicate element %v in Append result", wanted)
// 		}
		
// 		// 验证元素的顺序是否正确（当前元素的位置应该比上一个元素的位置大）
// 		if off <= lastoff { // 如果当前元素位置小于等于上一个元素位置，说明顺序错误
// 			t.Fatalf("wrong order for element %v in Append result", wanted)
// 		}
		
// 		lastoff = off // 更新上一个元素的位置
// 	}
// }

// // check that all known appends are present in a value,
// // and are in order for each concurrent client.
// func checkConcurrentAppends(t *testing.T, v string, counts []int) {
// 	nclients := len(counts)
// 	for i := 0; i < nclients; i++ {
// 		lastoff := -1
// 		for j := 0; j < counts[i]; j++ {
// 			wanted := "x " + strconv.Itoa(i) + " " + strconv.Itoa(j) + " y"
// 			off := strings.Index(v, wanted)
// 			if off < 0 {
// 				t.Fatalf("%v missing element %v in Append result %v", i, wanted, v)
// 			}
// 			off1 := strings.LastIndex(v, wanted)
// 			if off1 != off {
// 				t.Fatalf("duplicate element %v in Append result", wanted)
// 			}
// 			if off <= lastoff {
// 				t.Fatalf("wrong order for element %v in Append result", wanted)
// 			}
// 			lastoff = off
// 		}
// 	}
// }

// // repartition the servers periodically
// func partitioner(t *testing.T, cfg *config, ch chan bool, done *int32) {
// 	defer func() { ch <- true }()
// 	for atomic.LoadInt32(done) == 0 {
// 		a := make([]int, cfg.n)
// 		for i := 0; i < cfg.n; i++ {
// 			a[i] = (rand.Int() % 2)
// 		}
// 		pa := make([][]int, 2)
// 		for i := 0; i < 2; i++ {
// 			pa[i] = make([]int, 0)
// 			for j := 0; j < cfg.n; j++ {
// 				if a[j] == i {
// 					pa[i] = append(pa[i], j)
// 				}
// 			}
// 		}
// 		cfg.partition(pa[0], pa[1])
// 		time.Sleep(electionTimeout + time.Duration(rand.Int63()%200)*time.Millisecond)
// 	}
// }

// // 基本测试如下：一个或多个客户端在一段时间内向服务器组提交Append/Get操作。
// // 一段时间结束后，测试检查所有追加的值是否按特定键存在且有序。
// // 如果unreliable为true，RPC可能会失败。如果crash为true，则服务器在一段时间后崩溃并重启。
// // 如果partitions为true，则测试在网络重新分区的同时运行客户端和服务器。
// // 如果maxraftstate为正数，则Raft的状态大小（即日志大小）不应超过8*maxraftstate。
// // 如果maxraftstate为负数，则不应使用快照。
// func GenericTest(t *testing.T, part string, nclients int, unreliable bool, crash bool, partitions bool, maxraftstate int) {

// 	// 构建测试标题，根据不同的测试条件添加描述
// 	title := "Test: "
// 	if unreliable {
// 		// 网络会丢弃RPC请求和回复
// 		title = title + "unreliable net, "
// 	}
// 	if crash {
// 		// 节点重新启动，因此持久化必须有效
// 		title = title + "restarts, "
// 	}
// 	if partitions {
// 		// 网络可能分区
// 		title = title + "partitions, "
// 	}
// 	if maxraftstate != -1 {
// 		title = title + "snapshots, " // 如果maxraftstate不是-1，则启用快照
// 	}
// 	if nclients > 1 {
// 		title = title + "many clients" // 多客户端测试
// 	} else {
// 		title = title + "one client"   // 单客户端测试
// 	}
// 	title = title + " (" + part + ")" // 3A or 3B

// 	const nservers = 5 // 服务器数量固定为5
// 	cfg := make_config(t, nservers, unreliable, maxraftstate) // 创建测试配置
// 	DPrintf("make_config完成/n")
// 	defer cfg.cleanup() // 确保测试结束后清理资源

// 	cfg.begin(title) // 开始测试

// 	ck := cfg.makeClient(cfg.All()) // 创建客户端
// 	DPrintf("makeClient完成/n")
// 	// 控制分隔器和客户端的退出标志
// 	done_partitioner := int32(0) // 分隔器退出标志
// 	done_clients := int32(0)     // 客户端退出标志
// 	ch_partitioner := make(chan bool) // 分隔器完成通知通道
// 	clnts := make([]chan int, nclients) // 每个客户端的计数通道
// 	for i := 0; i < nclients; i++ {
// 		clnts[i] = make(chan int) // 为每个客户端创建计数通道
// 	}
	
// 	// 运行3轮测试
// 	for i := 0; i < 3; i++ {
// 		// log.Printf("Iteration %v\n", i)
// 		atomic.StoreInt32(&done_clients, 0)   // 重置客户端退出标志
// 		atomic.StoreInt32(&done_partitioner, 0) // 重置分隔器退出标志
		
// 		// 启动客户端goroutine
// 		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
// 			j := 0 // 操作计数器
// 			DPrintf("spawn_clients_and_wait完成/n")
// 			defer func() {
// 				clnts[cli] <- j // 将操作计数发送到对应通道
// 			}()
			
// 			last := "" // 记录最后的值
// 			key := strconv.Itoa(cli) // 使用客户端ID作为键
// 			Put(cfg, myck, key, last) // 初始化键值对
// 			DPrintf("Put完成/n")
			
// 			// 当客户端未被要求退出时继续操作
// 			for atomic.LoadInt32(&done_clients) == 0 {
// 				if (rand.Int() % 1000) < 500 { // 50%概率执行Append操作
// 					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y" // 生成新的值
// 					// log.Printf("%d: client new append %v\n", cli, nv)
// 					Append(cfg, myck, key, nv) // 执行追加操作
// 					last = NextValue(last, nv) // 更新期望的最后值
// 					j++ // 操作计数递增
// 					DPrintf("Append完成/n")
// 				} else { // 50%概率执行Get操作
// 					// log.Printf("%d: client new get %v\n", cli, key)
// 					v := Get(cfg, myck, key) // 获取当前值
// 					DPrintf("Get完成/n")
// 					if v != last { // 验证获取的值是否正确
// 						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
// 					}
// 				}
// 			}
// 		})

// 		// 如果启用网络分区，则启动分隔器
// 		if partitions {
// 			// 允许客户端在没有干扰的情况下执行一些操作
// 			time.Sleep(1 * time.Second)
// 			go partitioner(t, cfg, ch_partitioner, &done_partitioner) // 启动网络分隔器
// 		}
		
// 		// 让测试运行5秒
// 		time.Sleep(5 * time.Second)

// 		// 设置退出标志，通知客户端和分隔器退出
// 		atomic.StoreInt32(&done_clients, 1)     // 通知客户端退出
// 		atomic.StoreInt32(&done_partitioner, 1) // 通知分隔器退出

// 		// 如果启用了网络分区，处理分隔器的清理
// 		if partitions {
// 			// log.Printf("wait for partitioner\n")
// 			<-ch_partitioner // 等待分隔器完成
// 			// 重新连接网络并提交请求。客户端可能在少数派中提交了请求，
// 			// 该请求在服务器发现新任期开始之前不会返回。
// 			cfg.ConnectAll() // 重新连接所有网络
// 			// 等待一段时间以确保出现新任期
// 			time.Sleep(electionTimeout)
// 		}

// 		// 如果启用了崩溃测试，执行服务器崩溃和重启
// 		if crash {
// 			// log.Printf("shutdown servers\n")
// 			for i := 0; i < nservers; i++ {
// 				cfg.ShutdownServer(i) // 关闭每个服务器
// 			}
// 			// 等待一段时间让服务器关闭，因为关闭不是真正的崩溃，也不是即时的
// 			time.Sleep(electionTimeout)
// 			// log.Printf("restart servers\n")
// 			// 崩溃并重新启动所有服务器
// 			for i := 0; i < nservers; i++ {
// 				cfg.StartServer(i) // 重新启动每个服务器
// 			}
// 			cfg.ConnectAll() // 重新连接所有网络
// 		}

// 		// 等待所有客户端完成并验证结果
// 		// log.Printf("wait for clients\n")
// 		for i := 0; i < nclients; i++ {
// 			// log.Printf("read from clients %d\n", i)
// 			j := <-clnts[i] // 从通道获取客户端操作计数
// 			// if j < 10 {
// 			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
// 			// }
// 			key := strconv.Itoa(i) // 构建键
// 			// log.Printf("Check %v for client %d\n", j, i)
// 			v := Get(cfg, ck, key) // 获取最终值
// 			checkClntAppends(t, i, v, j) // 验证客户端追加操作的结果
// 		}

// 		// 检查日志大小限制（如果启用了快照）
// 		if maxraftstate > 0 {
// 			// 在服务器处理完所有客户端请求并有时间进行检查点后检查最大大小
// 			sz := cfg.LogSize() // 获取日志大小
// 			if sz > 8*maxraftstate { // 检查是否超过限制
// 				t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
// 			}
// 		}
		
// 		// 检查快照是否未被使用（如果maxraftstate为负数）
// 		if maxraftstate < 0 {
// 			// 检查快照是否未被使用
// 			ssz := cfg.SnapshotSize() // 获取快照大小
// 			if ssz > 0 { // 如果快照被使用了（大小大于0），则失败
// 				t.Fatalf("snapshot too large (%v), should not be used when maxraftstate = %d", ssz, maxraftstate)
// 			}
// 		}
// 	}

// 	cfg.end() // 结束测试
// }

// // similar to GenericTest, but with clients doing random operations (and using a
// // linearizability checker)
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

// func TestUnreliableOneKey3A(t *testing.T) {
// 	const nservers = 3
// 	cfg := make_config(t, nservers, true, -1)
// 	defer cfg.cleanup()

// 	ck := cfg.makeClient(cfg.All())

// 	cfg.begin("Test: concurrent append to same key, unreliable (3A)")

// 	Put(cfg, ck, "k", "")

// 	const nclient = 5
// 	const upto = 10
// 	spawn_clients_and_wait(t, cfg, nclient, func(me int, myck *Clerk, t *testing.T) {
// 		n := 0
// 		for n < upto {
// 			Append(cfg, myck, "k", "x "+strconv.Itoa(me)+" "+strconv.Itoa(n)+" y")
// 			n++
// 		}
// 	})

// 	var counts []int
// 	for i := 0; i < nclient; i++ {
// 		counts = append(counts, upto)
// 	}

// 	vx := Get(cfg, ck, "k")
// 	checkConcurrentAppends(t, vx, counts)

// 	cfg.end()
// }

// // Submit a request in the minority partition and check that the requests
// // doesn't go through until the partition heals.  The leader in the original
// // network ends up in the minority partition.
// func TestOnePartition3A(t *testing.T) {
// 	const nservers = 5
// 	cfg := make_config(t, nservers, false, -1)
// 	defer cfg.cleanup()
// 	ck := cfg.makeClient(cfg.All())

// 	Put(cfg, ck, "1", "13")

// 	cfg.begin("Test: progress in majority (3A)")

// 	p1, p2 := cfg.make_partition()
// 	cfg.partition(p1, p2)

// 	ckp1 := cfg.makeClient(p1)  // connect ckp1 to p1
// 	ckp2a := cfg.makeClient(p2) // connect ckp2a to p2
// 	ckp2b := cfg.makeClient(p2) // connect ckp2b to p2

// 	Put(cfg, ckp1, "1", "14")
// 	check(cfg, t, ckp1, "1", "14")

// 	cfg.end()

// 	done0 := make(chan bool)
// 	done1 := make(chan bool)

// 	cfg.begin("Test: no progress in minority (3A)")
// 	go func() {
// 		Put(cfg, ckp2a, "1", "15")
// 		done0 <- true
// 	}()
// 	go func() {
// 		Get(cfg, ckp2b, "1") // different clerk in p2
// 		done1 <- true
// 	}()

// 	select {
// 	case <-done0:
// 		t.Fatalf("Put in minority completed")
// 	case <-done1:
// 		t.Fatalf("Get in minority completed")
// 	case <-time.After(time.Second):
// 	}

// 	check(cfg, t, ckp1, "1", "14")
// 	Put(cfg, ckp1, "1", "16")
// 	check(cfg, t, ckp1, "1", "16")

// 	cfg.end()

// 	cfg.begin("Test: completion after heal (3A)")

// 	cfg.ConnectAll()
// 	cfg.ConnectClient(ckp2a, cfg.All())
// 	cfg.ConnectClient(ckp2b, cfg.All())

// 	time.Sleep(electionTimeout)

// 	select {
// 	case <-done0:
// 	case <-time.After(30 * 100 * time.Millisecond):
// 		t.Fatalf("Put did not complete")
// 	}

// 	select {
// 	case <-done1:
// 	case <-time.After(30 * 100 * time.Millisecond):
// 		t.Fatalf("Get did not complete")
// 	default:
// 	}

// 	check(cfg, t, ck, "1", "15")

// 	cfg.end()
// }

// func TestManyPartitionsOneClient3A(t *testing.T) {
// 	// Test: partitions, one client (3A) ...
// 	GenericTest(t, "3A", 1, false, false, true, -1)
// }

// func TestManyPartitionsManyClients3A(t *testing.T) {
// 	// Test: partitions, many clients (3A) ...
// 	GenericTest(t, "3A", 5, false, false, true, -1)
// }

// func TestPersistOneClient3A(t *testing.T) {
// 	// Test: restarts, one client (3A) ...
// 	GenericTest(t, "3A", 1, false, true, false, -1)
// }

// func TestPersistConcurrent3A(t *testing.T) {
// 	// Test: restarts, many clients (3A) ...
// 	GenericTest(t, "3A", 5, false, true, false, -1)
// }

// func TestPersistConcurrentUnreliable3A(t *testing.T) {
// 	// Test: unreliable net, restarts, many clients (3A) ...
// 	GenericTest(t, "3A", 5, true, true, false, -1)
// }

// func TestPersistPartition3A(t *testing.T) {
// 	// Test: restarts, partitions, many clients (3A) ...
// 	GenericTest(t, "3A", 5, false, true, true, -1)
// }

// func TestPersistPartitionUnreliable3A(t *testing.T) {
// 	// Test: unreliable net, restarts, partitions, many clients (3A) ...
// 	GenericTest(t, "3A", 5, true, true, true, -1)
// }

// func TestPersistPartitionUnreliableLinearizable3A(t *testing.T) {
// 	// Test: unreliable net, restarts, partitions, linearizability checks (3A) ...
// 	GenericTestLinearizability(t, "3A", 15, 7, true, true, true, -1)
// }

// //
// // if one server falls behind, then rejoins, does it
// // recover by using the InstallSnapshot RPC?
// // also checks that majority discards committed log entries
// // even if minority doesn't respond.
// //
// func TestSnapshotRPC3B(t *testing.T) {
// 	const nservers = 3
// 	maxraftstate := 1000
// 	cfg := make_config(t, nservers, false, maxraftstate)
// 	defer cfg.cleanup()

// 	ck := cfg.makeClient(cfg.All())

// 	cfg.begin("Test: InstallSnapshot RPC (3B)")

// 	Put(cfg, ck, "a", "A")
// 	check(cfg, t, ck, "a", "A")

// 	// a bunch of puts into the majority partition.
// 	cfg.partition([]int{0, 1}, []int{2})
// 	{
// 		ck1 := cfg.makeClient([]int{0, 1})
// 		for i := 0; i < 50; i++ {
// 			Put(cfg, ck1, strconv.Itoa(i), strconv.Itoa(i))
// 		}
// 		time.Sleep(electionTimeout)
// 		Put(cfg, ck1, "b", "B")
// 	}

// 	// check that the majority partition has thrown away
// 	// most of its log entries.
// 	sz := cfg.LogSize()
// 	if sz > 8*maxraftstate {
// 		t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
// 	}

// 	// now make group that requires participation of
// 	// lagging server, so that it has to catch up.
// 	cfg.partition([]int{0, 2}, []int{1})
// 	{
// 		ck1 := cfg.makeClient([]int{0, 2})
// 		Put(cfg, ck1, "c", "C")
// 		Put(cfg, ck1, "d", "D")
// 		check(cfg, t, ck1, "a", "A")
// 		check(cfg, t, ck1, "b", "B")
// 		check(cfg, t, ck1, "1", "1")
// 		check(cfg, t, ck1, "49", "49")
// 	}

// 	// now everybody
// 	cfg.partition([]int{0, 1, 2}, []int{})

// 	Put(cfg, ck, "e", "E")
// 	check(cfg, t, ck, "c", "C")
// 	check(cfg, t, ck, "e", "E")
// 	check(cfg, t, ck, "1", "1")

// 	cfg.end()
// }

// // are the snapshots not too huge? 500 bytes is a generous bound for the
// // operations we're doing here.
// func TestSnapshotSize3B(t *testing.T) {
// 	const nservers = 3
// 	maxraftstate := 1000
// 	maxsnapshotstate := 500
// 	cfg := make_config(t, nservers, false, maxraftstate)
// 	defer cfg.cleanup()

// 	ck := cfg.makeClient(cfg.All())

// 	cfg.begin("Test: snapshot size is reasonable (3B)")

// 	for i := 0; i < 200; i++ {
// 		Put(cfg, ck, "x", "0")
// 		check(cfg, t, ck, "x", "0")
// 		Put(cfg, ck, "x", "1")
// 		check(cfg, t, ck, "x", "1")
// 	}

// 	// check that servers have thrown away most of their log entries
// 	sz := cfg.LogSize()
// 	if sz > 8*maxraftstate {
// 		t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
// 	}

// 	// check that the snapshots are not unreasonably large
// 	ssz := cfg.SnapshotSize()
// 	if ssz > maxsnapshotstate {
// 		t.Fatalf("snapshot too large (%v > %v)", ssz, maxsnapshotstate)
// 	}

// 	cfg.end()
// }

// func TestSnapshotRecover3B(t *testing.T) {
// 	// Test: restarts, snapshots, one client (3B) ...
// 	GenericTest(t, "3B", 1, false, true, false, 1000)
// }

// func TestSnapshotRecoverManyClients3B(t *testing.T) {
// 	// Test: restarts, snapshots, many clients (3B) ...
// 	GenericTest(t, "3B", 20, false, true, false, 1000)
// }

// func TestSnapshotUnreliable3B(t *testing.T) {
// 	// Test: unreliable net, snapshots, many clients (3B) ...
// 	GenericTest(t, "3B", 5, true, false, false, 1000)
// }

// func TestSnapshotUnreliableRecover3B(t *testing.T) {
// 	// Test: unreliable net, restarts, snapshots, many clients (3B) ...
// 	GenericTest(t, "3B", 5, true, true, false, 1000)
// }

// func TestSnapshotUnreliableRecoverConcurrentPartition3B(t *testing.T) {
// 	// Test: unreliable net, restarts, partitions, snapshots, many clients (3B) ...
// 	GenericTest(t, "3B", 5, true, true, true, 1000)
// }

// func TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B(t *testing.T) {
// 	// Test: unreliable net, restarts, partitions, snapshots, linearizability checks (3B) ...
// 	GenericTestLinearizability(t, "3B", 15, 7, true, true, true, 1000)
// }
