package shardkv

import "../porcupine"
import "../models"
import "testing"
import "strconv"
import "time"
import "fmt"
import "sync/atomic"
import "sync"
import "math/rand"
import "io/ioutil"

const linearizabilityCheckTimeout = 1 * time.Second

func check(t *testing.T, ck *Clerk, key string, value string) {
	v := ck.Get(key)
	if v != value {
		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
	}
}

//
// test static 2-way sharding, without shard movement.
//
func TestStaticShards(t *testing.T) {
    fmt.Printf("Test: static shards ...\n")

    // 1. 初始化测试框架
    // 创建一个包含 3 台服务器的集群，false 表示网络不模拟丢包/延迟，-1 表示不使用快照
    cfg := make_config(t, 3, false, -1)
    defer cfg.cleanup() // 测试结束后自动清理资源

    // 创建一个客户端 Clerk，用于向系统发送 Get/Put 请求
    ck := cfg.makeClient()

    // 2. 初始化分片配置
    // 通知 ShardMaster 加入两个副本组：GID 0 和 GID 1
    // 此时 ShardMaster 会触发 rebalance，将 10 个分片平分给这两个组（通常是各 5 个分片）
    cfg.join(0)
    cfg.join(1)

    // 3. 全量写入与读取测试
    n := 10
    ka := make([]string, n)
    va := make([]string, n)
    // 循环 10 次，构造 10 个键值对
    for i := 0; i < n; i++ {
        ka[i] = strconv.Itoa(i) // 键名为 "0", "1", ..., "9"，确保它们会被哈希到不同的分片中
        va[i] = randstring(20)  // 随机生成 20 字节的值
        // 客户端自动计算 key 所属的 shard，去 ShardMaster 查对应的 GID，然后发 RPC 写入
        ck.Put(ka[i], va[i])
    }
    // 立刻进行读取验证，确保刚才写的 10 个数据都能正确读出来
    for i := 0; i < n; i++ {
        check(t, ck, ka[i], va[i])
    }
    // 4. 验证“真·分片”逻辑（核心高光时刻）
    // 怎么证明你的数据真的被分到了两个组？把其中一个组“拔网线”试试！
    cfg.ShutdownGroup(1) // 强行关闭 GID 1 组内的所有服务器
    cfg.checklogs()      // 内部测试断言，禁止此阶段产生快照
    ch := make(chan bool)
    // 并发发起 10 个 Get 请求，查询所有的 10 个 Key
    for xi := 0; xi < n; xi++ {
        ck1 := cfg.makeClient() // 每个并发请求使用一个独立的客户端对象
        go func(i int) {
            defer func() { ch <- true }() // 函数退出时，往管道塞入一个完成信号
            // 如果这个 key 属于活着的 GID 0，check 会瞬间成功
            // 如果这个 key 属于宕机的 GID 1，check 会无限重试/阻塞
            check(t, ck1, ka[i], va[i])
        }(xi)
    }

    // 5. 结果统计与断言
    ndone := 0
    done := false
    // 主协程等待 2 秒钟
    for done == false {
        select {
        case <-ch:
            ndone += 1 // 统计在这 2 秒内，有几个 Get 请求成功返回了
        case <-time.After(time.Second * 2):
            done = true // 2 秒时间到，强行终止等待
            break
        }
    }

    // 断言：由于 10 个分片被均分给了 2 个组，其中 1 个组死了。
    // 那么理应只有一半（5个）请求能成功，另一半必然卡死超时。
    // 如果 ndone 不是 5（比如是 10），说明你把所有数据都悄悄存在了一个组里，根本没有实现真正的 Sharding 分布式存储！
    if ndone != 5 {
        t.Fatalf("expected 5 completions with one shard dead; got %v\n", ndone)
    }

    // 6. 恢复与高可用测试
    // 把刚才宕机的 GID 1 重新启动（模拟停电后恢复正常）
    cfg.StartGroup(1) 
    
    // 再次循环检查 10 个 Key。
    // GID 1 恢复后，底层 Raft 会重新选主，刚才卡住的那 5 个请求会被新 Leader 处理。
    // 最终 10 个数据必须完好无损地全部读出。
    for i := 0; i < n; i++ {
        check(t, ck, ka[i], va[i])
    }

    fmt.Printf("  ... Passed\n")
}

func TestJoinLeave(t *testing.T) {
	fmt.Printf("Test: join then leave ...\n")

	// 1. 初始化测试集群
	// 创建一个包含 3 个 Replica Group (副本组) 的测试环境，但不开启崩溃恢复(false)，不限制 Raft 日志大小(-1)。
	// 注意：此时虽然物理机器启动了，但 ShardMaster 里的配置是空的，没有任何组接管分片。
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	// 创建一个与集群通信的客户端 (Clerk)
	ck := cfg.makeClient()

	// ==========================================
	// 阶段一：单节点创世 (Join 0)
	// ==========================================
	
	// 让第 0 个副本组 (通常 GID=100) 加入 ShardMaster。
	// 此时 ShardMaster 会生成 Config 1，将所有 10 个分片全部全部分配给这个组。
	// 期望反应：Group 0 的 ConfigTicker 拉到新配置，将 10 个分片状态变为 Serving（因为是创世配置，无需拉取数据）。
	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	
	// 写入 10 个完全不同的 Key，依靠 key2shard 哈希算法，确保这 10 个 Key 会散落到不同的分片中。
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // 保证跨分片写入
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])    // 此时所有请求都会被路由到 Group 0
	}
	// 立刻读取，验证写入是否成功
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}
	DPrintf("首次check成功\n")

	// ==========================================
	// 阶段二：扩容与分片迁移 (Join 1)
	// ==========================================
	
	// 让第 1 个副本组 (通常 GID=101) 加入集群。
	// ShardMaster 触发 rebalance，生成 Config 2。此时 10 个分片会被平分，比如 Group 0 留 5 个，Group 1 拿 5 个。
	// 期望反应：
	// 1. Group 1 发现自己获得了 5 个新分片，状态变为 Pulling，并主动向 Group 0 发起 FetchShard RPC。
	// 2. Group 0 发现自己失去了 5 个分片，状态变为 Pushing，拒绝客户端写入，并等待 Group 1 来拉数据。
	cfg.join(1)

	// 验证扩容后的数据完整性与连贯性
	for i := 0; i < n; i++ {
		// 先读：无论这个 Key 留在了 Group 0 还是迁移到了 Group 1，都必须能读到之前写入的数据（不能丢数据）。
		check(t, ck, ka[i], va[i])
		
		x := randstring(5)
		// 再写：确保 Group 1 拿到数据后，确实把状态改成了 Serving，能够正常处理新的 Append 请求。
		ck.Append(ka[i], x)
		va[i] += x
	}
	DPrintf("阶段三开始\n")
	// ==========================================
	// 阶段三：缩容与分片全量合并 (Leave 0)
	// ==========================================
	
	// 让 Group 0 离开集群。
	// ShardMaster 生成 Config 3，将 Group 0 剩余的 5 个分片全部划给 Group 1。此时 Group 1 拥有全部 10 个分片。
	// 期望反应：Group 1 再次触发 Pulling，去向 Group 0 把最后剩下的数据全部拉过来。
	cfg.leave(0)

	// 验证缩容后的数据一致性
	for i := 0; i < n; i++ {
		// 所有的读写请求此时都会打向 Group 1。
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		ck.Append(ka[i], x)
		va[i] += x
	}

	// ==========================================
	// 阶段四：物理隔离与终极校验 (非常狠的一招)
	// ==========================================
	DPrintf("阶段四开始\n")
	// 稍微等一会儿，让底层的 RPC 交互、Raft 共识以及 Challenge 1 的垃圾回收(DeleteShard)彻底跑完。
	time.Sleep(1 * time.Second)

	cfg.checklogs()
	
	// 🚨 杀手锏：直接在物理层面把 Group 0 的所有服务器全部宕机/断网！
	cfg.ShutdownGroup(0)
	DPrintf("关机开始\n")
	// 终极读取校验：
	// 如果 Group 1 在之前的阶段中没有真正把数据“拉(Pull)”到自己的内存里，而是搞了什么“代理读取”，
	// 或者分片迁移逻辑卡死了，那么这里的 check 就会因为连不上 Group 0 而永远阻塞 (Timeout) 或报错。
	// 只要这里能顺利读出所有数据，就证明 Group 1 已经完美继承了整个集群的衣钵。
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestSnapshot(t *testing.T) {
	fmt.Printf("Test: snapshots, join, and leave ...\n")

	// 1. 初始化测试集群
	// ⚠️ 极其关键的参数：第四个参数 1000 就是 maxraftstate (Raft 日志的最大字节数)。
	// 在之前的测试中，这个值是 -1（不限制日志大小）。
	// 现在限制为 1000 字节，意味着系统在跑下面这些密集的 Put/Append 时，
	// 日志很快就会被打满，强制触发你的 ShardKV 拍摄快照并截断 Raft 日志！
	cfg := make_config(t, 3, false, 1000)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	// ==========================================
	// 阶段一：单节点创世与海量数据灌入
	// ==========================================
	cfg.join(0) // G0 加入，接管所有 10 个分片

	n := 30
	ka := make([]string, n)
	va := make([]string, n)
	// 连续写入 30 个体积相当大的 KV 键值对（每个 value 长达 20 字符）。
	// 这么大的写入量，足以在底层触发至少一次 Raft Snapshot。
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) 
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}
	// 校验数据是否写入成功
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}
	DPrintf("首次check\n")
	// ==========================================
	// 阶段二：配置剧变中的持续快照
	// ==========================================
	// G1 加入，G2 加入，G0 离开。
	// 这会产生大量的 UpdateConfig 日志，以及分片在 G0 -> G1/G2 之间的疯狂迁移（拉取数据、发送 ACK）。
	cfg.join(1)
	cfg.join(2)
	cfg.leave(0)

	// 在配置剧变的“阵痛期”，继续施加海量的 Append 请求压力！
	// 此时底层 Raft 日志一边在记录分片迁移日志，一边在记录 Append 日志，
	// 绝对会频繁越过 1000 字节的红线，疯狂触发快照。
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
		DPrintf("不断append\n")
	}
	DPrintf("二次check\n")

	// ==========================================
	// 阶段三：再次洗牌
	// ==========================================
	// G1 离开，老将 G0 重新回归。分片再次在 G1/G2 -> G0 之间疯狂交接。
	cfg.leave(1)
	cfg.join(0)

	// 继续高强度 Append 写入，把日志再次打满。
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
		DPrintf("3-不断append\n")
	}

	time.Sleep(1 * time.Second)

	// 再次校验数据
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	time.Sleep(1 * time.Second)

	// 检查底层 Raft 的日志体积是否真的被控制在了 1000 字节左右（验证你是否真的做了快照）
	cfg.checklogs()

	// ==========================================
	// 阶段四：终极断电重启测试 (The Ultimate Crash Test)
	// ==========================================
	// 🚨 毫不留情地把 G0, G1, G2 的所有服务器直接拔电源！
	// 此时，所有节点内存里的 map[string]string、kv.config、kv.lastConfig 全部灰飞烟灭。
	cfg.ShutdownGroup(0)
	cfg.ShutdownGroup(1)
	cfg.ShutdownGroup(2)

	// 重新启动整个集群！
	// 此时，各个节点只能依靠底层的持久化状态（Raft State 和 Snapshot）来恢复记忆。
	cfg.StartGroup(0)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	// 终极审判：如果你的节点在崩溃前生成的快照漏存了任何关键变量，
	// 它唤醒后就会变成“失忆症患者”，路由错乱或者找不到数据，这里的 check 将会直接报错或死锁！
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestMissChange(t *testing.T) {
	fmt.Printf("Test: servers miss configuration changes...\n")

	cfg := make_config(t, 3, false, 1000)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	cfg.join(1)

	cfg.ShutdownServer(0, 0)
	cfg.ShutdownServer(1, 0)
	cfg.ShutdownServer(2, 0)

	cfg.join(2)
	cfg.leave(1)
	cfg.leave(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.join(1)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.StartServer(0, 0)
	cfg.StartServer(1, 0)
	cfg.StartServer(2, 0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	time.Sleep(2 * time.Second)

	cfg.ShutdownServer(0, 1)
	cfg.ShutdownServer(1, 1)
	cfg.ShutdownServer(2, 1)

	cfg.join(0)
	cfg.leave(2)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.StartServer(0, 1)
	cfg.StartServer(1, 1)
	cfg.StartServer(2, 1)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestConcurrent1(t *testing.T) {
	fmt.Printf("Test: concurrent puts and configuration changes...\n")

	cfg := make_config(t, 3, false, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(5)
			ck1.Append(ka[i], x)
			va[i] += x
			time.Sleep(10 * time.Millisecond)
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)

	cfg.ShutdownGroup(0)
	time.Sleep(100 * time.Millisecond)
	cfg.ShutdownGroup(1)
	time.Sleep(100 * time.Millisecond)
	cfg.ShutdownGroup(2)

	cfg.leave(2)

	time.Sleep(100 * time.Millisecond)
	cfg.StartGroup(0)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	time.Sleep(100 * time.Millisecond)
	cfg.join(0)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)

	time.Sleep(1 * time.Second)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

//
// this tests the various sources from which a re-starting
// group might need to fetch shard contents.
//
func TestConcurrent2(t *testing.T) {
	fmt.Printf("Test: more concurrent puts and configuration changes...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(1)
	cfg.join(0)
	cfg.join(2)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(1)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int, ck1 *Clerk) {
		defer func() { ch <- true }()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(1)
			ck1.Append(ka[i], x)
			va[i] += x
			time.Sleep(50 * time.Millisecond)
		}
	}

	for i := 0; i < n; i++ {
		ck1 := cfg.makeClient()
		go ff(i, ck1)
	}

	cfg.leave(0)
	cfg.leave(2)
	time.Sleep(3000 * time.Millisecond)
	cfg.join(0)
	cfg.join(2)
	cfg.leave(1)
	time.Sleep(3000 * time.Millisecond)
	cfg.join(1)
	cfg.leave(0)
	cfg.leave(2)
	time.Sleep(3000 * time.Millisecond)

	cfg.ShutdownGroup(1)
	cfg.ShutdownGroup(2)
	time.Sleep(1000 * time.Millisecond)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable1(t *testing.T) {
	fmt.Printf("Test: unreliable 1...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	cfg.join(1)
	cfg.join(2)
	cfg.leave(0)

	for ii := 0; ii < n*2; ii++ {
		i := ii % n
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.join(0)
	cfg.leave(1)

	for ii := 0; ii < n*2; ii++ {
		i := ii % n
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable2(t *testing.T) {
	fmt.Printf("Test: unreliable 2...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(5)
			ck1.Append(ka[i], x)
			va[i] += x
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)
	cfg.join(0)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	cfg.net.Reliable(true)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable3(t *testing.T) {
	fmt.Printf("Test: unreliable 3...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	begin := time.Now()
	var operations []porcupine.Operation
	var opMu sync.Mutex

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		start := int64(time.Since(begin))
		ck.Put(ka[i], va[i])
		end := int64(time.Since(begin))
		inp := models.KvInput{Op: 1, Key: ka[i], Value: va[i]}
		var out models.KvOutput
		op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: 0}
		operations = append(operations, op)
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		for atomic.LoadInt32(&done) == 0 {
			ki := rand.Int() % n
			nv := randstring(5)
			var inp models.KvInput
			var out models.KvOutput
			start := int64(time.Since(begin))
			if (rand.Int() % 1000) < 500 {
				ck1.Append(ka[ki], nv)
				inp = models.KvInput{Op: 2, Key: ka[ki], Value: nv}
			} else if (rand.Int() % 1000) < 100 {
				ck1.Put(ka[ki], nv)
				inp = models.KvInput{Op: 1, Key: ka[ki], Value: nv}
			} else {
				v := ck1.Get(ka[ki])
				inp = models.KvInput{Op: 0, Key: ka[ki]}
				out = models.KvOutput{Value: v}
			}
			end := int64(time.Since(begin))
			op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: i}
			opMu.Lock()
			operations = append(operations, op)
			opMu.Unlock()
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)
	cfg.join(0)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	cfg.net.Reliable(true)
	for i := 0; i < n; i++ {
		<-ch
	}

	res, info := porcupine.CheckOperationsVerbose(models.KvModel, operations, linearizabilityCheckTimeout)
	if res == porcupine.Illegal {
		file, err := ioutil.TempFile("", "*.html")
		if err != nil {
			fmt.Printf("info: failed to create temp file for visualization")
		} else {
			err = porcupine.Visualize(models.KvModel, info, file)
			if err != nil {
				fmt.Printf("info: failed to write history visualization to %s\n", file.Name())
			} else {
				fmt.Printf("info: wrote history visualization to %s\n", file.Name())
			}
		}
		t.Fatal("history is not linearizable")
	} else if res == porcupine.Unknown {
		fmt.Println("info: linearizability check timed out, assuming history is ok")
	}

	fmt.Printf("  ... Passed\n")
}

//
// optional test to see whether servers are deleting
// shards for which they are no longer responsible.
//
func TestChallenge1Delete(t *testing.T) {
	fmt.Printf("Test: shard deletion (challenge 1) ...\n")

	// "1" means force snapshot after every log entry.
	cfg := make_config(t, 3, false, 1)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	// 30,000 bytes of total values.
	n := 30
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i)
		va[i] = randstring(1000)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}

	for iters := 0; iters < 2; iters++ {
		cfg.join(1)
		cfg.leave(0)
		cfg.join(2)
		time.Sleep(3 * time.Second)
		for i := 0; i < 3; i++ {
			check(t, ck, ka[i], va[i])
		}
		cfg.leave(1)
		cfg.join(0)
		cfg.leave(2)
		time.Sleep(3 * time.Second)
		for i := 0; i < 3; i++ {
			check(t, ck, ka[i], va[i])
		}
	}

	cfg.join(1)
	cfg.join(2)
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}

	total := 0
	for gi := 0; gi < cfg.ngroups; gi++ {
		for i := 0; i < cfg.n; i++ {
			raft := cfg.groups[gi].saved[i].RaftStateSize()
			snap := len(cfg.groups[gi].saved[i].ReadSnapshot())
			total += raft + snap
		}
	}

	// 27 keys should be stored once.
	// 3 keys should also be stored in client dup tables.
	// everything on 3 replicas.
	// plus slop.
	expected := 3 * (((n - 3) * 1000) + 2*3*1000 + 6000)
	if total > expected {
		t.Fatalf("snapshot + persisted Raft state are too big: %v > %v\n", total, expected)
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestChallenge1Concurrent(t *testing.T) {
	fmt.Printf("Test: concurrent configuration change and restart (challenge 1)...\n")

	cfg := make_config(t, 3, false, 300)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i)
		va[i] = randstring(1)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int, ck1 *Clerk) {
		defer func() { ch <- true }()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(1)
			ck1.Append(ka[i], x)
			va[i] += x
		}
	}

	for i := 0; i < n; i++ {
		ck1 := cfg.makeClient()
		go ff(i, ck1)
	}

	t0 := time.Now()
	for time.Since(t0) < 12*time.Second {
		cfg.join(2)
		cfg.join(1)
		time.Sleep(time.Duration(rand.Int()%900) * time.Millisecond)
		cfg.ShutdownGroup(0)
		cfg.ShutdownGroup(1)
		cfg.ShutdownGroup(2)
		cfg.StartGroup(0)
		cfg.StartGroup(1)
		cfg.StartGroup(2)

		time.Sleep(time.Duration(rand.Int()%900) * time.Millisecond)
		cfg.leave(1)
		cfg.leave(2)
		time.Sleep(time.Duration(rand.Int()%900) * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

//
// optional test to see whether servers can handle
// shards that are not affected by a config change
// while the config change is underway
//
func TestChallenge2Unaffected(t *testing.T) {
	fmt.Printf("Test: unaffected shard access (challenge 2) ...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	// JOIN 100
	cfg.join(0)

	// Do a bunch of puts to keys in all shards
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = "100"
		ck.Put(ka[i], va[i])
	}

	// JOIN 101
	cfg.join(1)

	// QUERY to find shards now owned by 101
	c := cfg.mck.Query(-1)
	owned := make(map[int]bool, n)
	for s, gid := range c.Shards {
		owned[s] = gid == cfg.groups[1].gid
	}

	// Wait for migration to new config to complete, and for clients to
	// start using this updated config. Gets to any key k such that
	// owned[shard(k)] == true should now be served by group 101.
	<-time.After(1 * time.Second)
	for i := 0; i < n; i++ {
		if owned[i] {
			va[i] = "101"
			ck.Put(ka[i], va[i])
		}
	}

	// KILL 100
	cfg.ShutdownGroup(0)

	// LEAVE 100
	// 101 doesn't get a chance to migrate things previously owned by 100
	cfg.leave(0)

	// Wait to make sure clients see new config
	<-time.After(1 * time.Second)

	// And finally: check that gets/puts for 101-owned keys still complete
	for i := 0; i < n; i++ {
		shard := int(ka[i][0]) % 10
		if owned[shard] {
			check(t, ck, ka[i], va[i])
			ck.Put(ka[i], va[i]+"-1")
			check(t, ck, ka[i], va[i]+"-1")
		}
	}

	fmt.Printf("  ... Passed\n")
}

//
// optional test to see whether servers can handle operations on shards that
// have been received as a part of a config migration when the entire migration
// has not yet completed.
//
func TestChallenge2Partial(t *testing.T) {
	fmt.Printf("Test: partial migration shard access (challenge 2) ...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	// JOIN 100 + 101 + 102
	cfg.joinm([]int{0, 1, 2})

	// Give the implementation some time to reconfigure
	<-time.After(1 * time.Second)

	// Do a bunch of puts to keys in all shards
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = "100"
		ck.Put(ka[i], va[i])
	}

	// QUERY to find shards owned by 102
	c := cfg.mck.Query(-1)
	owned := make(map[int]bool, n)
	for s, gid := range c.Shards {
		owned[s] = gid == cfg.groups[2].gid
	}

	// KILL 100
	cfg.ShutdownGroup(0)

	// LEAVE 100 + 102
	// 101 can get old shards from 102, but not from 100. 101 should start
	// serving shards that used to belong to 102 as soon as possible
	cfg.leavem([]int{0, 2})

	// Give the implementation some time to start reconfiguration
	// And to migrate 102 -> 101
	<-time.After(1 * time.Second)

	// And finally: check that gets/puts for 101-owned keys now complete
	for i := 0; i < n; i++ {
		shard := key2shard(ka[i])
		if owned[shard] {
			check(t, ck, ka[i], va[i])
			ck.Put(ka[i], va[i]+"-2")
			check(t, ck, ka[i], va[i]+"-2")
		}
	}

	fmt.Printf("  ... Passed\n")
}
