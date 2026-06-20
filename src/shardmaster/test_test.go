package shardmaster

import (
	"sync"
	"testing"
)

// import "time"
import "fmt"

// check 函数用于验证分片主控（Shardmaster）当前的配置状态是否符合预期
// 参数 t: 测试上下文，用于在测试失败时抛出错误
// 参数 groups: 期望当前集群中存在的副本组 GID 列表
// 参数 ck: 客户端（Clerk），用于向 Shardmaster 发送 RPC 请求
func check(t *testing.T, groups []int, ck *Clerk) {
    // 1. 获取最新配置：传入 -1 调用 Query RPC，期望返回系统最新处理完成的 Config
    c := ck.Query(-1)
    
    // 2. 验证组数量：检查最新配置中的副本组数量，是否与期望的 groups 列表长度一致
    if len(c.Groups) != len(groups) {
        t.Fatalf("wanted %v groups, got %v", len(groups), len(c.Groups))
    }

    // 3. 验证组的完整性：遍历期望的 groups 列表，确保每一个预期的 GID 都真实存在于配置的 Groups map 中
    for _, g := range groups {
        _, ok := c.Groups[g]
        if ok != true {
            t.Fatalf("missing group %v", g)
        }
    }

    // 4. 验证分片分配的合法性：如果有副本组处于活跃状态，检查是否所有的分片都被分配到了合法的、且存在的组上
    if len(groups) > 0 {
        // 遍历所有分片 (s: 分片编号, g: 分配给的 GID)
        for s, g := range c.Shards {
            // 检查该分片指向的 GID 是否存在于当前的活跃组列表中
            _, ok := c.Groups[g]
            if ok == false {
                // 如果分片被分配到了一个不存在的 GID（比如已经被 Leave 移除的组），则测试失败
                t.Fatalf("shard %v -> invalid group %v", s, g)
            }
        }
    }

    // 5. 验证负载均衡（核心测试点）：检查分片在各组之间的分配是否尽可能均匀
    
    // 统计每个 GID 当前负责的分片数量
    counts := map[int]int{}
    for _, g := range c.Shards {
        counts[g] += 1
    }
    
    // 寻找分配分片数的最大值和最小值
    // min 初始化为 257（一个足够大的数字，因为实验中 NShards 通常固定为 10，257 远大于 10，作为初始下限是绝对安全的）
    min := 257
    max := 0
    
    // 遍历当前配置中【所有活跃的副本组】（注意：即使某个活跃组分到了 0 个分片，counts[g] 也会返回默认值 0）
    for g, _ := range c.Groups {
        if counts[g] > max {
            max = counts[g] // 更新最大值
        }
        if counts[g] < min {
            min = counts[g] // 更新最小值
        }
    }
    
    // 负载均衡的终极条件：拥有的分片数最多的组，和拥有的分片数最少的组，它们之间的分片数差值不能超过 1
    // 例如：10 个分片分给 3 个组，合法的分配只能是 4, 3, 3（max=4, min=3, max <= min+1）。不能是 5, 3, 2。
    if max > min+1 {
        t.Fatalf("max %v too much larger than min %v", max, min)
    }
}

func check_same_config(t *testing.T, c1 Config, c2 Config) {
	if c1.Num != c2.Num {
		t.Fatalf("Num wrong")
	}
	if c1.Shards != c2.Shards {
		t.Fatalf("Shards wrong")
	}
	if len(c1.Groups) != len(c2.Groups) {
		t.Fatalf("number of Groups is wrong")
	}
	for gid, sa := range c1.Groups {
		sa1, ok := c2.Groups[gid]
		if ok == false || len(sa1) != len(sa) {
			t.Fatalf("len(Groups) wrong")
		}
		if ok && len(sa1) == len(sa) {
			for j := 0; j < len(sa); j++ {
				if sa[j] != sa1[j] {
					t.Fatalf("Groups wrong")
				}
			}
		}
	}
}

func TestBasic(t *testing.T) {
    // 设定测试集群中的 Shardmaster 服务器数量为 3（构成一个 Raft 多数派集群）
    const nservers = 3
    // 初始化测试配置环境，启动 3 个 Shardmaster 节点，false 表示暂不引入不可靠网络
    cfg := make_config(t, nservers, false)
    // 确保测试结束时，清理并关闭所有服务器和网络资源
    defer cfg.cleanup()

    // 创建一个客户端（Clerk），它可以与集群中的所有 Shardmaster 节点通信
    ck := cfg.makeClient(cfg.All())

    // =========================================================================
    // 测试模块 1：基础的 Join（加入组）和 Leave（离开组）功能
    // =========================================================================
    fmt.Printf("Test: Basic leave/join ...\n")

    // 创建一个长度为 6 的 Config 数组，用于记录整个测试过程中的历史配置快照
    cfa := make([]Config, 6)
	DPrintf("准备query\n")
    // 获取系统的初始配置（Config 0），此时不应该有任何副本组
    cfa[0] = ck.Query(-1)
	DPrintf("准备check\n")
    // 调用外部的 check 函数，验证当前活跃的副本组列表是否为空（[]int{}）
    check(t, []int{}, ck)
	DPrintf("第一次check成功\n")
    // 声明第一个副本组的 GID 为 1
    var gid1 int = 1
    // 客户端发送 Join RPC，将 GID 1 以及其包含的 3 个服务器节点（x, y, z）加入集群
    ck.Join(map[int][]string{gid1: []string{"x", "y", "z"}})
    // 验证当前集群确实只包含 gid1，且所有的分片都分配给了 gid1
	
    check(t, []int{gid1}, ck)
	DPrintf("第二次check成功\n")
    // 获取最新的配置（Config 1），存入历史记录数组
    cfa[1] = ck.Query(-1)

    // 声明第二个副本组的 GID 为 2
    var gid2 int = 2
    // 客户端发送 Join RPC，将 GID 2 以及其包含的节点（a, b, c）加入集群
    ck.Join(map[int][]string{gid2: []string{"a", "b", "c"}})
    // 验证当前集群包含 gid1 和 gid2，且分片在这两个组之间分配均匀
    check(t, []int{gid1, gid2}, ck)
	DPrintf("第三次check成功\n")
    // 获取最新的配置（Config 2），存入历史记录数组
    cfa[2] = ck.Query(-1)

    // 再次查询最新配置，准备验证底层的服务器映射是否被正确存储
    cfx := ck.Query(-1)
    // 获取 GID 1 对应的服务器列表
    sa1 := cfx.Groups[gid1]
    // 严格校验 GID 1 的服务器数量是否为 3，且名称是否完全匹配
    if len(sa1) != 3 || sa1[0] != "x" || sa1[1] != "y" || sa1[2] != "z" {
        t.Fatalf("wrong servers for gid %v: %v\n", gid1, sa1)
    }
    // 获取 GID 2 对应的服务器列表
    sa2 := cfx.Groups[gid2]
    // 严格校验 GID 2 的服务器数量是否为 3，且名称是否完全匹配
    if len(sa2) != 3 || sa2[0] != "a" || sa2[1] != "b" || sa2[2] != "c" {
        t.Fatalf("wrong servers for gid %v: %v\n", gid2, sa2)
    }

    // 客户端发送 Leave RPC，命令 GID 1 离开集群
    // 此时系统需要进行负载均衡，将原本属于 GID 1 的分片全部分配给剩下的 GID 2
    ck.Leave([]int{gid1})
    // 验证当前集群只剩下 GID 2
    check(t, []int{gid2}, ck)
    // 获取最新的配置（Config 4），存入历史记录数组。（注：数组索引3这里被跳过未使用）
    cfa[4] = ck.Query(-1)

    // 客户端发送 Leave RPC，命令 GID 2 也离开集群
    ck.Leave([]int{gid2})
    // 获取最新的配置（Config 5），此时集群又回到了没有活跃组的状态
    cfa[5] = ck.Query(-1)

    fmt.Printf("  ... Passed\n")

    // =========================================================================
    // 测试模块 2：历史配置查询与 Raft 状态机持久化（宕机恢复）测试
    // =========================================================================
    fmt.Printf("Test: Historical queries ...\n")

    // 遍历集群中的 3 个 Shardmaster 节点
    for s := 0; s < nservers; s++ {
        // 关闭当前遍历到的节点（模拟节点宕机）
        cfg.ShutdownServer(s)
        // 遍历之前保存的每一个历史配置
        for i := 0; i < len(cfa); i++ {
            // 通过 Query(特定编号) 向剩下的多数派节点发起历史查询
            c := ck.Query(cfa[i].Num)
            // 验证查询到的历史配置与宕机前保存的快照完全一致
            check_same_config(t, c, cfa[i])
        }
        // 重新启动刚才被关闭的节点（验证它能否通过 Raft 日志回放恢复正确的状态机）
        cfg.StartServer(s)
        // 恢复网络连接
        cfg.ConnectAll()
    }

    fmt.Printf("  ... Passed\n")

    // =========================================================================
    // 测试模块 3：Move（手动移动分片）接口测试
    // =========================================================================
    fmt.Printf("Test: Move ...\n")
    {
        // 准备两个新的副本组，GID 分别为 503 和 504
        var gid3 int = 503
        ck.Join(map[int][]string{gid3: []string{"3a", "3b", "3c"}})
        var gid4 int = 504
        ck.Join(map[int][]string{gid4: []string{"4a", "4b", "4c"}})
        
        // 遍历系统中的所有分片（实验中 NShards 默认为 10）
        for i := 0; i < NShards; i++ {
            // 获取当前最新配置
            cf := ck.Query(-1)
            // 将前半部分的分片强制分配给 gid3
            if i < NShards/2 {
                ck.Move(i, gid3)
                // 验证：如果在 Move 之前，该分片不属于 gid3
                if cf.Shards[i] != gid3 {
                    // 查询 Move 之后的最新配置
                    cf1 := ck.Query(-1)
                    // 只要配置发生了变更，配置编号（Num）必须严格递增
                    if cf1.Num <= cf.Num {
                        t.Fatalf("Move should increase Config.Num")
                    }
                }
            } else {
                // 将后半部分的分片强制分配给 gid4
                ck.Move(i, gid4)
                // 同理验证配置编号是否递增
                if cf.Shards[i] != gid4 {
                    cf1 := ck.Query(-1)
                    if cf1.Num <= cf.Num {
                        t.Fatalf("Move should increase Config.Num")
                    }
                }
            }
        }
        // 所有的 Move 操作完成后，获取最终配置
        cf2 := ck.Query(-1)
        // 重新遍历验证每一个分片是否严格按照我们的要求分配到了指定的组
        for i := 0; i < NShards; i++ {
            if i < NShards/2 {
                if cf2.Shards[i] != gid3 {
                    t.Fatalf("expected shard %v on gid %v actually %v",
                        i, gid3, cf2.Shards[i])
                }
            } else {
                if cf2.Shards[i] != gid4 {
                    t.Fatalf("expected shard %v on gid %v actually %v",
                        i, gid4, cf2.Shards[i])
                }
            }
        }
        // 清理测试现场：让 gid3 和 gid4 离开集群
        ck.Leave([]int{gid3})
        ck.Leave([]int{gid4})
    }
    fmt.Printf("  ... Passed\n")

    // =========================================================================
    // 测试模块 4：并发 Leave / Join 压力测试（检测数据竞争和线性一致性）
    // =========================================================================
    fmt.Printf("Test: Concurrent leave/join ...\n")

    // 设置并发的客户端数量为 10
    const npara = 10
    // 创建包含 10 个独立客户端的数组
    var cka [npara]*Clerk
    for i := 0; i < len(cka); i++ {
        cka[i] = cfg.makeClient(cfg.All())
    }
    // 预先计算出这 10 个并发操作最终应该保留在集群中的 GID 列表
    gids := make([]int, npara)
    // 用于同步 10 个协程执行完毕的通道
    ch := make(chan bool)
    
    // 启动 10 个并发的 Goroutine
    for xi := 0; xi < npara; xi++ {
        // 计算目标 GID：100, 110, 120...190
        gids[xi] = int((xi * 10) + 100)
        go func(i int) {
            // 确保协程退出时向通道发送信号
            defer func() { ch <- true }()
            var gid int = gids[i]
            // 生成模拟的服务器名称
            var sid1 = fmt.Sprintf("s%da", gid)
            var sid2 = fmt.Sprintf("s%db", gid)
            
            // 并发执行三个操作：
            // 1. 加入一个编号为 gid+1000 的临时组
            cka[i].Join(map[int][]string{gid + 1000: []string{sid1}})
            // 2. 加入目标组（期望最终保留的组）
            cka[i].Join(map[int][]string{gid: []string{sid2}})
            // 3. 将刚刚加入的临时组 (gid+1000) 移除
            cka[i].Leave([]int{gid + 1000})
        }(xi)
    }
    // 主协程等待，直到接收到 10 个协程的完成信号
    for i := 0; i < npara; i++ {
        <-ch
    }
    // 并发结束后，严格检查系统中遗留的组是否刚好等于计算好的 gids 数组中的那 10 个
    check(t, gids, ck)

    fmt.Printf("  ... Passed\n")

    // =========================================================================
    // 测试模块 5：Join 操作后的最小化数据迁移验证（核心算法检查点）
    // =========================================================================
    fmt.Printf("Test: Minimal transfers after joins ...\n")

    // c1 记录此时的状态（目前有 10 个活跃的组，来自上一步并发测试）
    c1 := ck.Query(-1)
    
    // 再依次加入 5 个全新的组
    for i := 0; i < 5; i++ {
        var gid = int(npara + 1 + i)
        ck.Join(map[int][]string{gid: []string{
            fmt.Sprintf("%da", gid),
            fmt.Sprintf("%db", gid),
            fmt.Sprintf("%db", gid)}})
    }
    // c2 记录加入 5 个新组之后的状态（此时共有 15 个活跃组）
    c2 := ck.Query(-1)
    
    // 这里的逻辑极其关键：验证“老组之间不能互相转移分片”
    // 遍历原有的 10 个老组 (编号从 1 到 npara)
    for i := int(1); i <= npara; i++ {
        // 遍历所有的分片位置 j
        for j := 0; j < len(c1.Shards); j++ {
            // 如果在加入新组之后 (c2)，发现分片 j 被分配给了老组 i
            if c2.Shards[j] == i {
                // 那么在加入新组之前 (c1)，这个分片 j 必须本来就属于老组 i ！
                if c1.Shards[j] != i {
                    // 如果不属于，说明在 Join 导致重分配时，发生了老组和老组之间的多余数据迁移，测试失败
                    t.Fatalf("non-minimal transfer after Join()s")
                }
            }
        }
    }

    fmt.Printf("  ... Passed\n")

    // =========================================================================
    // 测试模块 6：Leave 操作后的最小化数据迁移验证（核心算法检查点）
    // =========================================================================
    fmt.Printf("Test: Minimal transfers after leaves ...\n")

    // 将刚才新加进来的那 5 个组移除，使集群又恢复到 10 个组的状态
    for i := 0; i < 5; i++ {
        ck.Leave([]int{int(npara + 1 + i)})
    }
    // c3 记录此时恢复到 10 个组之后的状态
    c3 := ck.Query(-1)
    
    // 验证逻辑同上：当别人离开时，存活的老组不应该把自己的旧数据给丢掉或者交换给别人
    // 遍历留下来的 10 个组
    for i := int(1); i <= npara; i++ {
        // 遍历所有分片
        for j := 0; j < len(c1.Shards); j++ {
            // 如果在离开操作之前 (c2)，分片 j 就属于老组 i
            if c2.Shards[j] == i {
                // 那么在离开操作之后 (c3)，因为老组 i 并没有离开，所以分片 j 必须依然属于老组 i
                if c3.Shards[j] != i {
                    // 如果分片飞到了别的地方，说明发生了不必要的迁移，测试失败
                    t.Fatalf("non-minimal transfer after Leave()s")
                }
            }
        }
    }

    fmt.Printf("  ... Passed\n")
}

func TestMulti(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	fmt.Printf("Test: Multi-group join/leave ...\n")

	cfa := make([]Config, 6)
	cfa[0] = ck.Query(-1)

	check(t, []int{}, ck)

	var gid1 int = 1
	var gid2 int = 2
	ck.Join(map[int][]string{
		gid1: []string{"x", "y", "z"},
		gid2: []string{"a", "b", "c"},
	})
	check(t, []int{gid1, gid2}, ck)
	cfa[1] = ck.Query(-1)

	var gid3 int = 3
	ck.Join(map[int][]string{gid3: []string{"j", "k", "l"}})
	check(t, []int{gid1, gid2, gid3}, ck)
	cfa[2] = ck.Query(-1)

	cfx := ck.Query(-1)
	sa1 := cfx.Groups[gid1]
	if len(sa1) != 3 || sa1[0] != "x" || sa1[1] != "y" || sa1[2] != "z" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid1, sa1)
	}
	sa2 := cfx.Groups[gid2]
	if len(sa2) != 3 || sa2[0] != "a" || sa2[1] != "b" || sa2[2] != "c" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid2, sa2)
	}
	sa3 := cfx.Groups[gid3]
	if len(sa3) != 3 || sa3[0] != "j" || sa3[1] != "k" || sa3[2] != "l" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid3, sa3)
	}

	ck.Leave([]int{gid1, gid3})
	check(t, []int{gid2}, ck)
	cfa[3] = ck.Query(-1)

	cfx = ck.Query(-1)
	sa2 = cfx.Groups[gid2]
	if len(sa2) != 3 || sa2[0] != "a" || sa2[1] != "b" || sa2[2] != "c" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid2, sa2)
	}

	ck.Leave([]int{gid2})

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Concurrent multi leave/join ...\n")

	const npara = 10
	var cka [npara]*Clerk
	for i := 0; i < len(cka); i++ {
		cka[i] = cfg.makeClient(cfg.All())
	}
	gids := make([]int, npara)
	var wg sync.WaitGroup
	for xi := 0; xi < npara; xi++ {
		wg.Add(1)
		gids[xi] = int(xi + 1000)
		go func(i int) {
			defer wg.Done()
			var gid int = gids[i]
			cka[i].Join(map[int][]string{
				gid: []string{
					fmt.Sprintf("%da", gid),
					fmt.Sprintf("%db", gid),
					fmt.Sprintf("%dc", gid)},
				gid + 1000: []string{fmt.Sprintf("%da", gid+1000)},
				gid + 2000: []string{fmt.Sprintf("%da", gid+2000)},
			})
			cka[i].Leave([]int{gid + 1000, gid + 2000})
		}(xi)
	}
	wg.Wait()
	check(t, gids, ck)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Minimal transfers after multijoins ...\n")

	c1 := ck.Query(-1)
	m := make(map[int][]string)
	for i := 0; i < 5; i++ {
		var gid = npara + 1 + i
		m[gid] = []string{fmt.Sprintf("%da", gid), fmt.Sprintf("%db", gid)}
	}
	ck.Join(m)
	c2 := ck.Query(-1)
	for i := int(1); i <= npara; i++ {
		for j := 0; j < len(c1.Shards); j++ {
			if c2.Shards[j] == i {
				if c1.Shards[j] != i {
					t.Fatalf("non-minimal transfer after Join()s")
				}
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Minimal transfers after multileaves ...\n")

	var l []int
	for i := 0; i < 5; i++ {
		l = append(l, npara+1+i)
	}
	ck.Leave(l)
	c3 := ck.Query(-1)
	for i := int(1); i <= npara; i++ {
		for j := 0; j < len(c1.Shards); j++ {
			if c2.Shards[j] == i {
				if c3.Shards[j] != i {
					t.Fatalf("non-minimal transfer after Leave()s")
				}
			}
		}
	}

	fmt.Printf("  ... Passed\n")
}
