package raft

import (
	"context"
	"sync"
	"time"
	//"log"
)

type AppendEntriesArgs struct {
	Term         int       // 领导者的任期
	LeaderId     int       // 领导者的 ID
	PrevLogIndex int       // 前一个日志条目的索引
	PrevLogTerm  int       // 前一个日志条目的任期
	Entries      []one_log // 要存储的日志条目（心跳消息时为空）
	LeaderCommit int       // 领导者的提交索引
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int//将其定义为出现差错的index
	ConflictTerm  int
}

// applyLog 是 Raft 的后台守护协程，负责将已提交的日志通过 applyCh 发送给上层应用。
func (rf *Raft) applyLog() {
    // 程序退出时触发一次持久化（这在循环内是不够的，通常持久化应在状态变更时立即执行）
    defer func() {
        rf.persist()
    }()
    
    for {
        // 1. 检查是否有需要应用的日志
        if rf.lastApplied == rf.commitIndex {
            // 没有待提交的日志，短暂休眠，避免 CPU 空转
            time.Sleep(5 * time.Millisecond)
            continue
        }
        
        // 2. 存在需要提交的日志，循环进行处理
        for rf.lastApplied < rf.commitIndex {
            rf.mu.Lock() // 锁住内部状态以读取日志和索引
            rf.lastApplied++ // 递增应用索引
            
            // 🌟 风险点：这里频繁加解锁，在复杂网络环境下可能导致状态不一致
            
            // 情况 A：如果该日志已经被标记为提交过且存在 Cmd，跳过（幂等性）
            if rf.committed[rf.lastApplied] == true && rf.logs[rf.lastApplied].Cmd != nil {
                rf.mu.Unlock()
                continue
            }
            
            // 情况 B：如果 Cmd 为空（可能是占位符/空日志），仅更新状态并统计占位数量
            if rf.logs[rf.lastApplied].Cmd == nil && rf.lastApplied != 0 {
                rf.CurnilNum++ // 记录空日志偏移量，用于修正索引映射
                rf.logs[rf.lastApplied].Committed = true
                rf.committed[rf.lastApplied] = true
                rf.persist() // 立即持久化状态变化
                rf.mu.Unlock()
                continue
            }
            
            // 情况 C：正常的日志提交逻辑
            newApplyMsg := new(ApplyMsg)
            rf.logs[rf.lastApplied].Committed = true
            rf.committed[rf.lastApplied] = true
            rf.persist() // 持久化修改后的 committed 状态
            
            newApplyMsg.Command = rf.logs[rf.lastApplied].Cmd
            // 计算向上传递的 CommandIndex，减去空日志偏移量，确保索引连续
            newApplyMsg.CommandIndex = rf.lastApplied - rf.CurnilNum
            newApplyMsg.CommandValid = true
            
            // 🌟 致命阻塞风险点：直接发送到 applyCh
            rf.applyCh <- *newApplyMsg
            
            DPrintf("主机：%d中索引为%d的日志提交完成,提交内容为：%v\n", rf.me, rf.lastApplied-rf.CurnilNum, rf.logs[rf.lastApplied].Cmd)
            rf.mu.Unlock()
        }
    }
}

// func (rf *Raft) applyLog() {
//     for !rf.killed() {
//         rf.mu.Lock()
        
//         // 1. 只要不需要提交日志，就释放锁并乖乖挂起睡觉，绝不消耗 CPU
//         for rf.commitIndex <= rf.lastApplied {
//             rf.applyCond.Wait() 
//             if rf.killed() {
//                 rf.mu.Unlock()
//                 return
//             }
//         }

//         // 2. 醒来发现有活干了！在锁内把需要推送的日志拷贝出来
//         commitIndex := rf.commitIndex
//         lastApplied := rf.lastApplied
//         entries := make([]ApplyMsg, 0, commitIndex-lastApplied)
        
//         for i := lastApplied + 1; i <= commitIndex; i++ {
//             entries = append(entries, ApplyMsg{
//                 CommandValid: true,
//                 Command:      rf.logs[i].Cmd,
//                 CommandIndex: i, // 如果你有 CurnilNum 偏移逻辑，在这里减掉
//             })
//         }

//         // 3. 更新 lastApplied，然后立刻释放锁！
//         rf.lastApplied = commitIndex
//         rf.mu.Unlock()

//         // 4. 在没有锁的“一身轻”状态下，慢慢把消息发给 KVServer
//         for _, msg := range entries {
//             rf.applyCh <- msg
//             DPrintf("主机：%d 成功投递 Index %d 到 applyCh", rf.me, msg.CommandIndex)
//         }
//     }
// }

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	//// 2A、2B 阶段实现：处理其他节点的投票请求
	DPrintf("主机：%d想要获得锁\n",rf.me)
	rf.mu.Lock()
	defer func(){
		rf.mu.Unlock()
		DPrintf("主机：%d处理完心跳即将退出\n",rf.me)
	}()
	DPrintf("主机：%d,AppendEntries启动,当前日志为%v\n", rf.me, rf.logs)
	//DPrintf("主机：%d,AppendEntries启动,args:%v\n", rf.me, args)
	rf.updateTime()
	
	if rf.currentTerm <= args.Term {
		if rf.currentTerm < args.Term {
			rf.votedFor = -1
			rf.persist()
		}
		if rf.state==Leader{
			rf.state = Follower
			rf.turnToLeader = 0
			if args.Term>rf.currentTerm{
				rf.currentTerm = args.Term
				rf.persist()
			}
			reply.Term = rf.currentTerm
			reply.Success=false
			reply.ConflictIndex=args.PrevLogIndex+1
			DPrintf("主机%d转为follower-心跳接收处\n", rf.me)
			return
		}
		rf.state = Follower
		rf.turnToLeader = 0
		DPrintf("主机%d保持follower-心跳接收处,当前commitIndex：%d\n", rf.me,rf.commitIndex)
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.persist()
		if rf.commitIndex>args.LeaderCommit{
			reply.Success=false
			reply.ConflictIndex=args.PrevLogIndex+1
			return
		}
		if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			if args.PrevLogIndex >= len(rf.logs) {
				DPrintf("主机：%d 日志存在问题 因为args.PrevLogIndex是%d但是len(rf.logs)为%d\n", rf.me, args.PrevLogIndex, len(rf.logs))
				reply.ConflictIndex = len(rf.logs)
				reply.ConflictTerm=-1
			} else {
				DPrintf("主机：%d 日志存在问题 因为rf.logs[args.PrevLogIndex].Term是%d但是args.PrevLogTerm为%d\n", rf.me, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)
				i := args.PrevLogIndex
				if rf.logs[args.PrevLogIndex].Term<args.PrevLogTerm{
					reply.ConflictIndex=args.PrevLogIndex-1
				}else if rf.logs[args.PrevLogIndex].Term>args.PrevLogTerm{
					for i >= 0 && rf.logs[i].Term == rf.logs[args.PrevLogIndex].Term {
						i--
					}
					reply.ConflictIndex = i + 1
				}
				reply.ConflictTerm=rf.logs[args.PrevLogIndex].Term
			}
			DPrintf("主机：%d reply内容为%v\n", rf.me, reply)
			reply.Success = false
		} else {
			reply.Success = true
			if args.Entries != nil {
				rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
				DPrintf("主机：%d 日志更新,当前日志内容为%v,当前term为%d\n", rf.me, rf.logs,rf.currentTerm)
				if len(rf.committed) < len(rf.logs) {
					// 扩展 committed 数组，新元素默认为 false
					extra := make([]bool, len(rf.logs)-len(rf.committed))
					rf.committed = append(rf.committed, extra...)
				}
				rf.lastLogIndex = len(rf.logs)
				rf.persist()
			} else {
				DPrintf("主机：%d收到空心跳信息,当前日志内容为%v，当前term为%d\n", rf.me, rf.logs,rf.currentTerm)
			}
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex=min(args.LeaderCommit,len(rf.logs)-1)
			}
		}
	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// HandleAppendEntriesReply 是 Leader 处理 Follower 返回的 AppendEntries 响应的回调函数。
// 它的核心职责是：
// 1. 检查自己是否已经“过气”（遇到更高的 Term），如果是则立即退位。
// 2. 如果日志产生冲突，利用快速回退算法（Fast Backup）迅速定位冲突点，更新 nextIndex。
// 3. 如果追加成功，更新 matchIndex 和 nextIndex，并唤醒后台协程去检查是否可以推进全局 commitIndex。
func (rf *Raft) HandleAppendEntriesReply(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    DPrintf("主机：%d, HandleAppendEntriesReply启动\n", rf.me)

    // ==========================================
    // 阶段 1：请求被拒绝 (Success == false)
    // ==========================================
    // 前置条件：当前节点必须还是 Leader（如果在等待 RPC 期间被降级了，就不该处理响应）
    if reply.Success == false && rf.state == Leader {
        
        // 场景 A：任期落后（大清亡了）
        // 如果 Follower 返回的 Term 比 Leader 当前的 Term 还要大，
        // 说明集群中已经有了新的 Leader，当前节点必须立刻无条件退位。
        if reply.Term > rf.currentTerm {
            rf.currentTerm = reply.Term
            rf.state = Follower // 降级为跟随者
            rf.turnToLeader = 0 // 重置新任 Leader 标志
            DPrintf("主机%d转为follower-心跳回复处\n", rf.me)
            
            rf.votedFor = -1    // 清空投票记录，准备可能的新一轮选举
            rf.updateTime()     // 重置选举超时定时器，防止刚降级就立刻发起选举
            rf.persist()        // 🌟 任期和投票记录改变，必须落盘持久化
            
            // 🌟 唤醒后台的 checkCommit 协程
            // 因为当前节点已经不是 Leader，需要唤醒正在 Wait() 的协程，
            // 让它在下一轮循环检查状态时发现自己是 Follower 从而安全退出或挂起。
            if rf.commitCond != nil {
                rf.commitCond.Broadcast() 
            }
            
        // 场景 B：任期没问题，但是日志不匹配（Log Inconsistency）
        // 此时触发 6.824 中极其重要的“快速回退 (Fast Backup)”逻辑，避免 nextIndex 一次只减 1 导致同步极慢。
        } else {
            // Case 1: Follower 那个位置根本没有日志（日志太短了）
            // Follower 返回 ConflictTerm = -1，ConflictIndex 是它的日志总长度。
            // Leader 直接把 nextIndex 降到 Follower 期望的那个位置。
            if reply.ConflictTerm == -1 {
                rf.nextIndex[i] = reply.ConflictIndex
                
            // Case 2 & 3: Follower 那个位置有日志，但是任期冲突了
            } else if reply.ConflictTerm > args.PrevLogTerm {
                // 如果 Follower 的冲突任期比 Leader 发过去的 PrevLogTerm 还大（逻辑上较为罕见，通常做统一回退）
                // Leader 直接回退到 Follower 提供的该冲突任期的第一条日志位置。
                rf.nextIndex[i] = reply.ConflictIndex
                
            } else if reply.ConflictTerm < args.PrevLogTerm {
                // 如果 Follower 的冲突任期小于 Leader 发过去的 PrevLogTerm。
                // Leader 需要在自己的日志中，从前往后倒推，试图找到这个 ConflictTerm 的最后一条日志。
                pos := args.PrevLogIndex
                for pos >= 0 && rf.logs[pos].Term != reply.ConflictTerm {
                    pos--
                }
                // 如果找到了，或者遍历完了，就把 nextIndex 设为该 Term 的后一位
                rf.nextIndex[i] = pos + 1
            }
            
            // 兜底防御：由于 Raft 真实日志通常从下标 1 开始（下标 0 是哨兵 nil 节点），
            // 严禁把 nextIndex 减到 0，否则后续发心跳时会数组越界。
            if rf.nextIndex[i] == 0 {
                rf.nextIndex[i]++
            }
            DPrintf("Leader:%d 更新matchIndex[%d]为%d(其实没更新) 更新nextIndex[%d]为%d", rf.me, i, rf.matchIndex[i], i, rf.nextIndex[i])
        }
        
    // ==========================================
    // 阶段 2：请求被接受 (Success == true)
    // ==========================================
    // 说明 Follower 完美匹配了 PrevLogIndex，并且成功把发过去的 Entries 记入了自己的日志。
    } else if reply.Success == true && rf.state == Leader {
        // 更新已经安全复制到的最高索引：传入的基准索引 + 这次打包发过去的日志数量
        rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
        // 更新下一次准备给该节点发日志的起点索引
        rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
        
        DPrintf("Leader:%d 更新matchIndex[%d]为%d 更新nextIndex[%d]为%d", rf.me, i, rf.matchIndex[i], i, rf.nextIndex[i])
        
        // 🌟 唤醒检查提交的后台协程
        // 既然有一个节点成功跟上了最新的进度，这意味着极有可能已经凑够了多数派！
        // 立刻 Broadcast 叫醒等待在条件变量上的 checkCommit 协程，让它去算选票、推 commitIndex。
        rf.commitCond.Broadcast()
        
        // 性能考量：虽然你加了 persist()，但实际上 matchIndex 和 nextIndex 
        // 属于 Leader 的 Volatile State（易失性状态），无需落盘。
        // 若没有改变 currentTerm、votedFor 或 logs，这里可以不调用 persist() 以节省 IO。
        rf.persist()
    }
}

// 1. 重构sendRequest函数：加入context超时控制
func (rf *Raft) sendEntries(i int, wg *sync.WaitGroup) {
	// 必须在函数开头调用wg.Done()，确保无论是否超时，WaitGroup计数都会递减
	defer wg.Done()
	reply := new(AppendEntriesReply)
	args := new(AppendEntriesArgs)
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[i] - 1
	//DPrintf("rf.logs的长度为%d,args.PrevLogIndex是%d\n",len(rf.logs),args.PrevLogIndex)
	args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
	args.Entries = rf.logs[rf.nextIndex[i]:]
	args.LeaderCommit = rf.commitIndex
	if rf.nextIndex[i] < len(rf.logs) {
		args.Entries = rf.logs[rf.nextIndex[i]:]
	}
	args.LeaderCommit = rf.commitIndex
	DPrintf("主机：%d send AppendEntries RPC to %d begin,自身日志为%v\n且日志内容中PrevLogIndex: %d,PrevLogTerm: %d,LeaderCommit:%d,Entries: %v", rf.me, i,rf.logs, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
	//DPrintf("主机：%d send AppendEntries RPC to %d begin,args: %v", rf.me, i,args)

	// 2. 创建20ms超时的上下文：ctx用于控制超时，cancel用于主动取消（需defer避免泄漏）
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel() // 确保函数退出时释放context资源，避免内存泄漏

	// 3. 创建一个通道：用于接收RPC的执行结果（成功/失败）
	rpcResult := make(chan bool, 1) // 缓冲通道，避免协程阻塞

	// 4. 启动子协程：执行RPC发送逻辑（将RPC与超时检测解耦）
	go func() {
		// 执行RPC发送（这是可能耗时的操作）
		ok := rf.sendAppendEntries(i, args, reply)
		// 将RPC结果发送到通道（即使超时，发送也不会阻塞，因为通道有缓冲）
		rpcResult <- ok
	}()

	// 5. 超时检测与结果处理：监听ctx.Done()（超时信号）或rpcResult（RPC结果）
	select {
	case ok := <-rpcResult:
		// case1：RPC在20ms内完成，处理结果
		if !ok {
			DPrintf("主机：%d,AppendEntries RPC action send to %d fail\n", rf.me, i)
		} else {
			DPrintf("主机：%d,AppendEntries RPC action send to %d finished\n", rf.me, i)
			// 处理投票回复（需加锁保护共享变量agreedCandidateNum）
			rf.HandleAppendEntriesReply(i, args, reply)
		}

	case <-ctx.Done():
		// case2：20ms超时，ctx.Done()通道被触发（返回超时原因）
		DPrintf("主机：%d send to %d AppendEntries RPC timeout (exceed 20ms), force exit\n", rf.me, i)
		// 超时后主动退出，不执行后续逻辑（相当于“强行结束”该协程的有效逻辑）
		return
	}
}


// checkCommit 是一个用于检查并推进 Leader 节点 commitIndex 的函数。
// 🚨 注意：当前实现为一个包含死循环和 time.Sleep 的后台轮询协程。
// 在严苛的分布式测试（如 6.824 3A 高并发分区测试）中，如果在每次发送心跳时
// 都启动一个这样的协程，会迅速导致数十万个睡眠协程泄漏，最终耗尽资源卡死系统。
//
// 正确的架构应该是：移除外层的 for 循环和 time.Sleep，将其变为一个普通的同步函数，
// 并仅在 Leader 收到 AppendEntries RPC 的成功回复且更新了 matchIndex 时调用一次。
func (rf *Raft) checkCommit() {
    // ❌ 架构缺陷：死循环轮询。这会将本应由事件驱动的逻辑转变为无休止的后台消耗。
    for {
        rf.mu.Lock() // 获取锁，准备读取核心状态
        
        // 1. 身份校验：只有 Leader 才有资格推进全局的 commitIndex
        if rf.state != Leader {
            rf.mu.Unlock() // 不是 Leader，立即释放锁
            // ❌ 架构缺陷：无效的睡眠等待。如果节点不是 Leader，这个协程应该直接 return 退出，
            // 而不是永远睡在这里浪费资源。
			return
            // time.Sleep(20 * time.Millisecond)
            // continue
        } else {
            // 2. 核心推进逻辑：根据 Raft 论文的 Rules for Servers (Leader) 5.3 & 5.4 节
            // 从最新的日志索引开始，从后往前遍历，寻找满足多数派复制条件的最高索引 N。
            // 遍历范围：[len(rf.logs)-1, rf.commitIndex)
            for N := len(rf.logs) - 1; N > rf.commitIndex; N-- {
                
                // 3. 关键安全性规则 (论文 Figure 8)：
                // Leader 只能提交**当前任期**内创建的日志条目。
                // 如果发现某条日志的任期不是当前任期，不能直接通过计算副本来提交它
                // （必须等到当前任期的一条新日志被提交，才能隐式地将旧任期的日志一起提交）。
                if rf.logs[N].Term != rf.currentTerm {
                    // 这里使用 break 是因为从后往前遍历，如果遇到旧任期日志，
                    // 说明当前任期还没有日志被安全复制（或者遍历已经越过了当前任期的边界），
                    // 因此无需继续向前检查，直接跳出。
                    break
                }
                
                numsCommit := 0 // 计数器：记录有多少个节点已经复制了索引至少为 N 的日志
                
                // 4. 统计副本数：遍历集群中的所有节点
                for i := 0; i < len(rf.peers); i++ {
                    if i == rf.me {
                        // Leader 本身必定拥有该日志，直接算作一票
                        numsCommit++
                        continue
                    } else if rf.matchIndex[i] >= N {
                        // 如果节点 i 的 matchIndex 大于等于 N，说明它已经复制了这条日志
                        numsCommit++
                    }
                }
                
                // 5. 多数派判定：如果已复制的节点数超过集群半数（包括自己）
                if numsCommit*2 > len(rf.peers) && rf.state == Leader {
                    // 更新 Leader 的 commitIndex 到 N
                    // 此时，后台的 applyLog 协程一旦发现 commitIndex > lastApplied，
                    // 就会立刻开始将日志推送给 KVServer 状态机。
                    rf.commitIndex = N
                    
                    // 找到最高的可提交索引后即可跳出（因为更小的索引自然也被多数派复制了）
                    break 
                }
            }
            // 2. 带着锁直接调用 Wait()！
            // Wait 会自动帮你 Unlock，醒来时会自动帮你重新 Lock。
            rf.commitCond.Wait() 
            
            rf.mu.Unlock() // 3. Wait 结束后释放锁，进入下一轮循环
            // ❌ 架构缺陷：成功或失败后的无脑睡眠。
            // 这种基于时间的轮询不仅响应迟钝（最多延迟 20ms），而且极耗性能。
            // time.Sleep(20 * time.Millisecond)
        }
    }
}

// updateCommitIndex 是纯计算函数，没有任何循环等待和阻塞。
// func (rf *Raft) updateCommitIndex() {
//     rf.mu.Lock()
//     defer rf.mu.Unlock()

//     if rf.state != Leader {
//         return
//     }

//     commitUpdated := false
//     // 从最新日志倒着往前找多数派
//     for N := len(rf.logs) - 1; N > rf.commitIndex; N-- {
//         // 只能提交本任期的日志
//         if rf.logs[N].Term != rf.currentTerm {
//             break 
//         }

//         numsCommit := 1 // 自己算一票
//         for i := 0; i < len(rf.peers); i++ {
//             if i != rf.me && rf.matchIndex[i] >= N {
//                 numsCommit++
//             }
//         }

//         // 如果达到多数派，推进 commitIndex
//         if numsCommit*2 > len(rf.peers) {
//             rf.commitIndex = N
//             commitUpdated = true
//             break
//         }
//     }

//     // 🌟 核心：如果真的推进了，立刻按铃唤醒 applyLog 协程起来干活！
//     if commitUpdated {
//         rf.applyCond.Broadcast()
//     }
// }

func (rf *Raft) initLeader() {
	if rf.killed(){
		return
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
	for i:=1;i<len(rf.logs);i++{
		if rf.logs[i].Cmd==nil{
			rf.MaxnilNum++
		}
	}
	rf.turnToLeader = 1
	DPrintf("主机：%d  initLeader完成", rf.me)
}

// LeaderAction 是一个常驻的后台守护协程 (Goroutine)，在整个 Raft 节点生命周期内不断循环。
// 它的职责是：监控当前节点状态，一旦发现自己是 Leader，就以固定的频率（HeartbeatInterval）
// 向集群中的所有其他节点并发发送 AppendEntries RPC（心跳或日志同步）。
func (rf *Raft) LeaderAction() {
    var wg sync.WaitGroup
    var lastLoopTime time.Time // 用于性能诊断：记录上一次心跳循环开始的时间

    // 无限循环，驱动 Leader 的生命周期
    for {
        // 1. 退出机制：如果测试框架调用了 Kill()，则等待所有正在发送的 RPC 协程结束，然后安全退出
        if rf.killed() {
            wg.Wait()
            return
        }

        // 2. 状态门禁：如果当前不是 Leader（是 Follower 或 Candidate），
        // 则休眠 10 毫秒后再次醒来检查。这里起到了“待命”的作用，避免死循环榨干 CPU。
        if rf.state != Leader {
            time.Sleep(10 * time.Millisecond)
        } else {
            // 3. Leader 初始化：如果刚刚通过选举成为 Leader (turnToLeader == 0)
            // 需要执行新任 Leader 的初始化工作（如重置 nextIndex 和 matchIndex 数组）
            if rf.turnToLeader == 0 {
                rf.MaxnilNum = 0 // 重置某些定制化的空日志计数
                rf.initLeader()  // 初始化 Leader 专属的易失性状态
            }

            // --- 性能监控代码 ---
            currentTime := time.Now()
            if !lastLoopTime.IsZero() {
                // 计算距离上一次发送心跳过去了多久，用于排查心跳是否发送过慢
                interval := currentTime.Sub(lastLoopTime)
                DPrintf("主机：%d，LeaderAction 循环间隔: %v\n", rf.me, interval)
            }
            lastLoopTime = currentTime

            // 4. 提交检查：开启一个新协程，去检查是否可以更新 commitIndex
            // 🚨 【极其危险】：这里就是导致你 30 万个协程泄漏的源头！
            go rf.checkCommit()

            // 5. 并发发送心跳准备
            i := 0
            wg = sync.WaitGroup{} // 初始化 WaitGroup，用于等待本轮心跳全部发送完毕

            // 遍历整个集群的所有节点
            for i = 0; i < len(rf.peers); i++ {
                if i == rf.me {
                    continue // 不给自己发心跳
                }
                
                // 防御性检查：在遍历发送的过程中，如果发现自己被降级了（收到了更高任期的 RPC）
                // 立即打断循环，停止发送本轮剩余的心跳
                if rf.state != Leader {
                    break
                }
                
                if rf.peers[i] != nil {
                    DPrintf("主机：%d，在心跳阶段调用wg.add（1）\n", rf.me)
                    wg.Add(1) // 注册一个等待任务
                    
                    // 6. 异步发送：启动一个新协程，去给节点 i 发送 AppendEntries RPC
                    // 传入 wg 的指针，以便在 sendEntries 结束时调用 wg.Done()
                    go rf.sendEntries(i, &wg)
                }
            }

            // 7. 同步屏障：阻塞等待上述 for 循环中发出的所有心跳 RPC 返回结果
            // 🚨 【极其危险】：这会彻底毁掉 Leader 的心跳频率！
            wg.Wait()

            // 8. 心跳间隔：本轮处理完毕，严格休眠 HeartbeatInterval 时间（通常约 100ms）
            time.Sleep(HeartbeatInterval)
        }
    }
}


// LeaderAction 是一个常驻的后台守护协程，驱动 Leader 的生命周期。
// 改造后：采用纯异步（Fire-and-Forget）模型，彻底移除 WaitGroup，
// 确保 Leader 的心跳节拍永远不会被断网或响应慢的 Follower 拖死。
// func (rf *Raft) LeaderAction() {
	
//     for {
//         // 1. 退出机制
//         if rf.killed() {
//             // ❌ 已经移除了 wg.Wait()，直接无牵无挂地退出
//             return
//         }

//         // 2. 状态检查（快速跳过非 Leader 状态）
//         rf.mu.Lock()
//         state := rf.state
//         rf.mu.Unlock()

//         if state != Leader {
//             time.Sleep(10 * time.Millisecond)
//             continue
//         }

//         // 3. 正式作为 Leader 开始干活，加锁保护状态读取
//         rf.mu.Lock()
        
//         // 防御性二次检查：加锁后可能发现自己已经被降级了
//         if rf.state != Leader {
//             rf.mu.Unlock()
//             continue
//         }

//         // 4. 新 Leader 初始化
//         if rf.turnToLeader == 0 {
//             rf.MaxnilNum = 0 
//             rf.initLeader()  
//         }

// 		wg = sync.WaitGroup{} // 初始化 WaitGroup，用于等待本轮心跳全部发送完毕
//         // 5. 异步发射心跳！
//         for i := 0; i < len(rf.peers); i++ {
//             if i == rf.me {
//                 continue
//             }
            
//             // 🚀 核心改造：纯异步发射！
//             // 启动协程去发包，发完直接走人，绝对不等待它返回！
//             // (注意：这里去掉了传给 sendEntries 的 &wg 参数)
//             go rf.sendEntries(i,wg)
//         }
        
//         // 释放锁，让其他协程（如处理回复的协程、收到客户端新请求的协程）能顺利执行
//         rf.mu.Unlock()

//         // 6. 心跳间隔：准时休眠一个心跳周期（通常约 100ms左右）。
//         // 因为没有了 wg.Wait() 的拖累，Leader 会像一个精准的节拍器一样，时间一到立刻醒来发下一轮。
//         time.Sleep(HeartbeatInterval)
//     }
// }
