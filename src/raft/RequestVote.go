package raft

import (
	"context"
	"sync"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//// 2A、2B 阶段需要补充：请求投票的参数（如候选人任期、日志信息等）
	// 字段名必须大写（Go RPC 仅传输大写字段）
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	//投票结果的回复（如是否同意投票、当前任期等）
	Term        int
	VoteGranted bool
}

// RequestVote 处理来自其他 Candidate 节点的拉票请求 (RPC 接收端)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 加锁保护节点状态（当前任期、投票记录、日志等）的并发读写
	rf.mu.Lock()
	defer rf.mu.Unlock() // 确保函数返回时释放锁

	DPrintf("主机：%d,RequestVote启动\n", rf.me)

	// 【⚠️ 隐患预警 1】无条件重置选举定时器（见下方解析）
	rf.updateTime()

	// 获取当前节点自身日志的最后一条记录的索引和任期
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.termAt(lastLogIndex)

	// ==========================================================
	// 场景一：候选人的任期比我新 (Candidate 发现了新大陆)
	// ==========================================================
	if rf.currentTerm < args.Term {
		// 只要看到更大的任期，立刻降级为 Follower 并更新自己的任期
		rf.state = Follower
		rf.turnToLeader = 0
		DPrintf("主机%d转为follower-收到投票请求时时\n", rf.me)
		rf.currentTerm = args.Term

		// 检查候选人的日志是否至少和自己一样新 (Raft 选举限制: Election Restriction)
		// 拒绝投票条件：我的最后日志任期更大，或者任期一样但我的日志更长
		if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
			reply.VoteGranted = false // 候选人日志太旧，拒绝投票
			rf.votedFor = -1          // 虽然更新了任期，但票没投出去，重置投票记录
		} else {
			reply.VoteGranted = true       // 候选人日志足够新，同意投票
			rf.votedFor = args.CandidateId // 记录把票投给了谁
		}
		reply.Term = rf.currentTerm // 返回自己更新后的最新任期
		rf.persist()                // 状态改变，落盘持久化

		// ==========================================================
		// 场景二：候选人的任期和我当前任期一样 (大家在同一届选举中竞争)
		// ==========================================================
	} else if rf.currentTerm == args.Term {
		reply.Term = rf.currentTerm

		// 1. 如果之前已经把票投给这个候选人了（RPC 幂等性，可能因为网络重传）
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.state = Follower
			rf.turnToLeader = 0
			DPrintf("主机%d转为follower-voteFor不为空时\n", rf.me)
			rf.votedFor = args.CandidateId
			rf.persist()

			// 2. 如果在当前任期还没有把票投给任何人
		} else if rf.votedFor == -1 {
			// 再次检查日志新旧程度
			// 【⚠️ 隐患预警 2】这里使用了 >=，这是一个逻辑错误（见下方解析）
			if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex >= args.LastLogIndex) {
				reply.VoteGranted = false // 拒绝投票
				rf.votedFor = -1
				rf.persist()
			} else {
				reply.VoteGranted = true // 同意投票
				rf.votedFor = args.CandidateId
				rf.persist()
			}

			// 3. 在当前任期，票已经投给别人了（一票多投限制）
		} else {
			reply.VoteGranted = false // 拒绝投票
		}

		// ==========================================================
		// 场景三：候选人的任期比我还老 (历史遗留的 Candidate 发来的请求)
		// ==========================================================
	} else {
		reply.Term = rf.currentTerm // 告诉他最新的任期，让他赶紧退位
		reply.VoteGranted = false   // 果断拒绝
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) HandleRequestVoteReply(reply *RequestVoteReply, nums *int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("主机：%d,HandleRequestVoteReply启动\n", rf.me)
	rf.lastMessageTime = time.Now()
	if reply.VoteGranted == false {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower // 若跟随者任期更高，当前节点应转为跟随者
			rf.turnToLeader = 0
			DPrintf("主机%d转为follower-投票请求回复处\n", rf.me)
			rf.ElectionTimeout = rf.randomTimeout()
			rf.votedFor = -1
			rf.persist()
		} else {
			rf.ElectionTimeout = rf.randomTimeout()
		}
	} else {
		*nums = *nums + 1
	}
}

// 1. 重构sendRequest函数：加入context超时控制
func (rf *Raft) sendRequest(i int, agreedCandidateNum *int, wg *sync.WaitGroup) {
	// 必须在函数开头调用wg.Done()，确保无论是否超时，WaitGroup计数都会递减
	defer wg.Done()
	DPrintf("主机：%d send candidate RPC to %d begin\n", rf.me, i)
	reply := new(RequestVoteReply)
	args := new(RequestVoteArgs)
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.lastLogIndex()
	args.LastLogTerm = rf.termAt(args.LastLogIndex)
	// 2. 创建20ms超时的上下文：ctx用于控制超时，cancel用于主动取消（需defer避免泄漏）
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel() // 确保函数退出时释放context资源，避免内存泄漏

	// 3. 创建一个通道：用于接收RPC的执行结果（成功/失败）
	rpcResult := make(chan bool, 1) // 缓冲通道，避免协程阻塞

	// 4. 启动子协程：执行RPC发送逻辑（将RPC与超时检测解耦）
	go func() {
		// 执行RPC发送（这是可能耗时的操作）
		ok := rf.sendRequestVote(i, args, reply)
		// 将RPC结果发送到通道（即使超时，发送也不会阻塞，因为通道有缓冲）
		rpcResult <- ok
	}()

	// 5. 超时检测与结果处理：监听ctx.Done()（超时信号）或rpcResult（RPC结果）
	select {
	case ok := <-rpcResult:
		// case1：RPC在20ms内完成，处理结果
		if !ok {
			DPrintf("主机：%d,candidate RPC action send to %d fail\n", rf.me, i)
		} else {
			DPrintf("主机：%d,candidate RPC action send to %d\n", rf.me, i)
			// 处理投票回复（需加锁保护共享变量agreedCandidateNum）
			rf.HandleRequestVoteReply(reply, agreedCandidateNum)
		}

	case <-ctx.Done():
		// case2：20ms超时，ctx.Done()通道被触发（返回超时原因）
		DPrintf("主机：%d send to %d RPC timeout (exceed 20ms), force exit\n", rf.me, i)
		// 超时后主动退出，不执行后续逻辑（相当于“强行结束”该协程的有效逻辑）
		return
	}
}

// FollowerAction 作为后台常驻协程运行，负责监控 Leader 心跳并在超时后触发选举。
func (rf *Raft) FollowerAction() {
	var wg sync.WaitGroup
	for {
		// 1. 检查节点是否已经被主动关闭 (Graceful Shutdown)
		if rf.killed() {
			wg.Wait() // 等待所有正在进行的 RPC 请求结束
			return
		}

		// 2. 如果当前节点已经是 Leader，则不需要进行选举超时检查
		// 这里选择短暂休眠交出 CPU 时间片。Leader 的心跳通常由另一个独立的协程负责发送。
		if rf.state == Leader {
			time.Sleep(10 * time.Millisecond)
		} else {
			rf.mu.Lock()
			// 3. 计算距离上一次收到有效消息（Leader 心跳、投票请求等）经过的时间
			elapsed := time.Since(rf.lastMessageTime)

			// 4. 判断是否发生选举超时
			if elapsed <= rf.ElectionTimeout {
				rf.mu.Unlock()
				// 未超时，说明 Leader 依然存活，当前节点休眠一小段时间后进入下一轮检查
				time.Sleep(40 * time.Millisecond)
			} else {
				// =================== 超时，开始发起选举 ===================
				if rf.state == Follower {
					DPrintf("超时！开始选举,当前是主机：%d\n", rf.me)
				} else {
					DPrintf("上一次选举失败，再次开始,当前是主机：%d\n", rf.me)
				}
				// 5. 状态转换：成为候选人 (Candidate)
				rf.state = Candidate
				rf.currentTerm = rf.currentTerm + 1 // 增加当前任期号
				rf.votedFor = rf.me                 // 首先把票投给自己
				rf.persist()                        // 持久化当前任期和投票信息，防止宕机重启后状态丢失
				rf.mu.Unlock()

				// 6. 准备发送拉票请求 (RequestVote RPC)
				i := 0
				agreedCandidateNum := 1 // 初始票数为 1（自己投给自己的一票）
				wg = sync.WaitGroup{}

				for i = 0; i < len(rf.peers); i++ {
					// 如果在发送请求期间状态发生了变化（例如收到了更高任期的心跳），则终止拉票
					if rf.state != Candidate {
						break
					}
					// 跳过自己
					if i == rf.me {
						continue
					}
					// 向有效的对端节点并行发送拉票请求
					if rf.peers[i] != nil {
						DPrintf("主机：%d，在投票阶段调用wg.add（1）\n", rf.me)
						wg.Add(1)
						// 注意：此处传递了 agreedCandidateNum 的指针以便在协程内累加选票
						go rf.sendRequest(i, &agreedCandidateNum, &wg)
					}
				}

				// 7. 等待所有拉票 RPC 返回
				wg.Wait()

				// =================== 计票与状态结算 ===================

				// 8. 选举失败：未获得大多数选票（<= 节点总数的一半），且状态仍为 Candidate
				if agreedCandidateNum*2 <= len(rf.peers) && rf.state == Candidate {
					DPrintf("主机：%d,上一次选举失败,重新产生超时时间，\n失败原因agreedCandidateNum=%d, rf.state=%d", rf.me, agreedCandidateNum, rf.state)
					// 重置消息时间和超时时间，准备下一轮的随机超时选举以避免瓜分选票（Split Vote）
					rf.lastMessageTime = time.Now()
					rf.ElectionTimeout = rf.randomTimeout()
				}

				// 9. 选举成功：获得大多数选票（> 节点总数的一半），且状态仍为 Candidate
				if agreedCandidateNum*2 > len(rf.peers) && rf.state == Candidate {
					rf.mu.Lock()
					DPrintf("主机：%d成为leader,当前Term：%d\n", rf.me, rf.currentTerm)
					rf.state = Leader

					// 10. Raft 核心细节：Leader 上任时追加一条 No-op（空操作）日志
					// 这是为了能够安全地提交之前任期遗留下来的日志
					rf.logs = append(rf.logs, one_log{Term: rf.currentTerm, Index: rf.lastLogIndex() + 1, Committed: false})
					rf.turnToLeader = 0
					rf.persist() // 持久化新追加的日志
					rf.mu.Unlock()
				}
			}
		}
	}
}
