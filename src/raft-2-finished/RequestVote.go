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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//// 2A、2B 阶段实现：处理其他节点的投票请求
	rf.mu.Lock()         // 加锁保护共享变量
	defer rf.mu.Unlock() // 退出时解锁
	DPrintf("主机：%d,RequestVote启动\n", rf.me)
	rf.updateTime()
	rf.lastLogIndex = len(rf.logs) - 1
	if rf.currentTerm < args.Term {
		rf.state = Follower
		rf.turnToLeader = 0
		DPrintf("主机%d转为follower-收到投票请求时时\n",rf.me)
		rf.currentTerm = args.Term
		if rf.logs[rf.lastLogIndex].Term > args.LastLogTerm || (rf.logs[rf.lastLogIndex].Term == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex) {
			reply.VoteGranted = false
			rf.votedFor = -1
		} else {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
		reply.Term = rf.currentTerm
		rf.persist()
	} else if rf.currentTerm == args.Term {
		reply.Term = rf.currentTerm
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.state = Follower
			rf.turnToLeader = 0
			DPrintf("主机%d转为follower-voteFor不为空时\n",rf.me)
			rf.votedFor = args.CandidateId
			rf.persist()
		} else if rf.votedFor == -1 {
			if rf.logs[rf.lastLogIndex].Term > args.LastLogTerm || (rf.logs[rf.lastLogIndex].Term == args.LastLogTerm && rf.lastLogIndex >= args.LastLogIndex) {
				reply.VoteGranted = false
				rf.votedFor = -1
				rf.persist()
			} else {
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				rf.persist()
			}
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
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
			DPrintf("主机%d转为follower-投票请求回复处\n",rf.me)
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
	args.LastLogIndex = rf.logs[len(rf.logs)-1].Index
	args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
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

// 检查是否是leader，如果是leader则向其它所有server发送心跳，如果不是leader则空转或time.sleep
func (rf *Raft) FollowerAction() {
	var wg sync.WaitGroup
	for {
		if rf.killed() {
			wg.Wait()
			return
		}
		if rf.state == Leader {
			time.Sleep(10 * time.Millisecond)
		} else {
			rf.mu.Lock()
			elapsed := time.Since(rf.lastMessageTime)
			if elapsed <= rf.ElectionTimeout {
				rf.mu.Unlock()
				// 未超时，短暂休眠后继续检查
				time.Sleep(10 * time.Millisecond)
			} else {
				if rf.state == Follower {
					DPrintf("超时！开始选举,当前是主机：%d\n", rf.me)
				} else {
					DPrintf("上一次选举失败，再次开始,当前是主机：%d\n", rf.me)
				}
				rf.state = Candidate
				rf.currentTerm = rf.currentTerm + 1
				rf.votedFor = rf.me
				rf.persist()
				rf.mu.Unlock()
				i := 0
				agreedCandidateNum := 1
				wg = sync.WaitGroup{}
				for i = 0; i < len(rf.peers); i++ {
					if rf.state != Candidate {
						break
					}
					if i == rf.me {
						continue
					}
					if rf.peers[i] != nil {
						DPrintf("主机：%d，在投票阶段调用wg.add（1）\n", rf.me)
						wg.Add(1)
						go rf.sendRequest(i, &agreedCandidateNum, &wg)
					}
				}
				wg.Wait()
				if agreedCandidateNum*2 <= len(rf.peers) && rf.state == Candidate {
					DPrintf("主机：%d,上一次选举失败,重新产生超时时间，\n失败原因agreedCandidateNum=%d, rf.state=%d", rf.me, agreedCandidateNum, rf.state)
					rf.lastMessageTime = time.Now()
					rf.ElectionTimeout = rf.randomTimeout()
				}
				if agreedCandidateNum*2 > len(rf.peers) && rf.state == Candidate {
					rf.mu.Lock()
					DPrintf("主机：%d成为leader,当前Term：%d\n", rf.me,rf.currentTerm)
					rf.state = Leader
					rf.logs = append(rf.logs, one_log{ Term: rf.currentTerm, Index: len(rf.logs), Committed: false})
					rf.committed = append(rf.committed, false)
					rf.persist()
					rf.mu.Unlock()
				}
			}
		}
	}
}
