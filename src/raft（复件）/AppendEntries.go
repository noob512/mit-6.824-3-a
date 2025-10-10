package raft

import (
	"context"
	"sync"
	"time"
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
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	//// 2A、2B 阶段实现：处理其他节点的投票请求
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("主机：%d,AppendEntries启动\n", rf.me)
	rf.updateTime()
	
	if rf.currentTerm <= args.Term {
		if rf.currentTerm < args.Term {
			rf.votedFor = -1
		}
		if rf.state==Leader{
			rf.state = Follower
			rf.turnToLeader = 0
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			reply.Success=false
			reply.ConflictIndex=args.PrevLogIndex+1
			DPrintf("主机%d转为follower-心跳接收处\n", rf.me)
			return
		}
		rf.state = Follower
		rf.turnToLeader = 0
		DPrintf("主机%d保持follower-心跳接收处\n", rf.me)
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
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
				}else{
					for i >= 0 && rf.logs[i].Term == args.PrevLogTerm {
						i--
					}
					reply.ConflictIndex = i + 1
				}
				reply.ConflictTerm=rf.logs[args.PrevLogIndex].Term
			}
			reply.Success = false
		} else {
			reply.Success = true
			if args.Entries != nil {
				rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
				DPrintf("主机：%d 日志更新,当前日志内容为%v,当前term为%d\n", rf.me, rf.logs,rf.currentTerm)
				rf.lastLogIndex = len(rf.logs)
				rf.persist()
			} else {
				DPrintf("主机：%d收到空心跳信息,当前日志内容为%v，当前term为%d\n", rf.me, rf.logs,rf.currentTerm)
			}
			if args.LeaderCommit > rf.commitIndex {
				for rf.commitIndex < min(args.LeaderCommit, len(rf.logs)-1) {
					rf.commitIndex++
					newApplyMsg := new(ApplyMsg)
					newApplyMsg.Command = rf.logs[rf.commitIndex].Cmd
					newApplyMsg.CommandIndex = rf.commitIndex
					newApplyMsg.CommandValid = true
					rf.applyCh <- *newApplyMsg
					DPrintf("主机：%d中索引为%d的日志提交完成", rf.me, rf.commitIndex)
				}

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

func (rf *Raft) HandleAppendEntriesReply(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("主机：%d,HandleAppendEntriesReply启动\n", rf.me)
	//在日志一样的情况下返回了false，代表对面任期更高
	if reply.Success == false&&rf.state==Leader {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower // 若跟随者任期更高，当前节点应转为跟随者
			rf.turnToLeader = 0
			DPrintf("主机%d转为follower-心跳回复处\n", rf.me)
			rf.votedFor = -1
			rf.updateTime()
		} else {
			if reply.ConflictTerm==-1{
				rf.nextIndex[i] = reply.ConflictIndex
			}else if reply.ConflictTerm>args.PrevLogTerm{
				rf.nextIndex[i] = reply.ConflictIndex
			}else if reply.ConflictTerm<args.PrevLogTerm{
				pos := args.PrevLogIndex
				for pos >= 0 && rf.logs[pos].Term != reply.ConflictTerm {
						pos--
					}
				rf.nextIndex[i] = pos + 1
			}
		}
	} else if reply.Success == true&&rf.state==Leader{
		rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
		DPrintf("Leader:%d 更新matchIndex[%d]为%d 更新nextIndex[%d]为%d", rf.me, i, rf.matchIndex[i], i, rf.nextIndex[i])
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
	DPrintf("主机：%d send AppendEntries RPC to %d begin\n且日志内容中PrevLogIndex: %d,PrevLogTerm: %d,LeaderCommit:%d,Entries: %v", rf.me, i, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)

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

func (rf *Raft) checkCommit() {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			continue
		} else {
			numsCommit := 0
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					numsCommit++
					continue
				} else if rf.matchIndex[i] > rf.commitIndex || rf.commitIndex == -1 {
					numsCommit++
				}
			}
			if numsCommit*2 > len(rf.peers) && rf.state == Leader && rf.commitIndex < len(rf.logs) {
				rf.commitIndex++
				newApplymsg := new(ApplyMsg)
				newApplymsg.Command = rf.logs[rf.commitIndex].Cmd
				newApplymsg.CommandIndex = rf.commitIndex
				newApplymsg.CommandValid = true
				rf.applyCh <- *newApplymsg
				DPrintf("主机：%d中索引为%d的日志提交完成", rf.me, rf.commitIndex)
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func (rf *Raft) initLeader() {
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs)
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}
	rf.turnToLeader = 1
	DPrintf("主机：%d  initLeader完成", rf.me)
}

// 检查是否是leader，如果是leader则向其它所有server发送心跳，如果不是leader则空转或time.sleep
func (rf *Raft) LeaderAction() {
	var wg sync.WaitGroup
	for {
		if rf.killed() {
			wg.Wait()
			return
		}
		//不是领导者则休眠
		if rf.state != Leader {
			time.Sleep(10 * time.Millisecond)
		} else {
			if rf.turnToLeader == 0 {
				rf.initLeader()
			}
			go rf.checkCommit()
			i := 0
			wg = sync.WaitGroup{}
			for i = 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.state != Leader {
					break
				}
				if rf.peers[i] != nil {
					//是领导者就通过协程并发发送
					DPrintf("主机：%d，在心跳阶段调用wg.add（1）\n", rf.me)
					wg.Add(1)
					go rf.sendEntries(i, &wg)
				}
			}
			wg.Wait()
			time.Sleep(HeartbeatInterval)
		}
	}
}
