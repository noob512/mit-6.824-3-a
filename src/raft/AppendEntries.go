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
	ConflictIndex int //将其定义为出现差错的index
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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
			rf.mu.Lock()     // 锁住内部状态以读取日志和索引
			rf.lastApplied++ // 递增应用索引

			// 情况 A：如果该日志已经被标记为提交过且存在 Cmd，跳过（幂等性）
			if rf.committed[rf.lastApplied] == true && rf.logs[rf.lastApplied].Cmd != nil {
				rf.persist() // 立即持久化状态变化
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

// InstallSnapshot 是 Follower 处理 Leader 发来的快照 RPC 的处理函数。
// 当 Leader 发现 Follower 落后太多，且需要的日志已经被 Leader 自身的快照丢弃时，会调用此 RPC。
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// 加锁保护 Raft 内部状态
	rf.mu.Lock()

	// 默认回复当前的 Term
	reply.Term = rf.currentTerm

	// 【规则 1：旧任期拒绝】
	// 如果 Leader 的 Term 比自己的小，说明这是一个过期的 Leader，直接拒绝并返回。
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	// 【规则 2：新任期服从】
	// 如果 Leader 的 Term 比自己大，立刻转变为 Follower 并更新自己的 Term。
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.turnToLeader = 0 // 清除之前可能当选 Leader 的标志位
		// 唤醒后台正在睡眠的 checkCommit 协程，让它尽早发现自己不再是 Leader 并退出
		if rf.commitCond != nil {
			rf.commitCond.Broadcast()
		}
	}

	// 既然收到了合法 Leader 的快照，说明 Leader 存活，重置自己的选举超时定时器
	rf.updateTime()
	// 更新回复的 Term 为最新的 Term
	reply.Term = rf.currentTerm

	// 【防御性编程：旧快照拦截】
	// 如果收到的快照所包含的最后索引，甚至比自己当前的快照索引还要老（或相等），
	// 说明这是一个网络延迟导致的过期 RPC，或者自己已经通过其他方式有了更新的快照。
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		// 持久化当前状态（因为上面可能更新了 currentTerm），然后直接忽略该快照
		rf.persist()
		rf.mu.Unlock()
		return
	}

	// 记录旧的快照边界，用于下面计算偏移量
	oldLastIncludedIndex := rf.lastIncludedIndex
	// 计算自己当前拥有的最后一条日志的绝对索引
	lastLogIndex := rf.lastIncludedIndex + len(rf.logs) - 1

	// ==========================================
	// 核心逻辑：日志裁剪与合并 (对应论文图13的第6步)
	// ==========================================

	// 如果本地日志的长度能够覆盖快照的最后一条日志 (即没有比快照更短)
	if args.LastIncludedIndex <= lastLogIndex {
		// 计算快照截断点在当前日志数组中的相对下标
		offset := args.LastIncludedIndex - oldLastIncludedIndex

		// 如果下标合法，并且在这个截断点上，本地日志的 Term 与快照的 Term 完全一致。
		// 这意味着从 offset+1 开始的后续日志仍然是有效的，我们需要保留它们（保留尾巴）。
		if offset >= 0 &&
			offset < len(rf.logs) &&
			rf.logs[offset].Term == args.LastIncludedTerm {

			// 深拷贝：创建一个新的切片，保留哨兵节点和有效的尾部日志
			newLogs := make([]one_log, 0)
			newLogs = append(newLogs, one_log{
				Term: args.LastIncludedTerm,
				Cmd:  nil,
			})
			newLogs = append(newLogs, rf.logs[offset+1:]...)
			rf.logs = newLogs
		} else {
			// 如果 Term 发生冲突，说明前面的日志全部失效。
			// 丢弃整个日志，只保留一个哨兵节点。
			rf.logs = []one_log{{
				Term: args.LastIncludedTerm,
				Cmd:  nil,
			}}
		}
	} else {
		// 如果本地日志比快照还要短，那本地日志全都是没用的，
		// 直接清空，并用快照的元数据初始化哨兵节点。
		rf.logs = []one_log{{
			Term: args.LastIncludedTerm,
			Cmd:  nil,
		}}
	}

	// 【更新 Raft 状态】
	// 将自己的快照元数据推进到 Leader 传来的进度
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	// 既然快照到了 LastIncludedIndex，说明此前的日志绝对已经被提交(Committed)且应用(Applied)了。
	// 强行推进 commitIndex 和 lastApplied，防止倒退。
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}

	// 【原子持久化】
	// 将更新后的 Raft 状态（Term, voteFor, 新的 logs）和真实的快照二进制数据一同写入磁盘。
	// 注意：rf.persistData() 应该是你实现的一个将当前状态序列化为 []byte 的函数。
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)

	// ==========================================
	// 核心逻辑：通知上层状态机 (kvserver)
	// ==========================================
	// 构造发给上层的消息，标记为快照类型
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	// 【极其关键的一步：先解锁，再发送】
	// 如果在持有 rf.mu 锁的情况下去操作 channel，而上层的 kvserver 碰巧正在阻塞处理其他事情，
	// 就会导致整个 Raft 节点死锁。
	rf.mu.Unlock()

	// 开启一个后台协程，将快照推送给 kvserver。
	// kvserver 收到后会清空自己的内存数据库，并用这个快照的数据进行覆盖恢复。
	go func() {
		rf.applyCh <- applyMsg
	}()
}

// // AppendEntries 是 Follower 或 Candidate 接收 Leader 发来的心跳或日志追加请求的 RPC 处理器。
// func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
//     // 1. 获取互斥锁，保护节点状态，并在函数退出时自动释放
//     rf.mu.Lock()
//     defer func() {
//         rf.mu.Unlock()
//         DPrintf("主机：%d处理完心跳即将退出\n", rf.me)
//     }()

//     DPrintf("主机：%d,AppendEntries启动,当前日志为%v\n", rf.me, rf.logs)

//     // 2. 重置选举超时计时器
//     // ⚠️ 注意：标准的 Raft 论文建议仅在 args.Term >= rf.currentTerm 时才重置定时器。
//     // 如果无脑重置，可能会导致被旧 Leader 的网络延迟包意外打断当前正常的选举。
//     rf.updateTime()

//     // ==========================================
//     // 阶段 1：任期校验与身份降级
//     // ==========================================
//     if rf.currentTerm <= args.Term {
//         // 如果对方的任期严格大于自己，说明自己已经“过气”
//         if rf.currentTerm < args.Term {
//             rf.votedFor = -1 // 清空曾经的投票记录，准备迎接新时代
//             rf.persist()     // 状态改变，落盘持久化
//         }

//         // 【特殊逻辑】：如果当前节点自认为是 Leader，但收到了等于或高于自己任期的合法 AppendEntries
//         if rf.state == Leader {
//             rf.state = Follower // 乖乖退位
//             rf.turnToLeader = 0

//             if args.Term > rf.currentTerm {
//                 rf.currentTerm = args.Term
//                 rf.persist()
//             }
//             reply.Term = rf.currentTerm
//             reply.Success = false
//             // ⚠️ 潜在缺陷：退位后直接拒绝了本次追加（ConflictIndex = PrevLogIndex+1）并 return。
//             // 在标准 Raft 中，退位后通常会继续往下走，直接处理这段合法 Leader 发来的日志，
//             // 否则 Leader 还需要多发一次 RPC 才能同步数据。
//             reply.ConflictIndex = args.PrevLogIndex + 1
//             DPrintf("主机%d转为follower-心跳接收处\n", rf.me)
//             return
//         }

//         // 如果当前是 Follower 或 Candidate，全面认怂，认同该 Leader
//         rf.state = Follower
//         rf.turnToLeader = 0
//         DPrintf("主机%d保持follower-心跳接收处,当前commitIndex：%d\n", rf.me, rf.commitIndex)
//         rf.currentTerm = args.Term
//         reply.Term = rf.currentTerm
//         rf.persist()

//         // // ⚠️ 防御性检查（非标准但有用的防御机制）：防止落后的 Leader 强行覆盖已提交的进度
//         // if rf.commitIndex > args.LeaderCommit {
//         //     reply.Success = false
//         //     reply.ConflictIndex = args.PrevLogIndex + 1
//         //     return
//         // }

//         // ==========================================
//         // 阶段 2：日志一致性检查与快速回退 (Fast Backup)
//         // ==========================================
//         // 规则：如果 PrevLogIndex 超出了本地日志长度，或者该位置的 Term 与 Leader 宣称的不一致，则追加失败。
//         if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {

//             // Case 1: 本地日志太短，根本没有 PrevLogIndex 这个位置的日志
//             if args.PrevLogIndex >= len(rf.logs) {
//                 DPrintf("主机：%d 日志存在问题 因为args.PrevLogIndex是%d但是len(rf.logs)为%d\n", rf.me, args.PrevLogIndex, len(rf.logs))
//                 reply.ConflictIndex = len(rf.logs) // 告诉 Leader："我只有这么长，你从这里开始发"
//                 reply.ConflictTerm = -1

//             // Case 2: 长度够，但是在 PrevLogIndex 处的任期冲突了 (发生了网络分区或换届导致的分支)
//             } else {
//                 DPrintf("主机：%d 日志存在问题 因为rf.logs[args.PrevLogIndex].Term是%d但是args.PrevLogTerm为%d\n", rf.me, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)
//                 i := args.PrevLogIndex

//                 // 【快速回退算法】：试图一次性跳过一整个冲突的 Term，而不是每次 RPC 只往前退一个 Index
//                 if rf.logs[args.PrevLogIndex].Term < args.PrevLogTerm {
//                     // 本地任期比 Leader 的小（较少见，通常是因为被旧 Leader 截断过）
//                     reply.ConflictIndex = args.PrevLogIndex - 1
//                 } else if rf.logs[args.PrevLogIndex].Term > args.PrevLogTerm {
//                     // 本地任期比 Leader 的大（这是典型的拥有未提交的“脏日志”）
//                     // 往前遍历，找到属于当前冲突 Term 的第一条日志的起始位置
//                     for i >= 0 && rf.logs[i].Term == rf.logs[args.PrevLogIndex].Term {
//                         i--
//                     }
//                     reply.ConflictIndex = i + 1 // 告诉 Leader："从我这个冲突 Term 的开头重新覆盖吧"
//                 }
//                 reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
//             }
//             DPrintf("主机：%d reply内容为%v\n", rf.me, reply)
//             reply.Success = false

//         // ==========================================
//         // 阶段 3：匹配成功，执行日志追加与提交
//         // ==========================================
//         } else {
//             reply.Success = true

//             // 如果 Leader 传来了新的日志条目 (非空心跳)
//             if args.Entries != nil {
//                 // 核心追加逻辑：保留本地日志 0 到 PrevLogIndex 的部分（因为这部分已验证匹配），
//                 // 然后强行拼接上 Leader 发来的全部新日志。这会隐式地截断本地之后所有的脏日志。
//                 rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
//                 DPrintf("主机：%d 日志更新,当前日志内容为%v,当前term为%d\n", rf.me, rf.logs, rf.currentTerm)

//                 // 扩容自定义的 committed 状态数组（如果你在上层状态机需要用到）
//                 if len(rf.committed) < len(rf.logs) {
//                     extra := make([]bool, len(rf.logs)-len(rf.committed))
//                     rf.committed = append(rf.committed, extra...)
//                 }

//                 rf.lastLogIndex = len(rf.logs)
//                 rf.persist() // 🌟 日志发生了变化，必须立刻持久化落盘
//             } else {
//                 DPrintf("主机：%d收到空心跳信息,当前日志内容为%v，当前term为%d\n", rf.me, rf.logs, rf.currentTerm)
//             }

//             // 推进 commitIndex
//             // 根据 Raft 论文，Follower 的 commitIndex 取决于 Leader 宣称的 commitIndex
//             // 和本地最新日志长度中的较小值（防止超前提交尚未收到的日志）
//             if args.LeaderCommit > rf.commitIndex {
//                 rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)

//                 // 💡 通常在这里还需要通过 Cond.Broadcast 唤醒 applyLog 协程，
//                 // 从而把 commitIndex 范围内的新日志推送到状态机层 (kv.applyCh)！
//             }
//         }

//     // ==========================================
//     // 阶段 4：拒绝过期 Leader
//     // ==========================================
//     } else {
//         // 如果对方的 Term 比自己还小，说明对方是个网络延迟的“前朝遗老”
//         reply.Success = false
//         reply.Term = rf.currentTerm // 返回自己更高的 Term，促使对方立刻降级
//     }
// }

// AppendEntries 是 Follower 或 Candidate 接收 Leader 发来的心跳或日志追加请求的 RPC 处理器。
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 1. 获取互斥锁，保护节点状态，并在函数退出时自动释放
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		DPrintf("主机：%d处理完心跳即将退出\n", rf.me)
	}()

	DPrintf("主机：%d,AppendEntries启动,当前日志为%v\n", rf.me, rf.logs)

	// 2. 重置选举超时计时器
	// ⚠️ 注意：标准的 Raft 论文建议仅在 args.Term >= rf.currentTerm 时才重置定时器。
	// 如果无脑重置，可能会导致被旧 Leader 的网络延迟包意外打断当前正常的选举。
	rf.updateTime()

	// ==========================================
	// 阶段 1：任期校验与身份降级
	// ==========================================
	if rf.currentTerm <= args.Term {
		// 如果对方的任期严格大于自己，说明自己已经“过气”
		DPrintf("主机%d的term是%d,对方的term是%d\n", rf.me, rf.currentTerm, args.Term)
		if rf.currentTerm < args.Term {
			rf.votedFor = -1 // 清空曾经的投票记录，准备迎接新时代
			rf.persist()     // 状态改变，落盘持久化
		}

		// 【特殊逻辑】：如果当前节点自认为是 Leader，但收到了等于或高于自己任期的合法 AppendEntries
		if rf.state == Leader {
			rf.state = Follower // 乖乖退位
			rf.turnToLeader = 0
			// 唤醒后台正在睡眠的 checkCommit 协程，让它尽早发现自己不再是 Leader 并退出
			if rf.commitCond != nil {
				rf.commitCond.Broadcast()
			}
			if args.Term > rf.currentTerm {
				rf.currentTerm = args.Term
				rf.persist()
			}
			reply.Term = rf.currentTerm
			reply.Success = false
			// ⚠️ 潜在缺陷：退位后直接拒绝了本次追加（ConflictIndex = PrevLogIndex+1）并 return。
			// 在标准 Raft 中，退位后通常会继续往下走，直接处理这段合法 Leader 发来的日志，
			// 否则 Leader 还需要多发一次 RPC 才能同步数据。
			reply.ConflictIndex = args.PrevLogIndex + 1
			DPrintf("主机%d转为follower-心跳接收处\n", rf.me)
			return
		}

		// 如果当前是 Follower 或 Candidate，全面认怂，认同该 Leader
		rf.state = Follower
		rf.turnToLeader = 0
		DPrintf("主机%d保持follower-心跳接收处,当前commitIndex：%d\n", rf.me, rf.commitIndex)
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.persist()

		// ==========================================
		// 阶段 2：日志一致性检查与快速回退 (Fast Backup)
		// ==========================================
		// 规则：如果 PrevLogIndex 超出了本地日志长度，或者该位置的 Term 与 Leader 宣称的不一致，则追加失败。
		if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {

			// Case 1: 本地日志太短，根本没有 PrevLogIndex 这个位置的日志
			if args.PrevLogIndex >= len(rf.logs) {
				DPrintf("主机：%d 日志存在问题 因为args.PrevLogIndex是%d但是len(rf.logs)为%d\n", rf.me, args.PrevLogIndex, len(rf.logs))
				reply.ConflictIndex = len(rf.logs) // 告诉 Leader："我只有这么长，你从这里开始发"
				reply.ConflictTerm = -1

				// Case 2: 长度够，但是在 PrevLogIndex 处的任期冲突了 (发生了网络分区或换届导致的分支)
			} else {
				DPrintf("主机：%d 日志存在问题 因为rf.logs[args.PrevLogIndex].Term是%d但是args.PrevLogTerm为%d\n", rf.me, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)
				i := args.PrevLogIndex

				// 【快速回退算法】：试图一次性跳过一整个冲突的 Term，而不是每次 RPC 只往前退一个 Index
				if rf.logs[args.PrevLogIndex].Term < args.PrevLogTerm {
					// 本地任期比 Leader 的小（较少见，通常是因为被旧 Leader 截断过）
					reply.ConflictIndex = args.PrevLogIndex - 1
				} else if rf.logs[args.PrevLogIndex].Term > args.PrevLogTerm {
					// 本地任期比 Leader 的大（这是典型的拥有未提交的“脏日志”）
					// 往前遍历，找到属于当前冲突 Term 的第一条日志的起始位置
					for i >= 0 && rf.logs[i].Term == rf.logs[args.PrevLogIndex].Term {
						i--
					}
					reply.ConflictIndex = i + 1 // 告诉 Leader："从我这个冲突 Term 的开头重新覆盖吧"
				}
				reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
			}
			DPrintf("主机：%d reply内容为%v\n", rf.me, reply)
			reply.Success = false

			// ==========================================
			// 阶段 3：匹配成功，执行日志追加与提交
			// ==========================================
		} else {
			reply.Success = true

			// 如果 Leader 传来了新的日志条目 (非空心跳)
			if args.Entries != nil {
				// 🌟 安全追加算法：挨个比对，只有发生冲突时才截断，防止旧包抹杀新数据
				for i, entry := range args.Entries {
					idx := args.PrevLogIndex + 1 + i

					if idx < len(rf.logs) {
						// 情况 1：同一位置日志已存在。比对 Term。
						if rf.logs[idx].Term != entry.Term {
							// 发现 Term 冲突！说明从这开始是旧 Leader 的脏数据。
							// 仅从这个【冲突位置】开始截断覆盖，并拼接上剩余的新日志
							rf.logs = append(rf.logs[:idx], args.Entries[i:]...)
							break // 剩余日志已全部追加，跳出循环
						}
						// 💡 隐式逻辑：如果 Term 相同，说明是网络重传的、之前已经收过的合法包。
						// 什么都不做，保留原日志，继续往下检查下一个条目。

					} else {
						// 情况 2：超出了本地日志的长度，说明后面的全都是未接收过的新日志。
						// 直接把剩下的 args.Entries 一次性全拼接到末尾
						rf.logs = append(rf.logs, args.Entries[i:]...)
						break // 追加完毕，跳出循环
					}
				}

				DPrintf("主机：%d 日志更新,当前日志内容为%v,当前term为%d\n", rf.me, rf.logs, rf.currentTerm)

				// 扩容自定义的 committed 状态数组（保留你原有的逻辑）
				if len(rf.committed) < len(rf.logs) {
					extra := make([]bool, len(rf.logs)-len(rf.committed))
					rf.committed = append(rf.committed, extra...)
				}

				// ⚠️ 注意：上面原本是 rf.lastLogIndex = len(rf.logs)，为了严谨，最好改成减 1
				//rf.lastLogIndex = len(rf.logs) - 1
				rf.persist() // 🌟 日志发生了变化，立刻持久化落盘
			} else {
				DPrintf("主机：%d收到空心跳信息,当前日志内容为%v，当前term为%d\n", rf.me, rf.logs, rf.currentTerm)
			}

			// 推进 commitIndex
			// 根据 Raft 论文，Follower 的 commitIndex 取决于 Leader 宣称的 commitIndex
			// 和本地最新日志长度中的较小值（防止超前提交尚未收到的日志）
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)

				// 💡 通常在这里还需要通过 Cond.Broadcast 唤醒 applyLog 协程，
				// 从而把 commitIndex 范围内的新日志推送到状态机层 (kv.applyCh)！
			}
		}

		// ==========================================
		// 阶段 4：拒绝过期 Leader
		// ==========================================
	} else {
		// 如果对方的 Term 比自己还小，说明对方是个网络延迟的“前朝遗老”
		reply.Success = false
		reply.Term = rf.currentTerm // 返回自己更高的 Term，促使对方立刻降级
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// // HandleAppendEntriesReply 是 Leader 处理 Follower 返回的 AppendEntries 响应的回调函数。
// // 它的核心职责是：
// // 1. 检查自己是否已经“过气”（遇到更高的 Term），如果是则立即退位。
// // 2. 如果日志产生冲突，利用快速回退算法（Fast Backup）迅速定位冲突点，更新 nextIndex。
// // 3. 如果追加成功，更新 matchIndex 和 nextIndex，并唤醒后台协程去检查是否可以推进全局 commitIndex。
// func (rf *Raft) HandleAppendEntriesReply(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
//     rf.mu.Lock()
//     defer rf.mu.Unlock()
//     DPrintf("主机：%d, HandleAppendEntriesReply启动\n", rf.me)

//     // ==========================================
//     // 阶段 1：请求被拒绝 (Success == false)
//     // ==========================================
//     // 前置条件：当前节点必须还是 Leader（如果在等待 RPC 期间被降级了，就不该处理响应）
//     if reply.Success == false && rf.state == Leader {

//         // 场景 A：任期落后（大清亡了）
//         // 如果 Follower 返回的 Term 比 Leader 当前的 Term 还要大，
//         // 说明集群中已经有了新的 Leader，当前节点必须立刻无条件退位。
//         if reply.Term > rf.currentTerm {
//             rf.currentTerm = reply.Term
//             rf.state = Follower // 降级为跟随者
//             rf.turnToLeader = 0 // 重置新任 Leader 标志
//             DPrintf("主机%d转为follower-心跳回复处\n", rf.me)

//             rf.votedFor = -1    // 清空投票记录，准备可能的新一轮选举
//             rf.updateTime()     // 重置选举超时定时器，防止刚降级就立刻发起选举
//             rf.persist()        // 🌟 任期和投票记录改变，必须落盘持久化

//             // 🌟 唤醒后台的 checkCommit 协程
//             // 因为当前节点已经不是 Leader，需要唤醒正在 Wait() 的协程，
//             // 让它在下一轮循环检查状态时发现自己是 Follower 从而安全退出或挂起。
//             if rf.commitCond != nil {
//                 rf.commitCond.Broadcast()
//             }

//         // 场景 B：任期没问题，但是日志不匹配（Log Inconsistency）
//         // 此时触发 6.824 中极其重要的“快速回退 (Fast Backup)”逻辑，避免 nextIndex 一次只减 1 导致同步极慢。
//         } else {
//             // Case 1: Follower 那个位置根本没有日志（日志太短了）
//             // Follower 返回 ConflictTerm = -1，ConflictIndex 是它的日志总长度。
//             // Leader 直接把 nextIndex 降到 Follower 期望的那个位置。
//             if reply.ConflictTerm == -1 {
//                 rf.nextIndex[i] = reply.ConflictIndex

//             // Case 2 & 3: Follower 那个位置有日志，但是任期冲突了
//             } else if reply.ConflictTerm > args.PrevLogTerm {
//                 // 如果 Follower 的冲突任期比 Leader 发过去的 PrevLogTerm 还大（逻辑上较为罕见，通常做统一回退）
//                 // Leader 直接回退到 Follower 提供的该冲突任期的第一条日志位置。
//                 rf.nextIndex[i] = reply.ConflictIndex

//             } else if reply.ConflictTerm < args.PrevLogTerm {
//                 // 如果 Follower 的冲突任期小于 Leader 发过去的 PrevLogTerm。
//                 // Leader 需要在自己的日志中，从前往后倒推，试图找到这个 ConflictTerm 的最后一条日志。
//                 pos := args.PrevLogIndex
//                 for pos >= 0 && rf.logs[pos].Term != reply.ConflictTerm {
//                     pos--
//                 }
//                 // 如果找到了，或者遍历完了，就把 nextIndex 设为该 Term 的后一位
//                 rf.nextIndex[i] = pos + 1
//             }

//             // 兜底防御：由于 Raft 真实日志通常从下标 1 开始（下标 0 是哨兵 nil 节点），
//             // 严禁把 nextIndex 减到 0，否则后续发心跳时会数组越界。
//             if rf.nextIndex[i] == 0 {
//                 rf.nextIndex[i]++
//             }
//             DPrintf("Leader:%d 更新matchIndex[%d]为%d(其实没更新) 更新nextIndex[%d]为%d", rf.me, i, rf.matchIndex[i], i, rf.nextIndex[i])
//         }

//     // ==========================================
//     // 阶段 2：请求被接受 (Success == true)
//     // ==========================================
//     // 说明 Follower 完美匹配了 PrevLogIndex，并且成功把发过去的 Entries 记入了自己的日志。
//     } else if reply.Success == true && rf.state == Leader {
//         // 更新已经安全复制到的最高索引：传入的基准索引 + 这次打包发过去的日志数量
//         rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
//         // 更新下一次准备给该节点发日志的起点索引
//         rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1

//         DPrintf("Leader:%d 更新matchIndex[%d]为%d 更新nextIndex[%d]为%d", rf.me, i, rf.matchIndex[i], i, rf.nextIndex[i])

//         // 🌟 唤醒检查提交的后台协程
//         // 既然有一个节点成功跟上了最新的进度，这意味着极有可能已经凑够了多数派！
//         // 立刻 Broadcast 叫醒等待在条件变量上的 checkCommit 协程，让它去算选票、推 commitIndex。
//         rf.commitCond.Broadcast()

//         // 性能考量：虽然你加了 persist()，但实际上 matchIndex 和 nextIndex
//         // 属于 Leader 的 Volatile State（易失性状态），无需落盘。
//         // 若没有改变 currentTerm、votedFor 或 logs，这里可以不调用 persist() 以节省 IO。
//         rf.persist()
//     }
// }

// HandleAppendEntriesReply 处理从 Follower 返回的 AppendEntries 响应。
// i: Follower 的节点下标
// args: 发送时的原始请求参数
// reply: Follower 返回的响应结果
func (rf *Raft) HandleAppendEntriesReply(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 加锁保护并发读写
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("主机：%d, HandleAppendEntriesReply启动\n", rf.me)

	// 【防御性编程：过期 RPC 检查】
	// 如果发送请求时的 Term 和现在的 Term 不一致，说明在等待网络回复的这段时间里，
	// 当前节点经历了重新选举或状态变更，这个回复已经是过期的了，直接丢弃。
	if args.Term != rf.currentTerm {
		return
	}

	// ==========================================
	// 分支 1：Follower 拒绝了日志同步 (Success == false)
	// ==========================================
	if reply.Success == false && rf.state == Leader {

		// 场景 A：Follower 的任期比自己大
		if reply.Term > rf.currentTerm {
			// 说明集群中出现了新的 Leader，当前节点必须立刻“退位”
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.turnToLeader = 0
			DPrintf("主机%d转为follower-心跳回复处\n", rf.me)

			// 既然进入了新任期，重置投票记录
			rf.votedFor = -1
			rf.updateTime() // 重置选举超时定时器
			rf.persist()    // Term 和 votedFor 发生了改变，必须落盘持久化

			// 唤醒后台正在睡眠的 checkCommit 协程，让它尽早发现自己不再是 Leader 并退出
			if rf.commitCond != nil {
				rf.commitCond.Broadcast()
			}

		} else {
			// 场景 B：任期没问题，但是发生了【日志一致性冲突】
			// 这里实现了 6.824 学生指南中的 Fast Backup (快速回退) 算法，避免 nextIndex 一次只减 1

			if reply.ConflictTerm == -1 {
				// 情况 1：Follower 在 PrevLogIndex 处根本就没有日志 (日志太短)。
				// Follower 会把它的日志总长度作为 ConflictIndex 返回。
				// Leader 直接把 nextIndex 回退到 Follower 期望的 ConflictIndex。
				rf.nextIndex[i] = reply.ConflictIndex

			} else if reply.ConflictTerm > args.PrevLogTerm {
				// 理论上标准的 Fast Backup 较少触发这个分支，
				// 这通常意味着 Follower 在冲突点的 Term 比 Leader 还要大（或者逻辑上有偏差）。
				// 保守起见，直接退到 Follower 提供的 ConflictIndex。
				rf.nextIndex[i] = reply.ConflictIndex

			} else if reply.ConflictTerm < args.PrevLogTerm {
				// 情况 2：Follower 在 PrevLogIndex 处有日志，但是任期冲突。
				// Leader 尝试在自己的日志中寻找 ConflictTerm 的【最后一条日志】。
				pos := args.PrevLogIndex

				// 向前遍历 Leader 自己的日志，注意不能越过快照截断点 (lastIncludedIndex)
				for pos > rf.lastIncludedIndex &&
					rf.logs[pos-rf.lastIncludedIndex].Term != reply.ConflictTerm {
					pos--
				}

				// 如果遍历到了快照边界，且快照边界的 Term 也不是我们要找的 ConflictTerm
				// 说明 Leader 自身已经没有该 ConflictTerm 的任何日志了。
				if pos == rf.lastIncludedIndex &&
					rf.lastIncludedTerm != reply.ConflictTerm {
					// 只能直接跨过整个 Term，回退到 Follower 提供的该 Term 的第一条日志的索引
					rf.nextIndex[i] = reply.ConflictIndex
				} else {
					// 找到了 Leader 日志中 ConflictTerm 的最后一条日志！
					// 下次发日志就从它的下一条开始发，这样能快速跳过大量冲突的日志
					rf.nextIndex[i] = pos + 1
				}
			}

			// 【 Lab 3B 核心防护：快照边界限制】
			// 经过上述快速回退，nextIndex 可能变得非常小。
			// 如果退到了快照截断点及之前，说明 Follower 需要的日志已经被 Leader 删了。
			// 这里将 nextIndex 锁定在 lastIncludedIndex，
			// 这样在下一次发心跳时，sendEntries 函数就会触发 InstallSnapshot RPC。
			if rf.nextIndex[i] < rf.lastIncludedIndex+1 {
				rf.nextIndex[i] = rf.lastIncludedIndex
			}

			DPrintf("Leader:%d 更新matchIndex[%d]为%d(其实没更新) 更新nextIndex[%d]为%d",
				rf.me, i, rf.matchIndex[i], i, rf.nextIndex[i])
		}

		// ==========================================
		// 分支 2：Follower 成功接收并追加了日志 (Success == true)
		// ==========================================
	} else if reply.Success == true && rf.state == Leader {
		// 追加成功，更新已匹配的索引 (matchIndex) 和 下次发送的索引 (nextIndex)
		// 注意：不要使用 len(rf.logs) 来更新，因为请求在网络中飞行时，Leader 可能又接收了新日志。
		// 必须基于本次 RPC 【发送成功的数据量】来更新。
		rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1

		DPrintf("Leader:%d 更新matchIndex[%d]为%d 更新nextIndex[%d]为%d",
			rf.me, i, rf.matchIndex[i], i, rf.nextIndex[i])

		// matchIndex 发生了推进，立刻唤醒后台的 checkCommit 协程，
		// 检查是否已经复制到了多数派，从而可以推进 commitIndex。
		rf.commitCond.Broadcast()

		// （见下方代码审查：这里调用 persist() 是一个性能拖累）
		rf.persist()
	}
}

// 1. 重构sendRequest函数：加入context超时控制
// func (rf *Raft) sendEntries(i int, wg *sync.WaitGroup,term int) {
// 	// 必须在函数开头调用wg.Done()，确保无论是否超时，WaitGroup计数都会递减
// 	defer wg.Done()
// 	reply := new(AppendEntriesReply)
// 	args := new(AppendEntriesArgs)
// 	args.Term = term
// 	args.LeaderId = rf.me
// 	args.PrevLogIndex = rf.nextIndex[i] - 1
// 	//DPrintf("rf.logs的长度为%d,args.PrevLogIndex是%d\n",len(rf.logs),args.PrevLogIndex)
// 	args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
// 	args.Entries = rf.logs[rf.nextIndex[i]:]
// 	args.LeaderCommit = rf.commitIndex
// 	if rf.nextIndex[i] < len(rf.logs) {
// 		args.Entries = rf.logs[rf.nextIndex[i]:]
// 	}
// 	args.LeaderCommit = rf.commitIndex
// 	DPrintf("主机：%d send AppendEntries RPC to %d begin,自身term=%d,日志为%v\n且日志内容中PrevLogIndex: %d,PrevLogTerm: %d,LeaderCommit:%d,Entries: %v", rf.me, i,rf.currentTerm,rf.logs, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
// 	//DPrintf("主机：%d send AppendEntries RPC to %d begin,args: %v", rf.me, i,args)

// 	// 2. 创建20ms超时的上下文：ctx用于控制超时，cancel用于主动取消（需defer避免泄漏）
// 	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
// 	defer cancel() // 确保函数退出时释放context资源，避免内存泄漏

// 	// 3. 创建一个通道：用于接收RPC的执行结果（成功/失败）
// 	rpcResult := make(chan bool, 1) // 缓冲通道，避免协程阻塞

// 	// 4. 启动子协程：执行RPC发送逻辑（将RPC与超时检测解耦）
// 	go func() {
// 		// 执行RPC发送（这是可能耗时的操作）
// 		ok := rf.sendAppendEntries(i, args, reply)
// 		// 将RPC结果发送到通道（即使超时，发送也不会阻塞，因为通道有缓冲）
// 		rpcResult <- ok
// 	}()

// 	// 5. 超时检测与结果处理：监听ctx.Done()（超时信号）或rpcResult（RPC结果）
// 	select {
// 	case ok := <-rpcResult:
// 		// case1：RPC在20ms内完成，处理结果
// 		if !ok {
// 			DPrintf("主机：%d,AppendEntries RPC action send to %d fail\n", rf.me, i)
// 		} else {
// 			DPrintf("主机：%d,AppendEntries RPC action send to %d finished\n", rf.me, i)
// 			// 处理投票回复（需加锁保护共享变量agreedCandidateNum）
// 			rf.HandleAppendEntriesReply(i, args, reply)
// 		}

// 	case <-ctx.Done():
// 		// case2：20ms超时，ctx.Done()通道被触发（返回超时原因）
// 		DPrintf("主机：%d send to %d AppendEntries RPC timeout (exceed 20ms), force exit\n", rf.me, i)
// 		// 超时后主动退出，不执行后续逻辑（相当于“强行结束”该协程的有效逻辑）
// 		return
// 	}
// }

// sendEntries 是 Leader 为单个 Follower (下标为 i) 同步日志或发送快照的函数。
// 通常作为 Goroutine 并发运行。wg 用于通知主调用协程该发送任务已结束。
func (rf *Raft) sendEntries(i int, wg *sync.WaitGroup, term int) {
	// 确保退出时调用 Done()，释放 waitGroup 计数，防止死锁
	defer wg.Done()

	// 加锁读取当前 Raft 状态
	rf.mu.Lock()

	// 【状态检查】
	// 如果自己已经不是 Leader，或者由于网络延迟当前的 term 发生了改变，或者节点已死，
	// 说明当前的发送任务已经失去意义，直接解锁并退出。
	if rf.state != Leader || term != rf.currentTerm || rf.killed() {
		rf.mu.Unlock()
		return
	}

	// ==========================================
	// 分支 1：发送快照 (InstallSnapshot)
	// ==========================================
	// 如果 Leader 发现打算发给 Follower 的下一条日志 (nextIndex)
	// 已经被自己打包成快照删除了 (<= lastIncludedIndex)，就只能发送整个快照。
	if rf.nextIndex[i] <= rf.lastIncludedIndex {
		args := &InstallSnapshotArgs{
			Term:              term,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.persister.ReadSnapshot(), // 从底层的持久化存储读取真实的快照二进制数据
		}
		reply := &InstallSnapshotReply{}

		// 准备好请求参数后立刻解锁，绝对不要拿着锁进行网络 RPC 调用！
		rf.mu.Unlock()

		// 【RPC 超时控制】设置 50ms 的上下文超时，防止 RPC 死锁或长时间阻塞
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		rpcResult := make(chan bool, 1)

		// 在后台开启协程进行真实的 RPC 发送
		go func() {
			ok := rf.sendInstallSnapshot(i, args, reply)
			rpcResult <- ok
		}()

		// select 监听 RPC 的结果或者超时信号
		select {
		case ok := <-rpcResult:
			// 如果网络本身出现问题（断开、拒绝连接）
			if !ok {
				DPrintf("主机：%d,InstallSnapshot RPC action send to %d fail\n", rf.me, i)
				return
			}

			// RPC 成功返回，重新加锁处理回复状态
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 醒来后再次检查状态，防并发篡改
			if term != rf.currentTerm || rf.state != Leader {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower // 降级为跟随者
				rf.turnToLeader = 0 // 重置新任 Leader 标志
				DPrintf("主机%d转为follower-发送快照心跳回复处\n", rf.me)

				rf.votedFor = -1 // 清空投票记录，准备可能的新一轮选举
				rf.updateTime()  // 重置选举超时定时器，防止刚降级就立刻发起选举
				rf.persist()     // 🌟 任期和投票记录改变，必须落盘持久化

				// 🌟 唤醒后台的 checkCommit 协程
				// 因为当前节点已经不是 Leader，需要唤醒正在 Wait() 的协程，
				// 让它在下一轮循环检查状态时发现自己是 Follower 从而安全退出或挂起。
				if rf.commitCond != nil {
					rf.commitCond.Broadcast()
				}
				return
			}

			// 【状态更新】
			// 快照发送成功！将该 Follower 的匹配点和下一次发送点强制对齐到快照末尾。
			rf.matchIndex[i] = args.LastIncludedIndex
			rf.nextIndex[i] = args.LastIncludedIndex + 1

		case <-ctx.Done():
			// 50ms 内没收到回复，直接放弃本次操作。下一轮心跳会重新触发发送逻辑。
			DPrintf("主机：%d send to %d InstallSnapshot RPC timeout\n", rf.me, i)
			return
		}

		// 发送完快照直接返回，不走下面的 AppendEntries 逻辑
		return
	}

	// ==========================================
	// 分支 2：发送普通日志记录 (AppendEntries)
	// ==========================================
	reply := new(AppendEntriesReply)
	args := new(AppendEntriesArgs)

	// 【核心逻辑：相对索引的计算】
	nextIndex := rf.nextIndex[i]
	prevLogIndex := nextIndex - 1 // 发送的新日志的前一条日志的全局索引
	// 将全局索引转换为当前在内存中的 rf.logs 数组的具体下标（偏移量）
	prevLogOffset := prevLogIndex - rf.lastIncludedIndex

	args.Term = term
	args.LeaderId = rf.me
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = rf.logs[prevLogOffset].Term
	args.LeaderCommit = rf.commitIndex

	// 计算当前内存中最新日志的全局绝对索引
	lastLogIndex := rf.lastIncludedIndex + len(rf.logs) - 1

	// 如果存在要发送的日志（即 nextIndex 还在 Leader 当前的日志范围内）
	if nextIndex <= lastLogIndex {
		// 计算新日志在当前切片里的相对起始下标
		entryOffset := nextIndex - rf.lastIncludedIndex
		// 将从 entryOffset 到末尾的所有日志切片，追加到 RPC 参数中。
		// 注意：网络框架在发送时会自动序列化这个切片，不需要担心浅拷贝影响原始切片。
		args.Entries = append(args.Entries, rf.logs[entryOffset:]...)
	}

	DPrintf("主机：%d send AppendEntries RPC to %d begin,自身term=%d,日志为%v\n且日志内容中PrevLogIndex: %d,PrevLogTerm: %d,LeaderCommit:%d,Entries: %v",
		rf.me, i, rf.currentTerm, rf.logs, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)

	// 解锁，准备进行网络调用
	rf.mu.Unlock()

	// 同样使用 50ms 超时控制防止网络卡死
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	rpcResult := make(chan bool, 1)

	go func() {
		ok := rf.sendAppendEntries(i, args, reply)
		rpcResult <- ok
	}()

	select {
	case ok := <-rpcResult:
		if !ok {
			DPrintf("主机：%d,AppendEntries RPC action send to %d fail\n", rf.me, i)
		} else {
			DPrintf("主机：%d,AppendEntries RPC action send to %d finished\n", rf.me, i)
			// RPC 收到响应后，交由专门的 handler 去处理 nextIndex 递减或推进、
			// 推进 commitIndex 等复杂逻辑。
			rf.HandleAppendEntriesReply(i, args, reply)
		}

	case <-ctx.Done():
		DPrintf("主机：%d send to %d AppendEntries RPC timeout (exceed 50ms), force exit\n", rf.me, i)
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
// func (rf *Raft) checkCommit() {
//     DPrintf("主机：%d启动checkCommit\n", rf.me)
//     // ❌ 架构缺陷：死循环轮询。这会将本应由事件驱动的逻辑转变为无休止的后台消耗。
//     for {
//         rf.mu.Lock() // 获取锁，准备读取核心状态

//         // 1. 身份校验：只有 Leader 才有资格推进全局的 commitIndex
//         if rf.state != Leader||rf.killed() {
//             rf.mu.Unlock() // 不是 Leader，立即释放锁
//             // ❌ 架构缺陷：无效的睡眠等待。如果节点不是 Leader，这个协程应该直接 return 退出，
//             // 而不是永远睡在这里浪费资源。
//             DPrintf("主机：%d退出checkCommit\n", rf.me)
// 			return
//             // time.Sleep(20 * time.Millisecond)
//             // continue
//         } else {
//             // 2. 核心推进逻辑：根据 Raft 论文的 Rules for Servers (Leader) 5.3 & 5.4 节
//             // 从最新的日志索引开始，从后往前遍历，寻找满足多数派复制条件的最高索引 N。
//             // 遍历范围：[len(rf.logs)-1, rf.commitIndex)
//             for N := len(rf.logs) - 1; N > rf.commitIndex; N-- {

//                 // 3. 关键安全性规则 (论文 Figure 8)：
//                 // Leader 只能提交**当前任期**内创建的日志条目。
//                 // 如果发现某条日志的任期不是当前任期，不能直接通过计算副本来提交它
//                 // （必须等到当前任期的一条新日志被提交，才能隐式地将旧任期的日志一起提交）。
//                 if rf.logs[N].Term != rf.currentTerm {
//                     // 这里使用 break 是因为从后往前遍历，如果遇到旧任期日志，
//                     // 说明当前任期还没有日志被安全复制（或者遍历已经越过了当前任期的边界），
//                     // 因此无需继续向前检查，直接跳出。
//                     break
//                 }

//                 numsCommit := 0 // 计数器：记录有多少个节点已经复制了索引至少为 N 的日志

//                 // 4. 统计副本数：遍历集群中的所有节点
//                 for i := 0; i < len(rf.peers); i++ {
//                     if i == rf.me {
//                         // Leader 本身必定拥有该日志，直接算作一票
//                         numsCommit++
//                         continue
//                     } else if rf.matchIndex[i] >= N {
//                         // 如果节点 i 的 matchIndex 大于等于 N，说明它已经复制了这条日志
//                         numsCommit++
//                     }
//                 }

//                 // 5. 多数派判定：如果已复制的节点数超过集群半数（包括自己）
//                 if numsCommit*2 > len(rf.peers) && rf.state == Leader {
//                     // 更新 Leader 的 commitIndex 到 N
//                     // 此时，后台的 applyLog 协程一旦发现 commitIndex > lastApplied，
//                     // 就会立刻开始将日志推送给 KVServer 状态机。
//                     rf.commitIndex = N

//                     // 找到最高的可提交索引后即可跳出（因为更小的索引自然也被多数派复制了）
//                     break
//                 }
//             }
//             // 2. 带着锁直接调用 Wait()！
//             // Wait 会自动帮你 Unlock，醒来时会自动帮你重新 Lock。
//             rf.commitCond.Wait()

//             rf.mu.Unlock() // 3. Wait 结束后释放锁，进入下一轮循环
//         }
//     }
// }

// checkCommit 是一个在 Leader 当选后通常以后台协程 (goroutine) 运行的函数。
// 它的职责是：不断检查 matchIndex 数组，看是否有一条当前任期的新日志已经被复制到了多数派节点，
// 如果是，则推进 Leader 的 commitIndex。
func (rf *Raft) checkCommit() {
	DPrintf("主机：%d启动checkCommit\n", rf.me)

	// 无限循环，作为一个长期的守护协程运行
	for {
		// 加锁，因为我们要读取和修改大量 Raft 核心状态
		rf.mu.Lock()

		// 【退出条件】
		// 如果当前节点已经不是 Leader（比如因为网络分区被降级），或者节点被杀死了，
		// 这个协程就失去了存在的意义，应该立刻释放锁并退出。
		if rf.state != Leader || rf.killed() {
			rf.mu.Unlock()
			DPrintf("主机：%d退出checkCommit\n", rf.me)
			return
		}

		// 计算当前真实日志的最大索引 (绝对索引)
		// 公式: 快照截断点 + 当前切片长度 - 1
		lastIndex := rf.lastIncludedIndex + len(rf.logs) - 1
		DPrintf("主机：%d在此处算出来的lastIndex=%d\n", rf.me, lastIndex)
		// 【核心逻辑：寻找 N 推进 commitIndex】
		// 从最新的日志往前倒序遍历，寻找符合条件的 N。
		// Raft 论文规定: 寻找一个 N > commitIndex
		for N := lastIndex; N > rf.commitIndex; N-- {

			// 【边界保护】
			// 如果 N 已经落入快照区（甚至更小），说明前面的日志早就被提交并打包了，
			// 不需要再检查，直接终止循环。
			if N <= rf.lastIncludedIndex {
				break
			}

			// 【Raft 论文关键限制：图 8 规则】
			// Leader 不能通过“计算副本数”来直接提交之前任期的日志。
			// Leader 只能提交“当前任期”的日志。一旦当前任期的日志被提交，
			// 之前的所有日志就会被隐式地（附带地）一并提交。
			// 因此，如果我们遇到了一条旧任期的日志，说明当前任期还没有任何日志可以提交，直接 break。
			if rf.logs[N-rf.lastIncludedIndex].Term != rf.currentTerm {
				break
			}

			// 统计将日志 N 成功复制的节点数量
			numsCommit := 0

			// 遍历所有的节点
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					// 自己本地当然已经有了这条日志
					numsCommit++
				} else if rf.matchIndex[i] >= N {
					// 如果 Follower i 的 matchIndex 大于等于 N，
					// 说明 Follower i 的磁盘上已经有了 N 及其之前的所有日志
					numsCommit++
				}
			}

			// 【多数派判定】
			// 如果超过半数（numsCommit * 2 > 节点总数）拥有该日志，且当前仍是 Leader
			if numsCommit*2 > len(rf.peers) && rf.state == Leader {
				// 更新 Leader 的提交索引
				rf.commitIndex = N

				// 因为是从大到小遍历的，找到的第一个符合条件的 N 就是最大的 N，
				// 直接 break 结束这轮寻找。
				break
			}
		}

		// 【休眠与等待唤醒】
		// 调用 condition variable 的 Wait() 方法。
		// 注意：Wait() 被调用时，会自动释放 rf.mu 锁，并挂起当前协程。
		// 当其他地方（例如处理 AppendEntries 的响应时）调用了 rf.commitCond.Signal() 或 Broadcast()，
		// 这个协程会被唤醒，并在 Wait() 返回前**自动重新获取** rf.mu 锁。
		rf.commitCond.Wait()

		// 醒来并重新拿到锁后，立刻释放掉它。
		// 这样下一次 for 循环的开头可以再次干净地执行 rf.mu.Lock()。
		rf.mu.Unlock()
	}
}

func (rf *Raft) initLeader() {
	if rf.killed() {
		return
	}

	lastLogIndex := rf.lastIncludedIndex + len(rf.logs) - 1

	for i := 0; i < len(rf.peers); i++ {
		// nextIndex 是全局日志 index，表示下一条要发给 follower 的日志
		rf.nextIndex[i] = lastLogIndex + 1

		// matchIndex 也是全局日志 index。
		// 快照之前的日志已经等价包含在 lastIncludedIndex 中。
		rf.matchIndex[i] = rf.lastIncludedIndex
	}

	for i := 1; i < len(rf.logs); i++ {
		if rf.logs[i].Cmd == nil {
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
	var cur_term int
	// 无限循环，驱动 Leader 的生命周期
	for {
		// 1. 退出机制：如果测试框架调用了 Kill()，则等待所有正在发送的 RPC 协程结束，然后安全退出
		if rf.killed() {
			wg.Wait()
			if rf.commitCond != nil {
				rf.commitCond.Broadcast()
			}
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
				go rf.checkCommit()
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
			//go rf.checkCommit()

			// 5. 并发发送心跳准备
			i := 0
			wg = sync.WaitGroup{} // 初始化 WaitGroup，用于等待本轮心跳全部发送完毕
			cur_term = rf.currentTerm
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
					go rf.sendEntries(i, &wg, cur_term)
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
