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
	Term                    int
	LeaderId                int
	LastIncludedIndex       int
	LastIncludedTerm        int
	LastIncludedPublicIndex int
	Data                    []byte
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
			offset := rf.lastApplied - rf.lastIncludedIndex

			// 情况 B：如果 Cmd 为空（可能是占位符/空日志），仅更新状态并统计占位数量
			if rf.logs[offset].Cmd == nil {
				rf.logs[offset].Committed = true
				rf.persist() // 立即持久化状态变化
				rf.mu.Unlock()
				continue
			}

			// 情况 C：正常的日志提交逻辑
			newApplyMsg := new(ApplyMsg)
			rf.logs[offset].Committed = true
			rf.persist() // 持久化修改后的 committed 状态

			newApplyMsg.Command = rf.logs[offset].Cmd
			// 计算向上传递的 CommandIndex，减去空日志偏移量，确保索引连续
			newApplyMsg.CommandIndex = rf.realToPublicIndex(rf.lastApplied)
			newApplyMsg.CommandValid = true
			commandIndex := newApplyMsg.CommandIndex
			command := rf.logs[offset].Cmd

			rf.mu.Unlock()
			rf.applyCh <- *newApplyMsg

			DPrintf("主机：%d中索引为%d的日志提交完成,提交内容为：%v\n", rf.me, commandIndex, command)
		}
	}
}

// InstallSnapshot 是 Follower 处理 Leader 发来的快照 RPC 的处理函数。
// 当 Leader 发现 Follower 落后太多，且需要的日志已经被 Leader 自身的快照丢弃时，会调用此 RPC。
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// 加锁保护 Raft 内部状态
	rf.mu.Lock()
	DPrintf("调用安装快照函数\n")
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
			tail := append([]one_log(nil), rf.logs[offset+1:]...)
			newLogs = append(newLogs, tail...)
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
	rf.lastIncludedPublicIndex = args.LastIncludedPublicIndex

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
		SnapshotIndex: args.LastIncludedPublicIndex,
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
		lastLogIndex := rf.lastLogIndex()
		prevLogOffset := args.PrevLogIndex - rf.lastIncludedIndex
		if args.PrevLogIndex < rf.lastIncludedIndex || args.PrevLogIndex > lastLogIndex || rf.logs[prevLogOffset].Term != args.PrevLogTerm {

			// Case 1: 本地日志太短，根本没有 PrevLogIndex 这个位置的日志
			if args.PrevLogIndex < rf.lastIncludedIndex {
				DPrintf("主机：%d 日志存在问题 因为args.PrevLogIndex是%d但是lastIncludedIndex为%d\n", rf.me, args.PrevLogIndex, rf.lastIncludedIndex)
				reply.ConflictIndex = rf.lastIncludedIndex + 1
				reply.ConflictTerm = -1
			} else if args.PrevLogIndex > lastLogIndex {
				DPrintf("主机：%d 日志存在问题 因为args.PrevLogIndex是%d但是lastLogIndex为%d\n", rf.me, args.PrevLogIndex, lastLogIndex)
				reply.ConflictIndex = lastLogIndex + 1 // 告诉 Leader："我只有这么长，你从这里开始发"
				reply.ConflictTerm = -1

				// Case 2: 长度够，但是在 PrevLogIndex 处的任期冲突了 (发生了网络分区或换届导致的分支)
			} else {
				DPrintf("主机：%d 日志存在问题 因为rf.logs[prevLogOffset].Term是%d但是args.PrevLogTerm为%d\n", rf.me, rf.logs[prevLogOffset].Term, args.PrevLogTerm)
				i := args.PrevLogIndex
				conflictTerm := rf.logs[prevLogOffset].Term

				// 【快速回退算法】：试图一次性跳过一整个冲突的 Term，而不是每次 RPC 只往前退一个 Index
				if conflictTerm < args.PrevLogTerm {
					// 本地任期比 Leader 的小（较少见，通常是因为被旧 Leader 截断过）
					reply.ConflictIndex = args.PrevLogIndex - 1
				} else if conflictTerm > args.PrevLogTerm {
					// 本地任期比 Leader 的大（这是典型的拥有未提交的“脏日志”）
					// 往前遍历，找到属于当前冲突 Term 的第一条日志的起始位置
					for i >= rf.lastIncludedIndex && rf.logs[i-rf.lastIncludedIndex].Term == conflictTerm {
						i--
					}
					reply.ConflictIndex = i + 1 // 告诉 Leader："从我这个冲突 Term 的开头重新覆盖吧"
				}
				reply.ConflictTerm = conflictTerm
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
					offset := idx - rf.lastIncludedIndex

					if offset < len(rf.logs) {
						// 情况 1：同一位置日志已存在。比对 Term。
						if rf.logs[offset].Term != entry.Term {
							// 发现 Term 冲突！说明从这开始是旧 Leader 的脏数据。
							// 仅从这个【冲突位置】开始截断覆盖，并拼接上剩余的新日志
							rf.logs = append(rf.logs[:offset], args.Entries[i:]...)
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
				rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())

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
			Term:                    term,
			LeaderId:                rf.me,
			LastIncludedIndex:       rf.lastIncludedIndex,
			LastIncludedTerm:        rf.lastIncludedTerm,
			LastIncludedPublicIndex: rf.lastIncludedPublicIndex,
			Data:                    rf.persister.ReadSnapshot(), // 从底层的持久化存储读取真实的快照二进制数据
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
				//rf.MaxnilNum = 0 // 重置某些定制化的空日志计数
				rf.initLeader() // 初始化 Leader 专属的易失性状态
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
