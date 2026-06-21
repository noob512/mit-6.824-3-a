package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "../labrpc"
import "crypto/rand"
import "math/big"
import "../shardmaster"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId    int64
	pos         int
	CommandId int
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	
	// You'll have to add code here.

	// 1. ClientId: 生成一个唯一的 64 位随机大整数作为客户端身份标识
	ck.clientId = nrand() 
	
	// 2. CommandId: 初始化请求序列号，通常从 1 开始
	ck.CommandId = 1 

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
// Get 向 ShardKV 集群发起读请求，根据 Key 获取对应的 Value。
// 如果 Key 不存在，则返回空字符串 ""。
func (ck *Clerk) Get(key string) string {
    // 1. 封装 RPC 请求参数
    args := GetArgs{}
    args.Key = key

    // 🔴 【防重放/线性一致性提醒】
    // 虽然 Get 是只读操作，但在严格的线性一致性（Linearizability）要求下，
    // 为了防止网络延迟导致的“旧 Leader 读”（Stale Read），Get 请求通常也需要走一遍 Raft 共识。
    // 因此，这里同样建议加上 ClientId 和 CommandId：
    // args.ClientId = ck.clientId
    // args.CommandId = ck.nextCommandId()

    // 2. 外层死循环：负责在配置变更（分片迁移）时不断更新地图并重试
    for {
        // 根据固定哈希算法，计算这个 Key 属于哪个分片（0~9）
        shard := key2shard(key)
        
        // 查本地缓存的“地图”，看看这个分片目前归哪个副本组 (GID) 管
        gid := ck.config.Shards[shard]
        
        // 如果本地配置中存在这个 GID 的服务器列表
        if servers, ok := ck.config.Groups[gid]; ok {
            
            // 3. 内层循环：负责在这个副本组内挨个尝试，找到真正的 Raft Leader
            for si := 0; si < len(servers); si++ {
                // 将服务器名字转换为可通信的网络端点
                srv := ck.make_end(servers[si])
                
                var reply GetReply
                
                // 发起同步 RPC 调用
                ok := srv.Call("ShardKV.Get", &args, &reply)
                
                // 4. 结果判定
                // 情况 A：请求成功执行！
                // 注意这里的特殊逻辑：对于 Get 来说，哪怕查不到数据 (ErrNoKey)，也是一次“成功的业务响应”。
                if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.CommandId++
                    return reply.Value // 如果是 ErrNoKey，通常 reply.Value 会是默认的空字符串 ""
                }
                
                // 情况 B：找错组了 (配置已过期)
                if ok && (reply.Err == ErrWrongGroup) {
                    // 当前组已经把这个分片交接出去了。
                    // 继续在这个组里重试毫无意义，必须 break 跳出内层循环，去外层更新地图。
                    break
                }
                
                // 情况 C：网络超时 (ok == false)，或者当前节点不是 Leader (ErrWrongLeader)
                // 代码走到这里会静默向下执行，顺理成章地进入下一次 for si 循环，尝试下一台机器。
            }
        }
        
        // 5. 更新配置
        // 走到这里说明：要么收到了 ErrWrongGroup，要么整个组都宕机/断网了，要么刚初始化本地还没配置。
        
        // 休息 100 毫秒，避免客户端疯狂重试打满 CPU，也给 ShardMaster 留出反应时间
        time.Sleep(100 * time.Millisecond)
        
        // 去总公司 (ShardMaster) 强行拉取最新的配置覆盖本地。
        // 下一次外层循环就会拿着最新的 gid 和 servers 去寻址了。
        ck.config = ck.sm.Query(-1)
    }

    return ""
}

//
// shared by Put and Append.
// You will have to modify this function. (你需要修改这个函数)
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
    // 1. 封装 RPC 请求参数
    args := PutAppendArgs{}
    args.Key = key
    args.Value = value
    args.Op = op // "Put" 或 "Append"
	args.ClientId = ck.clientId
    args.CommandId = ck.CommandId

    // 2. 外层死循环：负责在配置变更（分片迁移）时不断重试
    for {
        // 计算这个 Key 属于哪个分片（0~9）
        shard := key2shard(key)
        
        // 查本地缓存的“地图”（ck.config），看看这个分片目前归哪个副本组 (GID) 管
        gid := ck.config.Shards[shard]
        
        // 从地图里找出这个 GID 组内所有服务器的名称列表
        if servers, ok := ck.config.Groups[gid]; ok {
            
            // 3. 内层循环：负责在这个副本组内找到真正的 Raft Leader
            for si := 0; si < len(servers); si++ {
                // 将服务器的字符串名字转换成可以发 RPC 的网络端点
                srv := ck.make_end(servers[si])
                
                var reply PutAppendReply
                // 发起 RPC 调用（此处会阻塞等待回复或超时）
                ok := srv.Call("ShardKV.PutAppend", &args, &reply)
                
                // 情况 A：RPC 成功到达，且服务器成功执行了操作
                if ok && reply.Err == OK {
					ck.CommandId++
                    return // 大功告成，安全退出！
                }
                
                // 情况 B：RPC 成功到达，但服务器说：“对不起，配置更新了，这个分片现在不归我管了”
                if ok && reply.Err == ErrWrongGroup {
                    // 这个错误意味着继续在这个组里试其他节点毫无意义，它们都不会接客。
                    // 必须 break 跳出内层循环，去外层循环更新配置！
                    break 
                }
                // 情况 C：网络超时 (ok == false)，或者找错了人 (reply.Err == ErrWrongLeader)
                // 此时代码什么都不做，顺理成章地进入下一次 for si 循环，尝试同组内的下一台服务器。
            }
        }
        
        // 4. 更新“地图”（配置）
        // 如果代码走到了这里，说明遇到了以下几种情况：
        // 1. 刚才遇到了 ErrWrongGroup 被 break 出来了。
        // 2. 本地 config 里根本找不到对应的 GID（比如刚初始化，全是 GID 0）。
        // 3. 遍历了组内的所有服务器，全都不通（可能整个组宕机了）。
        
        // 休息 100 毫秒，防止客户端陷入疯狂的死循环（CPU 飙升并导致网络风暴）
        time.Sleep(100 * time.Millisecond)
        
        // 向控制面 ShardMaster 请求最新版本的 Config，覆盖本地的旧地图。
        // 下一次 for 循环时，就会拿着新地图重新寻址了。
        ck.config = ck.sm.Query(-1)
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
