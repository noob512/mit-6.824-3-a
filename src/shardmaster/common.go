package shardmaster

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10



// Config 定义了系统的“配置快照”（Configuration）
// 它是分片（Shards）到副本组（Replica Groups）的分配映射表。
// 官方强烈要求：绝对不要修改这个结构体的定义，否则测试框架会直接崩溃。
type Config struct {
    // Num: 当前配置的版本号（单调递增）。
    // 系统的初始空配置 Num 必须为 0。
    // 之后每当集群拓扑发生变化（即成功执行了一次 Join、Leave 或 Move RPC），
    // 主控节点就会生成一个全新的 Config，其 Num 会在上一个配置的基础上 +1。
    Num    int              
    
    // Shards: 分片路由表（定长数组）。
    // NShards 是一个常量，代表系统将数据总共切分成了多少块（Lab 中通常固定为 10）。
    // 数组的【索引】代表：分片编号 (Shard ID，范围是 0 到 NShards-1)。
    // 数组的【值】代表：负责存储该分片的副本组编号 (Group ID, 简称 GID)。
    // 例如：Shards[3] = 101 意味着第 3 号分片目前由 GID 为 101 的集群负责。
    // 特殊情况：如果值为 0，代表该分片尚未分配给任何有效的组（通常在 Config 0 时全部为 0）。
    Shards [NShards]int     
    
    // Groups: 活跃副本组的成员注册表。
    // 这是一个映射 (Map)，记录了当前配置下集群中所有存活的组。
    // 【Key (int)】: 副本组的 GID（必须是非零正整数，测试用例里通常是 1, 2, 100 等）。
    // 【Value ([]string)】: 该副本组包含的所有物理服务器的网络地址/端点名称列表。
    // 例如：Groups[101] = []string{"server-A", "server-B", "server-C"} 
    // 客户端拿到这个 Map 后，就会向这三台服务器发送底层数据请求。
    Groups map[int][]string 
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	CommandId int
	ClientId int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	CommandId int
	ClientId int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	CommandId int
	ClientId int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	CommandId int
	ClientId int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
