package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers     []*labrpc.ClientEnd
	me          int64
	pos         int
	numOfOrders int
	// You will have to modify this struct.
}

// nrand 生成一个62位的随机整数
// 返回值范围: [0, 2^62 - 1]，即 [0, 4611686018427387903]
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)     // 创建最大值 2^62
	bigx, _ := rand.Int(rand.Reader, max) // 生成 [0, max) 范围的随机大整数
	x := bigx.Int64()                     // 转换为int64
	return x                              // 返回62位随机整数
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.me = nrand()
	ck.servers = servers
	ck.numOfOrders = 1
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Get(key string) string {
    args := GetArgs{
        Key:      key,
        Commiter: ck.me,
        RealPos:  ck.pos,
    }
    
    // 如果你在 Clerk 里维护了 ck.lastLeader，这里可以直接设为 ck.lastLeader
    i := 0 

    // 改为无限循环，成功后直接 return 退出
    for {
        // 🌟 核心修复 1：每次请求前，必须声明一个干干净净的 reply！
        reply := GetReply{}
        
        // 🌟 核心修复 2：捕获 Call 的网络连通状态 ok
        ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
        
        // 只有网络成功送达，且服务器明确承认自己是 Leader 并处理了数据
        if ok && reply.Err == OK {
            DPrintf("客户端%v Get key:%v,value:%v\n", ck.pos, key, reply.Value)
            return reply.Value // ✅ 拿到数据，大功告成，直接返回！
        }
        
        // 如果网络断了，或者对方不是 Leader，尝试问下一个人
        i = (i + 1) % len(ck.servers)
        
        // 🌟 核心修复 3：休眠必须放在循环内部！
        // 找不到 Leader 时稍微等一下，把 CPU 让给底层的 Raft 去进行选举
        time.Sleep(10 * time.Millisecond)
    }
}




func (ck *Clerk) PutAppend(key string, value string, op string) {
    // You will have to modify this function.
    args := PutAppendArgs{
        Key:      key,
        Value:    value,
        Op:       op,
        Pos:      ck.numOfOrders,
        Commiter: ck.me,
        RealPos:  ck.pos,
    }
    
    // 性能优化建议：如果你的 Clerk 结构体里记录了 ck.lastLeader（上一次成功的节点），
    // 这里的 i 可以初始化为 ck.lastLeader，而不是每次都从 0 开始试。
    i := 0 

    for {
        // 🌟 核心调整：必须在循环内部声明 reply！
        // 确保每次调用 Call 时，传入的都是一个字段全为空/零值的干净结构体。
        reply := PutAppendReply{}
        
        // 发起 RPC 调用，注意要接收 Call 的布尔返回值，判断网络是否连通
        ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)

        // 只有网络连通，且服务器明确返回 OK 时，才算成功
        if ok && reply.Err == OK {
            DPrintf("客户端%v PutAppend key:%v,value:%v 成功,尝试主机 %v 失败\n", ck.pos, key, value,i)
            ck.numOfOrders++
            return // ✅ 成功，直接结束函数
        } 
        
        // 失败情况打印（网络不通、不是 Leader、或超时）
        DPrintf("客户端%v PutAppend key:%v,value:%v 尝试主机 %v 失败, Err: %v\n", 
                ck.pos, key, value, i, reply.Err)

        // 尝试下一个节点
        i = (i + 1) % len(ck.servers)
        
        // 遇到错误时短暂休眠，防止死循环瞬间打满 CPU
        time.Sleep(10 * time.Millisecond)
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
