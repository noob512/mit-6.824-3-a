package shardmaster

//
// Shardmaster clerk.
//

import "../labrpc"
import "time"
import "crypto/rand"
import "math/big"

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

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.ClientId=ck.me
	args.CommandId=ck.numOfOrders
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.numOfOrders++
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId=ck.me
	args.CommandId=ck.numOfOrders
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.numOfOrders++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId=ck.me
	args.CommandId=ck.numOfOrders
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.numOfOrders++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId=ck.me
	args.CommandId=ck.numOfOrders

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.numOfOrders++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
