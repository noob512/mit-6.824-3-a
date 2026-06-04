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

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
		Commiter: ck.me,
		RealPos:  ck.pos,
	}
	reply := GetReply{}
	i := 0
	for reply.Err != OK {
		ck.servers[i].Call("KVServer.Get", &args, &reply)
		if reply.Err == OK {
			DPrintf("客户端%v Get key:%v,value:%v\n", ck.pos, key, reply.Value)
		}
		i = (i + 1) % len(ck.servers)
	}
	time.Sleep(10 * time.Millisecond)
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
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
	reply := PutAppendReply{}
	//DPrintf("客户端%v PutAppend %v %v %v\n", ck.pos, key, value, op)
	i := 0
	// if args.Op=="Put"{
	// 	DPrintf("客户端%v PutAppend key:%v\n", ck.pos, key)
	// 	for reply.Err!=OK{
	// 		i=0
	// 		for i<len(ck.servers){
	// 			ck.servers[i].Call("KVServer.PutAppend", &args, &put_reply)
	// 			i++
	// 			if put_reply.Err ==OK{
	// 				reply=put_reply
	// 			}
	// 		}
	// 	}
	// 	time.Sleep(10 * time.Millisecond)
	// 	return
	// }
	for reply.Err != OK {
		ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if reply.Err == OK {
			DPrintf("客户端%v PutAppend key:%v,value:%v\n", ck.pos, key, value)
			ck.numOfOrders++
		} else {
			DPrintf("客户端%v PutAppend key:%v,value:%v,但出现错误未能获得OK,错误类型为%v\n", ck.pos, key, value, reply.Err)
		}
		i = (i + 1) % len(ck.servers)
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
