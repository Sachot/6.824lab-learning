package kvraft

import (
	"../labrpc"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd   // kv-servers/raft
	// You will have to modify this struct.
	leader int  // 最近的leader编号
	seq int32     // 最近的请求号
	cid int64     // clerk编号
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.cid = nrand()
	ck.seq = 0
	ck.leader = 0

	return ck
}

func (ck *Clerk) changeLeader() {
	ck.leader = (ck.leader+1) % len(ck.servers)
}

//
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
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	for {
		args := GetArgs{}
		reply := GetReply{}
		args.Key = key
		args.Cid = ck.cid

		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.changeLeader()
			continue
		}

		if reply.Err == ErrNoKey {
			return ""
		}

		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	curSeq := atomic.AddInt32(&ck.seq, 1)
	for {
		args := PutAppendArgs{}
		reply := PutAppendReply{}
		args.Value = value
		args.Key = key
		args.Op = op
		args.Cid = ck.cid
		args.Seq = curSeq

		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)

		if !ok || reply.Err == ErrWrongLeader {
			ck.changeLeader()
			continue
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
