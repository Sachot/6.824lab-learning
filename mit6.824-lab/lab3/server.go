package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string
	Operation string
	Cid int64
	Seq int32
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvDB    map[string]string
	cid2seq map[int64]int32
	commandIndex2op  map[int]Op  // k:index v:Op
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{}
	command.Key = args.Key
	command.Operation = "Get"
	command.Cid = args.Cid

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	time.Sleep(500 * time.Millisecond)
	op, ok1 := kv.commandIndex2op[index]
	if !ok1 {
		reply.Err = ErrWrongLeader
		return
	}
	if !isOpCorrect(command, op) {  // 当leader网络出现分区，可能会出现过期leader仍然把自己作为leader发送AE
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	value, ok2 := kv.kvDB[args.Key]
	if ok2 {
		reply.Value = value
		reply.Err = ""
	}else{
		reply.Err = ErrNoKey
	}
	kv.mu.Unlock()
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op{}
	command.Key = args.Key
	command.Value = args.Value
	command.Operation = args.Op
	command.Cid = args.Cid
	command.Seq = args.Seq

	index, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	time.Sleep(500 * time.Millisecond)
	op, ok := kv.commandIndex2op[index]
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	if !isOpCorrect(command, op) {  // 当leader网络出现分区，可能会出现过期leader仍然把自己作为leader发送AE
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = ""
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readApplyChLoop() {
	for !kv.killed() {
		msg := <- kv.applyCh

		/*op := msg.Command.(int)
		fmt.Printf("%v, %T\n", op, op)*/

		op, ok := msg.Command.(Op)  // 这里msg.Command实际上是Op，因此可以cast为Op

		// fmt.Printf("ok-return:%v,test:%T\n",ok,test)

		if ok {
			kv.mu.Lock()
			if op.Operation == "Put" || op.Operation == "Append" {
				curSeq, ok := kv.cid2seq[op.Cid]
				if !ok || curSeq < op.Seq {  // 只处理最新的请求
					kv.cid2seq[op.Cid] = op.Seq
					switch op.Operation {
					case "Put":
						kv.kvDB[op.Key] = op.Value
					case "Append":
						kv.kvDB[op.Key] += op.Value
					}
				}
			}

			kv.commandIndex2op[msg.CommandIndex] = op
			kv.mu.Unlock()
		}

	}
}

func isOpCorrect(op1, op2 Op) bool {
	return op1.Seq == op2.Seq && op1.Cid == op2.Cid && op1.Operation == op2.Operation &&
		op1.Key == op2.Key && op1.Value == op2.Value
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.commandIndex2op = make(map[int] Op)
	kv.kvDB = make(map[string] string)
	kv.cid2seq = make(map[int64] int32)

	go kv.readApplyChLoop()

	return kv
}
