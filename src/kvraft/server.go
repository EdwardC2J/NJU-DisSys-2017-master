package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	//"fmt"

)

const Debug = 0
const TIMEOUT = time.Second * 3

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

	//insert code
	Type   int //操作类型
  Key    string
  Value  string
  Client int64
  Id     int64
}

type WOp struct { //正在执行的操作
	op *Op
	flag chan bool //同步判断是否已经提交
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister  *raft.Persister
	data       map[string]string
	waitingOps map[int]*WOp
	op_id   map[int64]int64
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	var op Op

	op.Type = GetType
	op.Key = args.Key
	op.Client = args.ClientId
	op.Id = args.Id

	reply.WrongLeader = kv.Operate(op)

	if reply.WrongLeader {
		reply.Err = ErrWrongLeader
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if value, ok := kv.data[args.Key]; ok {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	//insert code
	var op Op

	op.Key = args.Key
	op.Value = args.Value
	op.Client = args.ClientId
	op.Id = args.Id

	if args.Op == Put{
		op.Type = PutType
	}else if args.Op == Append{
		op.Type = AppendType
	}else{
		//fmt.Printf("Wrong PutAppendArgs op")
	}

	reply.WrongLeader = kv.Operate(op)

	if reply.WrongLeader{
		reply.Err = ErrWrongLeader
	}else{
		reply.Err = OK
	}
}

func (kv *RaftKV) Operate(op Op)bool{

	//fmt.Printf("Operate !\n")
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		//fmt.Printf("This Server is Wrong Leader")
		return true
	}

	result := make(chan bool, 1)
  //fmt.Printf("Append to watingOps index:%v  op:%v\n", index, op.Value)
  kv.mu.Lock()
  kv.waitingOps[index] = &WOp{flag: result, op: &op}//append(kv.waitingOps[index], &WOp{flag: result, op: &op})
  kv.mu.Unlock()

  var ok bool
  timer := time.NewTimer(TIMEOUT)
  select {
	case ok = <-result:
  case <-timer.C:
        //fmt.Printf("Wait operation apply to state machine exceeds timeout....\n")
        ok = false
  }
	kv.mu.Lock()
  delete(kv.waitingOps, index)
	kv.mu.Unlock()
  if !ok {
        //fmt.Printf("Wrong leader\n")
        return true
    }
  return false

}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *RaftKV) Handle(msg *raft.ApplyMsg){

	//加锁
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//fmt.Printf("handle message !\n")
	var args Op
	args = msg.Command.(Op)

	if kv.op_id[args.Client] >= args.Id {
    //fmt.Printf("Done operation before\n")
  } else {
        switch args.Type {
        case PutType:
            //fmt.Printf("Put Key/Value %v/%v\n", args.Key, args.Value)
            kv.data[args.Key] = args.Value
        case AppendType:
            //fmt.Printf("Append Key/Value %v/%v\n", args.Key, args.Value)
            kv.data[args.Key] = kv.data[args.Key] + args.Value
        default:
        }
        kv.op_id[args.Client] = args.Id
    }
		/*
    for _, wop := range kv.waitingOps[msg.Index] {
        if wop.op.Client == args.Client && wop.op.Id == args.Id {
            //fmt.Printf("Client:%v %v, Id:%v %v", wop.op.Client, args.Client, wop.op.Id, args.Id)
            wop.flag <- true
        } else {
            //fmt.Printf("Client:%v %v, Id:%v %v", wop.op.Client, args.Client, wop.op.Id, args.Id)
            wop.flag <- false
        }
    }
		*/
		wop :=  kv.waitingOps[msg.Index]
		if wop != nil{
			if wop.op.Client == args.Client && wop.op.Id == args.Id {
					//fmt.Printf("Client:%v %v, Id:%v %v", wop.op.Client, args.Client, wop.op.Id, args.Id)
					wop.flag <- true
			} else {
					//fmt.Printf("Client:%v %v, Id:%v %v", wop.op.Client, args.Client, wop.op.Id, args.Id)
					wop.flag <- false
			}
		}


}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	//insert code
	//初始化
	kv.data = make(map[string]string)
	kv.waitingOps = make(map[int]*WOp)
	kv.op_id = make(map[int64]int64)

	go func() {//处理接收到的消息
        for msg := range kv.applyCh {
						//fmt.Printf("Get message !\n")
						//kv.mu.Lock()
            kv.Handle(&msg)
						//kv.mu.Unlock()
        }
  }()


	return kv
}
