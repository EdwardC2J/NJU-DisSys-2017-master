package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"
//import "fmt"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	//insert code
	leaderId int
	clientId int64
	currentOpId int64
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

	//insert code
	ck.leaderId = 0
	ck.currentOpId = 0
	ck.clientId = nrand()

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	var args GetArgs
	args.Key = key
	args.ClientId = ck.clientId
	args.Id = atomic.AddInt64(&ck.currentOpId, 1)

	for{
		var replyArgs GetReply
		ck.servers[ck.leaderId].Call("RaftKV.Get",&args,&replyArgs)

		if replyArgs.WrongLeader == false && replyArgs.Err == OK{
			return replyArgs.Value
		}else{
			ck.leaderId = (ck.leaderId + 1)% len(ck.servers)

			if replyArgs.WrongLeader == false {
				//fmt.Printf("Wrong Leader in Clerk.Get value of "    + "\n")
			}else{
				//fmt.Printf("Reply Error in Clerk.Get value of "  + " because "  + "\n")
			}

		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//insert code

	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	args.Id = atomic.AddInt64(&ck.currentOpId, 1)

	for{
		var replyArgs PutAppendReply
		ck.servers[ck.leaderId].Call("RaftKV.PutAppend",&args,&replyArgs)
		if replyArgs.WrongLeader == false && replyArgs.Err == OK{
			break;
		}else{
			ck.leaderId = (ck.leaderId + 1)% len(ck.servers)

			if replyArgs.WrongLeader == false {
				//fmt.Printf("Wrong Leader in Clerk.PutAppend to " + op + "\n")
			}else{
				//fmt.Printf("Reply Error in Clerk.PutAppend to " + op + " because "+"\n")
			}

		}
	}



}

func (ck *Clerk) Put(key string, value string) {

	//fmt.Printf("Clerk Put begin !\n")
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	//fmt.Printf("Clerk Append begin !\n")
	ck.PutAppend(key, value, "Append")
}
