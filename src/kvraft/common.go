package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	ErrWrongLeader = "ErrorWrongLeader"
	Put			 = "Put"
	Append   = "Append"
	PutType  = 0
	AppendType = 1
	GetType = 2

)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Id int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Value string
	ClientId int64
	Id int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
