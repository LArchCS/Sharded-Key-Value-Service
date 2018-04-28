package kvpaxos

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
	"strconv"
	"time"
)

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	me string
}

// from lab1
// generate numbers that have a high probability of being unique
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// generate uuid for a client in increasing order
func getClerkId() string {
	time.Sleep(10 * time.Millisecond)
	return strconv.Itoa(int(time.Now().UnixNano())) + "-" + strconv.Itoa(int(nrand()))
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = getClerkId()
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{key, nrand(), ck.me}
	reply := GetReply{}
	i := 0
	to := 2 * time.Second
	ok := call(ck.servers[i], "KVPaxos.Get", &args, &reply)
	for !ok {
		time.Sleep(to)
		i = (i + 1) % len(ck.servers)
		ok = call(ck.servers[i], "KVPaxos.Get", &args, &reply)
	}
	return reply.Value
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	// You will have to modify this function.
	args := PutArgs{key, value, dohash, nrand(), ck.me}
	reply := PutReply{}
	i := 0
	to := 2 * time.Second
	ok := call(ck.servers[i], "KVPaxos.Put", &args, &reply)
	for !ok {
		time.Sleep(to)
		i = (i + 1) % len(ck.servers)
		ok = call(ck.servers[i], "KVPaxos.Put", &args, &reply)
	}
	return reply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}

func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
