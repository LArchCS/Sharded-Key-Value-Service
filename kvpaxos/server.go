package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"strconv"
	"sync"
	"syscall"
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
	DoPut  bool   // if it is a put or get
	DoHash bool   // if it is put hash or reg push
	Id     int64  // unique id per request
	Key    string // key
	Value  string // val
	Me     string // clerk.me
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos
	// Your definitions here.
	seq     int               // last processed seq
	kvs     map[string]string // key: value
	ids     map[string]int64  // clerk: last seen id
	replies map[string]string // clerk: last reply
}

// a mix used put/get handler
func (kv *KVPaxos) handler(op Op) string {
	id, _ := kv.ids[op.Me]
	val, _ := kv.replies[op.Me]
	for id != op.Id {
		kv.seq += 1
		ok, value := kv.px.Status(kv.seq)
		if !ok {
			// if falling behind, then call start
			kv.px.Start(kv.seq, op)
			// check status for value
			ok, value = kv.px.Status(kv.seq)
			// wait for instance to complete agreement
			to := 10 * time.Millisecond
			for !ok {
				time.Sleep(to)
				ok, value = kv.px.Status(kv.seq)
				if to < 10*time.Second {
					to *= 2
				}
			}
		}
		oop := value.(Op)
		// handle oop
		val, _ = kv.kvs[oop.Key]
		kv.replies[oop.Me] = val
		kv.ids[oop.Me] = oop.Id
		if oop.DoPut { // if it is put, then update kv map
			if oop.DoHash {
				h := hash(val + oop.Value) // use the hash in test file
				kv.kvs[oop.Key] = strconv.Itoa(int(h))
			} else {
				kv.kvs[oop.Key] = oop.Value
			}
		}
		id = oop.Id        // check uuid
		kv.px.Done(kv.seq) // call done after one instance is processed
	}
	return val
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{false, false, args.Id, args.Key, "", args.Me}
	reply.Value = kv.handler(op)
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{true, args.DoHash, args.Id, args.Key, args.Value, args.Me}
	reply.PreviousValue = kv.handler(op)
	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// this call is all that's needed to persuade
	// Go's RPC library to marshall/unmarshall
	// struct Op.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.seq = 0
	kv.kvs = make(map[string]string)
	kv.ids = make(map[string]int64)
	kv.replies = make(map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
