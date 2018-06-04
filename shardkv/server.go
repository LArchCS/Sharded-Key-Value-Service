package shardkv

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"shardmaster"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Op struct {
	// Your definitions here.
	Type   string
	Key    string
	Value  string
	Pid    string
	DoHash bool
	Kv     map[string]Kvpair // pid -> (key, value)
	Vv     map[string]Vvpair // key -> (val, version)
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	gid        int64 // my replica group ID
	// Your definitions here.
	seq    int
	config *shardmaster.Config
	vv     map[string]Vvpair // key -> (val, version)
	vKv    map[string]Kvpair // visited: pid -> Kvpair(key, val)
	vPid   map[string]string // visited: key -> pid
}

// run an Op and create a new config in master after run Op
func (kv *ShardKV) runOp(op Op) {
	if strings.Compare(op.Type, "Put") == 0 {
		oldv, _ := kv.vv[op.Key]
		if !op.DoHash {
			kv.vv[op.Key] = Vvpair{op.Value, oldv.Version + 1}
		} else {
			kv.vv[op.Key] = Vvpair{strconv.Itoa(int(hash(oldv.Value + op.Value))), oldv.Version + 1}
			kv.vKv[op.Pid] = Kvpair{op.Key, oldv.Value}
			kv.vPid[op.Key] = op.Pid
		}
	} else if strings.Compare(op.Type, "Join") == 0 {
		for pid, v := range op.Kv {
			kv.vKv[pid] = Kvpair{v.Key, v.Value}
			kv.vPid[v.Key] = pid
		}
		for key, val := range op.Vv {
			if val.Version > kv.vv[key].Version {
				kv.vv[key] = val
			}
		}
	}
}

func (kv *ShardKV) Run(op Op) {
	_, ok := kv.vKv[op.Pid]
	if ok {
		return
	}
	var o Op
	for true {
		ok, v := kv.px.Status(kv.seq + 1)
		if ok {
			o = v.(Op)
		} else {
			kv.px.Start(kv.seq+1, op)
			o = waitForAgreement(kv, kv.seq+1)
		}
		kv.runOp(o)
		kv.seq += 1
		if o.Pid == op.Pid {
			break
		}
	}
	kv.px.Done(kv.seq)
}

//wait for paxos for agreement
func waitForAgreement(kv *ShardKV, seq int) Op {
	to := 10 * time.Millisecond
	var res interface{}
	var decided bool
	for true {
		decided, res = kv.px.Status(seq)
		if decided {
			break
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
	return res.(Op)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = ErrWrongGroup
	if kv.config.Shards[key2shard(args.Key)] == kv.gid {
		kv.Run(Op{Type: "Get", Key: args.Key, Pid: args.Pid})
		reply.Err = OK
		reply.Value = kv.vv[args.Key].Value
	}
	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = ErrWrongGroup
	if kv.config.Shards[key2shard(args.Key)] == kv.gid {
		reply.Err = OK
		kv.Run(Op{Type: "Put", Key: args.Key, Value: args.Value, Pid: args.Pid, DoHash: args.DoHash})
		val, ok := kv.vKv[args.Pid]
		reply.PreviousValue = ""
		if ok {
			reply.PreviousValue = val.Value
		}
	}
	return nil
}

func (kv *ShardKV) Join(args *KVArgs, reply *interface{}) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.Run(Op{Type: "Join", Pid: fmt.Sprintf("%d__%d", args.Num, args.Shard), Vv: args.Vv, Kv: args.Kv})
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	config := kv.sm.Query(-1)
	if kv.config.Num < config.Num {
		for cfg := kv.config.Num + 1; cfg <= config.Num; cfg++ {
			config := kv.sm.Query(cfg)
			if kv.config.Num != 0 {
				for i := 0; i < shardmaster.NShards; i++ {
					if config.Shards[i] == kv.gid && kv.config.Shards[i] != kv.gid {
						reply := KVArgs{}
						servers := kv.config.Groups[kv.config.Shards[i]]
						if len(servers) > 0 {
							x := 0
							for !call(servers[x], "ShardKV.UpdateShard", &KVArgs{Shard: i, Gid: kv.gid}, &reply) {
								x = (x + 1) % len(servers)
							}
						}
						if len(reply.Vv) > 0 {
							kv.Join(&KVArgs{Shard: i, Num: config.Num, Kv: reply.Kv, Vv: reply.Vv}, nil)
						}
						kv.config.Shards[i] = kv.gid
					}
				}
			}
			kv.config = &config
		}
	}
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.config = &shardmaster.Config{Num: 0}
	kv.vv = make(map[string]Vvpair)
	kv.vKv = make(map[string]Kvpair)
	kv.vPid = make(map[string]string)

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
