package shardmaster

import (
	crand "crypto/rand"
	"encoding/gob"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"strings"
	"sync"
	"syscall"
	"time"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos
	configs    []Config // indexed by config num

	// Your data here.
	prevNum int // config number from which need to be executed
	nextNum int // next config number
}

type Op struct {
	// Your data here.
	Type    string   // type of Op, get or put
	UUID    int64    // unique id for each Op
	GID     int64    // GID
	Num     int      // for Query
	Shard   int      // for Move
	Servers []string // for Join
}

type CountResult struct {
	// Your data here.
	min      int   // min number of shards
	max      int   // max number of shards
	minGroup int64 // group with min shards
	maxGroup int64 // group with max shards
}

// from lab1
// generate numbers that have a high probability of being unique
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

// count the min/max groups in config
func countConfig(shards [NShards]int64, groups map[int64][]string) CountResult {
	var min int = NShards
	var max int
	var minGroup int64
	var maxGroup int64
	for group, _ := range groups {
		n := 0
		for _, g := range shards {
			if g == group {
				n += 1
			}
		}
		if n < min {
			minGroup = group
			min = n
		}
		if n > max {
			maxGroup = group
			max = n
		}
	}
	return CountResult{min, max, minGroup, maxGroup}
}

// rebalance config, called by Join and Leave
func balanceConfig(shards [NShards]int64, groups map[int64][]string) ([NShards]int64, map[int64][]string) {
	// assign un-hosted shard to minGroup
	for shard, group := range shards {
		if group == 0 {
			cnt := countConfig(shards, groups)
			shards[shard] = cnt.minGroup
		}
	}
	for true {
		cnt := countConfig(shards, groups)
		if cnt.max-cnt.min <= 1 {
			break
		}
		for shard, group := range shards {
			cnt := countConfig(shards, groups)
			if group == cnt.maxGroup {
				shards[shard] = cnt.minGroup
			}
		}
	}
	return shards, groups
}

//wait for Paxos instances to complete agreement
//sleep between calls, and return agreed value
func waitForAgreement(sm *ShardMaster, seq int) interface{} {
	to := 10 * time.Millisecond
	var agreed interface{}
	var decided bool
	for true {
		decided, agreed = sm.px.Status(seq)
		if decided {
			break
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
	return agreed
}

// run an Op and create a new config in master after run Op
func runOp(sm *ShardMaster, op Op) {
	config := sm.configs[len(sm.configs)-1]
	// create new groups and new shards
	var shards [NShards]int64
	groups := make(map[int64][]string)
	for shard, group := range config.Shards {
		shards[shard] = group
	}
	for group, servers := range config.Groups {
		groups[group] = servers
	}
	if strings.Compare(op.Type, "Move") == 0 {
		shards[op.Shard] = op.GID // assign shard to group
		sm.configs = append(sm.configs, Config{config.Num + 1, shards, groups})
	} else if strings.Compare(op.Type, "Join") == 0 {
		groups[op.GID] = op.Servers // add group
		shards, groups = balanceConfig(shards, groups)
		sm.configs = append(sm.configs, Config{config.Num + 1, shards, groups})
	} else if strings.Compare(op.Type, "Leave") == 0 {
		delete(groups, op.GID) // delete group, and unassign related shard
		for pos, group := range shards {
			if group == op.GID {
				shards[pos] = 0
			}
		}
		shards, groups = balanceConfig(shards, groups)
		sm.configs = append(sm.configs, Config{config.Num + 1, shards, groups})
	}
}

//start agreement on Op
func (sm *ShardMaster) Agree(op Op) {
	sm.prevNum = sm.nextNum
	for {
		sm.px.Start(sm.nextNum, op)
		agreed := waitForAgreement(sm, sm.nextNum).(Op)
		if agreed.UUID == op.UUID {
			break
		}
		sm.nextNum += 1
	}
}

//execute Ops in sm
func (sm *ShardMaster) Run() {
	for num := sm.prevNum; num < sm.nextNum+1; num++ {
		waitForAgreement(sm, num)
		decided, agreed := sm.px.Status(num)
		op := agreed.(Op)
		if decided {
			runOp(sm, op)
		}
	}
}

//set executed Ops to done, and increment nextNum
func (sm *ShardMaster) Done() {
	sm.px.Done(sm.nextNum)
	sm.nextNum += 1
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.Agree(Op{Type: "Join", UUID: nrand(), GID: args.GID, Servers: args.Servers})
	sm.Run()
	sm.Done()
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.Agree(Op{Type: "Leave", UUID: nrand(), GID: args.GID})
	sm.Run()
	sm.Done()
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.Agree(Op{Type: "Move", UUID: nrand(), GID: args.GID, Shard: args.Shard})
	sm.Run()
	sm.Done()
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.Agree(Op{Type: "Query", UUID: nrand(), Num: args.Num})
	sm.Run()
	if args.Num >= len(sm.configs) || args.Num == -1 {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
	sm.Done()
	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	// start self defined fields
	sm.prevNum = 0
	sm.nextNum = 0

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
