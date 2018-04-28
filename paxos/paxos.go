package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"
)

const OK = "OK"
const REJECT = "REJECT"

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances map[int]*PaxosInstance // paxos instances
	dones     []int                  // done states of instances
}

type PaxosInstance struct {
	seq            int
	decided        bool
	preparedNumber int
	acceptedNumber int
	decidedValue   interface{}
}

type PaxosArgs struct {
	Seq          int
	AcceptNumber int
	AcceptValue  interface{}
	// update done in done map of peers
	Me   int // sender's index in peers
	Done int // sender's done value
}

type PaxosReply struct {
	Reply        string
	AcceptNumber int
	AcceptValue  interface{}
}

func (px *Paxos) CreatePaxosInstance(n int) {
	px.instances[n] = &PaxosInstance{seq: n}
}

// generate a proposal number for paxos instance in increasing order
func (px *Paxos) GeneratePaxosNumber() int {
	return int(time.Now().UnixNano())*len(px.peers) + px.me
}

// send a prepare request to all peers
func (px *Paxos) SendPrepare(seq int, n int, v interface{}) (bool, interface{}) {
	highest := 0
	value := v
	oks := 0
	for i, peer := range px.peers {
		args := PaxosArgs{Seq: seq, AcceptNumber: n, Me: px.me}
		var reply PaxosReply = PaxosReply{Reply: REJECT}
		if i == px.me {
			px.TakePrepare(&args, &reply)
		} else {
			call(peer, "Paxos.TakePrepare", &args, &reply)
		}
		if reply.Reply == OK {
			oks += 1
			if reply.AcceptNumber > highest {
				highest = reply.AcceptNumber
				value = reply.AcceptValue
			}
		}
	}
	return oks > len(px.peers)/2, value
}

// a paxos handles a prepare request
func (px *Paxos) TakePrepare(args *PaxosArgs, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	reply.Reply = REJECT
	_, exist := px.instances[args.Seq]
	if !exist {
		px.CreatePaxosInstance(args.Seq)
	}
	if px.instances[args.Seq].preparedNumber < args.AcceptNumber {
		reply.Reply = OK
		px.instances[args.Seq].preparedNumber = args.AcceptNumber
		reply.AcceptNumber = px.instances[args.Seq].acceptedNumber
		reply.AcceptValue = px.instances[args.Seq].decidedValue
	}
	return nil
}

// send an accept request to peers
func (px *Paxos) SendAccept(seq int, n int, value interface{}) bool {
	oks := 0
	for i, peer := range px.peers {
		args := PaxosArgs{Seq: seq, AcceptNumber: n, AcceptValue: value, Me: px.me}
		var reply PaxosReply = PaxosReply{Reply: REJECT}
		if i == px.me {
			px.TakeAccept(&args, &reply)
		} else {
			call(peer, "Paxos.TakeAccept", &args, &reply)
		}
		if reply.Reply == OK {
			oks += 1
			if oks > len(px.peers)/2 {
				break
			}
		}
	}
	return oks > len(px.peers)/2
}

// a paxos handles an accepet request
func (px *Paxos) TakeAccept(args *PaxosArgs, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	reply.Reply = REJECT
	_, exits := px.instances[args.Seq]
	if !exits {
		px.CreatePaxosInstance(args.Seq)
	}
	if args.AcceptNumber >= px.instances[args.Seq].preparedNumber {
		reply.Reply = OK
		px.instances[args.Seq].preparedNumber = args.AcceptNumber
		px.instances[args.Seq].acceptedNumber = args.AcceptNumber
		px.instances[args.Seq].decidedValue = args.AcceptValue
	}
	return nil
}

func (px *Paxos) SendDecision(seq int, n int, value interface{}) {
	args := PaxosArgs{Seq: seq, AcceptNumber: n, AcceptValue: value, Done: px.dones[px.me], Me: px.me}
	reply := PaxosReply{}
	for i := range px.peers {
		if i == px.me {
			px.TakeDecision(&args, &reply)
		} else {
			call(px.peers[i], "Paxos.TakeDecision", &args, &reply)
		}
	}
}

func (px *Paxos) TakeDecision(args *PaxosArgs, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	_, exists := px.instances[args.Seq]
	if !exists {
		px.CreatePaxosInstance(args.Seq)
	}
	px.instances[args.Seq].acceptedNumber = args.AcceptNumber
	px.instances[args.Seq].decidedValue = args.AcceptValue
	px.instances[args.Seq].decided = true
	px.dones[args.Me] = args.Done // update done in done map of peers
	return nil
}

func (px *Paxos) RunPaxos(seq int, v interface{}) {
	if seq >= px.Min() {
		for {
			n := px.GeneratePaxosNumber()
			ok, value := px.SendPrepare(seq, n, v)
			if ok && px.SendAccept(seq, n, value) {
				px.SendDecision(seq, n, value)
				break
			}
			time.Sleep(time.Second)
		}
	}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			//fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go px.RunPaxos(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.dones[px.me] < seq {
		px.dones[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	max := 0
	for seq, _ := range px.instances {
		if seq > max {
			max = seq
		}
	}
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// It is illegal to call Done(i) on a peer and
// then call Start(j) on that peer for any j <= i.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	min := px.dones[px.me]
	for _, done := range px.dones {
		if done < min {
			min = done
		}
	}
	// free up memory
	for seq, instance := range px.instances {
		if seq <= min && instance.decided {
			delete(px.instances, seq)
		}
	}
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.
	min := px.Min()
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq < min {
		return false, nil
	}
	_, exist := px.instances[seq]
	if !exist {
		return false, nil
	}
	return px.instances[seq].decided, px.instances[seq].decidedValue
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instances = make(map[int]*PaxosInstance)
	px.dones = make([]int, len(peers))
	for i := range peers {
		px.dones[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							//fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					//fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
