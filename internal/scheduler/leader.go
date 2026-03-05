package scheduler

import (
	"fmt"
	"net/rpc"
	"sync"
	"time"

	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/proto"
	"github.com/vladimir120307-droid/distributed-task-scheduler/pkg/logger"
)

// NodeState represents the role of a scheduler node in the cluster.
type NodeState int

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

func (s NodeState) String() string {
	switch s {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	default:
		return "unknown"
	}
}

// LeaderElector implements a simple heartbeat-based leader election protocol.
type LeaderElector struct {
	mu              sync.RWMutex
	nodeID          string
	listenAddr      string
	state           NodeState
	currentTerm     uint64
	leaderID        string
	leaderAddr      string
	peers           map[string]string
	heartbeatTick   time.Duration
	electionTimeout time.Duration
	lastHeartbeat   time.Time
	stopCh          chan struct{}
	log             *logger.Logger
	onBecomeLeader  func()
	onLoseLeader    func()
}

// ElectorConfig holds the leader election configuration.
type ElectorConfig struct {
	NodeID          string
	ListenAddr      string
	HeartbeatTick   time.Duration
	ElectionTimeout time.Duration
	Log             *logger.Logger
	OnBecomeLeader  func()
	OnLoseLeader    func()
}

// NewLeaderElector creates a new elector instance.
func NewLeaderElector(cfg ElectorConfig) *LeaderElector {
	return &LeaderElector{
		nodeID:          cfg.NodeID,
		listenAddr:      cfg.ListenAddr,
		state:           StateFollower,
		currentTerm:     0,
		peers:           make(map[string]string),
		heartbeatTick:   cfg.HeartbeatTick,
		electionTimeout: cfg.ElectionTimeout,
		lastHeartbeat:   time.Now(),
		stopCh:          make(chan struct{}),
		log:             cfg.Log,
		onBecomeLeader:  cfg.OnBecomeLeader,
		onLoseLeader:    cfg.OnLoseLeader,
	}
}

func (le *LeaderElector) AddPeer(nodeID, addr string) {
	le.mu.Lock()
	defer le.mu.Unlock()
	le.peers[nodeID] = addr
}

func (le *LeaderElector) RemovePeer(nodeID string) {
	le.mu.Lock()
	defer le.mu.Unlock()
	delete(le.peers, nodeID)
}

func (le *LeaderElector) Start() {
	go le.electionLoop()
	le.log.Info("leader election started for node %s", le.nodeID)
}

func (le *LeaderElector) Stop() {
	close(le.stopCh)
	le.log.Info("leader election stopped for node %s", le.nodeID)
}

func (le *LeaderElector) IsLeader() bool {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.state == StateLeader
}

func (le *LeaderElector) LeaderAddr() string {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.leaderAddr
}

func (le *LeaderElector) LeaderID() string {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.leaderID
}

func (le *LeaderElector) State() NodeState {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.state
}

func (le *LeaderElector) Term() uint64 {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.currentTerm
}

// HandleElection processes an incoming election message from a peer.
func (le *LeaderElector) HandleElection(msg *proto.LeaderElectionMessage, resp *proto.LeaderElectionResponse) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	if msg.Term > le.currentTerm {
		le.currentTerm = msg.Term
		if le.state == StateLeader {
			le.state = StateFollower
			le.log.Warn("stepping down: peer %s has higher term %d", msg.NodeID, msg.Term)
			if le.onLoseLeader != nil {
				go le.onLoseLeader()
			}
		}
	}

	if msg.IsLeader && msg.Term >= le.currentTerm {
		le.leaderID = msg.NodeID
		le.lastHeartbeat = time.Now()
		le.state = StateFollower
		resp.Accepted = true
		resp.LeaderID = msg.NodeID
		resp.Term = le.currentTerm
		return nil
	}

	if le.nodeID < msg.NodeID {
		resp.Accepted = false
		resp.LeaderID = le.nodeID
	} else {
		resp.Accepted = true
		resp.LeaderID = msg.NodeID
	}
	resp.Term = le.currentTerm
	return nil
}

// HandleHeartbeat processes a heartbeat from the current leader.
func (le *LeaderElector) HandleHeartbeat(msg *proto.LeaderElectionMessage, resp *proto.LeaderElectionResponse) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	if msg.Term >= le.currentTerm && msg.IsLeader {
		le.currentTerm = msg.Term
		le.leaderID = msg.NodeID
		le.lastHeartbeat = time.Now()
		if le.state == StateLeader && msg.NodeID != le.nodeID {
			le.state = StateFollower
			le.log.Warn("stepping down: received heartbeat from leader %s", msg.NodeID)
			if le.onLoseLeader != nil {
				go le.onLoseLeader()
			}
		}
		resp.Accepted = true
	} else {
		resp.Accepted = false
	}
	resp.LeaderID = le.leaderID
	resp.Term = le.currentTerm
	return nil
}

func (le *LeaderElector) electionLoop() {
	ticker := time.NewTicker(le.heartbeatTick)
	defer ticker.Stop()

	for {
		select {
		case <-le.stopCh:
			return
		case <-ticker.C:
			le.tick()
		}
	}
}

func (le *LeaderElector) tick() {
	le.mu.Lock()
	state := le.state
	elapsed := time.Since(le.lastHeartbeat)
	le.mu.Unlock()

	switch state {
	case StateLeader:
		le.sendHeartbeats()
	case StateFollower, StateCandidate:
		if elapsed > le.electionTimeout {
			le.startElection()
		}
	}
}

func (le *LeaderElector) startElection() {
	le.mu.Lock()
	le.currentTerm++
	le.state = StateCandidate
	term := le.currentTerm
	le.mu.Unlock()

	le.log.Info("starting election for term %d", term)

	votes := 1
	total := 1

	le.mu.RLock()
	peers := make(map[string]string, len(le.peers))
	for k, v := range le.peers {
		peers[k] = v
	}
	le.mu.RUnlock()

	for peerID, addr := range peers {
		total++
		msg := &proto.LeaderElectionMessage{
			NodeID:    le.nodeID,
			Term:      term,
			Timestamp: time.Now(),
			IsLeader:  false,
		}
		var resp proto.LeaderElectionResponse
		err := le.callPeer(addr, "ElectionService.RequestVote", msg, &resp)
		if err != nil {
			le.log.Debug("election RPC to %s failed: %v", peerID, err)
			continue
		}
		if resp.Accepted {
			votes++
		}
	}

	majority := (total / 2) + 1
	le.mu.Lock()
	defer le.mu.Unlock()

	if le.currentTerm != term {
		return
	}

	if votes >= majority {
		le.state = StateLeader
		le.leaderID = le.nodeID
		le.leaderAddr = le.listenAddr
		le.log.Info("elected as leader for term %d (%d/%d votes)", term, votes, total)
		if le.onBecomeLeader != nil {
			go le.onBecomeLeader()
		}
	} else {
		le.state = StateFollower
		le.log.Info("election for term %d failed (%d/%d votes)", term, votes, total)
	}
}

func (le *LeaderElector) sendHeartbeats() {
	le.mu.RLock()
	term := le.currentTerm
	peers := make(map[string]string, len(le.peers))
	for k, v := range le.peers {
		peers[k] = v
	}
	le.mu.RUnlock()

	msg := &proto.LeaderElectionMessage{
		NodeID:    le.nodeID,
		Term:      term,
		Timestamp: time.Now(),
		IsLeader:  true,
	}

	for peerID, addr := range peers {
		var resp proto.LeaderElectionResponse
		err := le.callPeer(addr, "ElectionService.Heartbeat", msg, &resp)
		if err != nil {
			le.log.Debug("heartbeat to %s failed: %v", peerID, err)
		}
	}
}

func (le *LeaderElector) callPeer(addr, method string, args interface{}, reply interface{}) error {
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer client.Close()

	done := make(chan *rpc.Call, 1)
	client.Go(method, args, reply, done)

	select {
	case call := <-done:
		return call.Error
	case <-time.After(2 * time.Second):
		return fmt.Errorf("rpc timeout to %s", addr)
	}
}
