package hash

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// Ring implements a consistent hashing ring with virtual nodes.
// It distributes keys across a set of physical nodes by mapping each
// physical node to multiple positions on a 32-bit hash space.
type Ring struct {
	mu           sync.RWMutex
	vnodeCount   int
	replicas     int
	sortedHashes []uint32
	ring         map[uint32]string
	nodeSet      map[string]bool
	nodeLoad     map[string]int
}

// NewRing creates a consistent hashing ring. vnodes controls how many
// virtual nodes each physical node receives on the ring. replicas controls
// how many distinct physical nodes a key should be assigned to.
func NewRing(vnodes, replicas int) *Ring {
	if vnodes < 1 {
		vnodes = 150
	}
	if replicas < 1 {
		replicas = 1
	}
	return &Ring{
		vnodeCount:   vnodes,
		replicas:     replicas,
		sortedHashes: make([]uint32, 0),
		ring:         make(map[uint32]string),
		nodeSet:      make(map[string]bool),
		nodeLoad:     make(map[string]int),
	}
}

// hashKey computes a 32-bit hash for the given key using SHA-256 truncation.
func hashKey(key string) uint32 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint32(h[:4])
}

// AddNode inserts a physical node into the ring. Each physical node gets
// vnodeCount positions spread across the hash space.
func (r *Ring) AddNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nodeSet[node] {
		return
	}

	r.nodeSet[node] = true
	r.nodeLoad[node] = 0

	for i := 0; i < r.vnodeCount; i++ {
		vkey := fmt.Sprintf("%s#vnode%d", node, i)
		h := hashKey(vkey)
		r.ring[h] = node
		r.sortedHashes = append(r.sortedHashes, h)
	}

	sort.Slice(r.sortedHashes, func(i, j int) bool {
		return r.sortedHashes[i] < r.sortedHashes[j]
	})
}

// RemoveNode removes a physical node and all its virtual nodes from the ring.
func (r *Ring) RemoveNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.nodeSet[node] {
		return
	}

	delete(r.nodeSet, node)
	delete(r.nodeLoad, node)

	// Rebuild sorted hashes excluding this node.
	newHashes := make([]uint32, 0, len(r.sortedHashes)-r.vnodeCount)
	for _, h := range r.sortedHashes {
		if r.ring[h] != node {
			newHashes = append(newHashes, h)
		} else {
			delete(r.ring, h)
		}
	}
	r.sortedHashes = newHashes
}

// GetNode returns the primary physical node responsible for the given key.
// Returns empty string if the ring is empty.
func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedHashes) == 0 {
		return ""
	}

	h := hashKey(key)
	idx := sort.Search(len(r.sortedHashes), func(i int) bool {
		return r.sortedHashes[i] >= h
	})

	if idx >= len(r.sortedHashes) {
		idx = 0
	}

	return r.ring[r.sortedHashes[idx]]
}

// GetNodes returns up to r.replicas distinct physical nodes for the given key.
// The first element is the primary; subsequent elements are replicas.
func (r *Ring) GetNodes(key string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedHashes) == 0 {
		return nil
	}

	h := hashKey(key)
	idx := sort.Search(len(r.sortedHashes), func(i int) bool {
		return r.sortedHashes[i] >= h
	})
	if idx >= len(r.sortedHashes) {
		idx = 0
	}

	seen := make(map[string]bool)
	result := make([]string, 0, r.replicas)

	for len(result) < r.replicas && len(seen) < len(r.nodeSet) {
		node := r.ring[r.sortedHashes[idx]]
		if !seen[node] {
			seen[node] = true
			result = append(result, node)
		}
		idx = (idx + 1) % len(r.sortedHashes)
	}

	return result
}

// Nodes returns all physical nodes currently in the ring.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]string, 0, len(r.nodeSet))
	for n := range r.nodeSet {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	return nodes
}

// Size returns the number of physical nodes in the ring.
func (r *Ring) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodeSet)
}

// HasNode checks whether a physical node exists in the ring.
func (r *Ring) HasNode(node string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodeSet[node]
}

// IncrementLoad atomically increments the load counter for a node.
func (r *Ring) IncrementLoad(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodeLoad[node]++
}

// DecrementLoad atomically decrements the load counter for a node.
func (r *Ring) DecrementLoad(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nodeLoad[node] > 0 {
		r.nodeLoad[node]--
	}
}

// GetLoad returns the current load counter for a node.
func (r *Ring) GetLoad(node string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodeLoad[node]
}

// GetLeastLoadedNode returns the node in the ring with the lowest load.
func (r *Ring) GetLeastLoadedNode() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodeSet) == 0 {
		return ""
	}

	var best string
	bestLoad := int(^uint(0) >> 1)
	for node := range r.nodeSet {
		load := r.nodeLoad[node]
		if load < bestLoad {
			bestLoad = load
			best = node
		}
	}
	return best
}

// Distribution returns a map of physical node to the number of virtual
// nodes it owns. Useful for diagnostics and rebalancing decisions.
func (r *Ring) Distribution() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	dist := make(map[string]int, len(r.nodeSet))
	for _, node := range r.ring {
		dist[node]++
	}
	return dist
}
