package distribution

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
)

type ReplicaState uint8

const (
	ReplicaPending ReplicaState = iota
	ReplicaUploading
	ReplicaCommitted
	ReplicaFailed
)

// GlobalReplicaTracker tracks chunk replicas across all providers to guarantee the R-replication invariant.
type GlobalReplicaTracker struct {
	mu        sync.RWMutex
	state     map[core.ChunkID]map[peer.ID]ReplicaState
	requiredR int
}

var peerMapPool = sync.Pool{
	New: func() any {
		return make(map[peer.ID]ReplicaState)
	},
}

func getPeerMap() map[peer.ID]ReplicaState {
	m := peerMapPool.Get().(map[peer.ID]ReplicaState)
	for k := range m {
		delete(m, k)
	}
	return m
}

func putPeerMap(m map[peer.ID]ReplicaState) {
	if m == nil {
		return
	}
	for k := range m {
		delete(m, k)
	}
	peerMapPool.Put(m)
}

// NewGlobalReplicaTracker initializes a GlobalReplicaTracker with required replication degree requiredR.
// Optional initialChunks can be passed to pre-allocate peer replica map state for known chunks:
// - core.ChunkID (individual chunk ID)
// - []core.ChunkID (slice of chunk IDs)
// - int (capacity hint for the chunk state map)
func NewGlobalReplicaTracker(requiredR int, initialChunks ...any) *GlobalReplicaTracker {
	t := &GlobalReplicaTracker{
		requiredR: requiredR,
	}

	var chunks []core.ChunkID
	for _, item := range initialChunks {
		switch v := item.(type) {
		case core.ChunkID:
			chunks = append(chunks, v)
		case []core.ChunkID:
			chunks = append(chunks, v...)
		case int:
			if t.state == nil && v > 0 {
				t.state = make(map[core.ChunkID]map[peer.ID]ReplicaState, v)
			}
		}
	}

	if t.state == nil {
		t.state = make(map[core.ChunkID]map[peer.ID]ReplicaState, len(chunks))
	}

	for _, cid := range chunks {
		if t.state[cid] == nil {
			t.state[cid] = getPeerMap()
		}
	}

	return t
}

// Preallocate pre-allocates inner peer replica maps for the provided chunk IDs using the sync.Pool.
func (t *GlobalReplicaTracker) Preallocate(items ...any) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, item := range items {
		switch v := item.(type) {
		case core.ChunkID:
			if t.state[v] == nil {
				t.state[v] = getPeerMap()
			}
		case []core.ChunkID:
			for _, cid := range v {
				if t.state[cid] == nil {
					t.state[cid] = getPeerMap()
				}
			}
		}
	}
}

func (t *GlobalReplicaTracker) SetStatus(chunkID core.ChunkID, p peer.ID, status ReplicaState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state[chunkID] == nil {
		t.state[chunkID] = getPeerMap()
	}
	t.state[chunkID][p] = status
}

func (t *GlobalReplicaTracker) GetStatus(chunkID core.ChunkID, p peer.ID) ReplicaState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if peerMap, ok := t.state[chunkID]; ok {
		return peerMap[p]
	}
	return ReplicaPending
}

func (t *GlobalReplicaTracker) CommittedCount(chunkID core.ChunkID) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	count := 0
	for _, status := range t.state[chunkID] {
		if status == ReplicaCommitted {
			count++
		}
	}
	return count
}

func (t *GlobalReplicaTracker) IsChunkSatisfied(chunkID core.ChunkID) bool {
	return t.CommittedCount(chunkID) >= t.requiredR
}

// IsComplete returns true if and only if EVERY chunk in chunkIDs has >= requiredR committed replicas.
func (t *GlobalReplicaTracker) IsComplete(chunkIDs []core.ChunkID) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, id := range chunkIDs {
		committedCount := 0
		for _, status := range t.state[id] {
			if status == ReplicaCommitted {
				committedCount++
			}
		}
		if committedCount < t.requiredR {
			return false
		}
	}
	return true
}

func (t *GlobalReplicaTracker) GetSummary(chunkIDs []core.ChunkID) (satisfied int, total int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	total = len(chunkIDs)
	for _, id := range chunkIDs {
		count := 0
		for _, status := range t.state[id] {
			if status == ReplicaCommitted {
				count++
			}
		}
		if count >= t.requiredR {
			satisfied++
		}
	}
	return satisfied, total
}

// Reset clears all tracked state and recycles inner peer replica maps back to the sync.Pool.
func (t *GlobalReplicaTracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, peerMap := range t.state {
		putPeerMap(peerMap)
		delete(t.state, id)
	}
}

// Release returns allocated replica maps to the sync.Pool.
func (t *GlobalReplicaTracker) Release() {
	t.Reset()
}
