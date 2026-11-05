package scheduler

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

const DefaultMaxPeerFaults = 3

// PeerTracker maintains session-scoped fault counters and quarantine status for peers.
type PeerTracker struct {
	mu          sync.RWMutex
	faults      map[peer.ID]int
	quarantined map[peer.ID]bool
	maxFaults   int
}

// NewPeerTracker creates a new session-scoped peer fault tracker.
func NewPeerTracker(maxFaults int) *PeerTracker {
	if maxFaults <= 0 {
		maxFaults = DefaultMaxPeerFaults
	}
	return &PeerTracker{
		faults:      make(map[peer.ID]int),
		quarantined: make(map[peer.ID]bool),
		maxFaults:   maxFaults,
	}
}

// RecordFault increments the fault counter for peerID and quarantines the peer if the fault threshold is reached.
// Returns the updated fault count and whether the peer is quarantined.
func (pt *PeerTracker) RecordFault(p peer.ID) (int, bool) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.faults[p]++
	count := pt.faults[p]
	if count >= pt.maxFaults {
		pt.quarantined[p] = true
	}
	return count, pt.quarantined[p]
}

// IsQuarantined checks if the specified peer is quarantined.
func (pt *PeerTracker) IsQuarantined(p peer.ID) bool {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	return pt.quarantined[p]
}

// FaultCount returns the current fault count for the specified peer.
func (pt *PeerTracker) FaultCount(p peer.ID) int {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	return pt.faults[p]
}
