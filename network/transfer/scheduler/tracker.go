package scheduler

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	DefaultRewardSuccess         = 5
	DefaultPenaltyNetworkError   = -10
	DefaultPenaltyIntegrityError = -50
	DefaultBanThreshold          = -100
	DefaultMaxIntegrityFailures  = 2
	DefaultBaseBackoff           = 50 * time.Millisecond
	DefaultMaxBackoff            = 10 * time.Second
)

// PeerStats holds the in-memory reputation metrics for a single peer.
type PeerStats struct {
	PeerID              peer.ID
	Successes           int
	Timeouts            int
	IntegrityFailures   int
	Score               int
	ConsecutiveFailures int
	IsBanned            bool
}

// PeerTracker maintains in-memory peer reputation scores, penalty thresholds, and backoff state.
type PeerTracker struct {
	mu                     sync.Mutex
	stats                  map[peer.ID]*PeerStats
	RewardSuccess          int
	PenaltyNetworkError    int
	PenaltyIntegrityError  int
	BanThreshold           int
	MaxIntegrityFailures   int
	BaseBackoff            time.Duration
	MaxBackoff             time.Duration
}

// NewPeerTracker creates a new in-memory PeerTracker initialized with default thresholds.
func NewPeerTracker() *PeerTracker {
	return &PeerTracker{
		stats:                 make(map[peer.ID]*PeerStats),
		RewardSuccess:         DefaultRewardSuccess,
		PenaltyNetworkError:   DefaultPenaltyNetworkError,
		PenaltyIntegrityError: DefaultPenaltyIntegrityError,
		BanThreshold:          DefaultBanThreshold,
		MaxIntegrityFailures:  DefaultMaxIntegrityFailures,
		BaseBackoff:           DefaultBaseBackoff,
		MaxBackoff:            DefaultMaxBackoff,
	}
}

func (pt *PeerTracker) getOrCreateStats(peerID peer.ID) *PeerStats {
	ps, exists := pt.stats[peerID]
	if !exists {
		ps = &PeerStats{
			PeerID: peerID,
			Score:  0,
		}
		pt.stats[peerID] = ps
	}
	return ps
}

// RecordSuccess rewards a peer for a successful chunk delivery.
func (pt *PeerTracker) RecordSuccess(peerID peer.ID) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	ps := pt.getOrCreateStats(peerID)
	ps.Successes++
	ps.ConsecutiveFailures = 0
	ps.Score += pt.RewardSuccess
}

// RecordTimeout penalizes a peer for a timeout or network error (-10 by default).
func (pt *PeerTracker) RecordTimeout(peerID peer.ID) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	ps := pt.getOrCreateStats(peerID)
	ps.Timeouts++
	ps.ConsecutiveFailures++
	ps.Score += pt.PenaltyNetworkError
	if ps.Score <= pt.BanThreshold {
		ps.IsBanned = true
	}
}

// RecordIntegrityFailure penalizes a peer for returning corrupted data (-50 by default).
func (pt *PeerTracker) RecordIntegrityFailure(peerID peer.ID) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	ps := pt.getOrCreateStats(peerID)
	ps.IntegrityFailures++
	ps.ConsecutiveFailures++
	ps.Score += pt.PenaltyIntegrityError
	if ps.Score <= pt.BanThreshold || (pt.MaxIntegrityFailures > 0 && ps.IntegrityFailures >= pt.MaxIntegrityFailures) {
		ps.IsBanned = true
	}
}

// GetScore returns the current score for a peer.
func (pt *PeerTracker) GetScore(peerID peer.ID) int {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	ps, exists := pt.stats[peerID]
	if !exists {
		return 0
	}
	return ps.Score
}

// IsBanned returns true if the peer is isolated or banned.
func (pt *PeerTracker) IsBanned(peerID peer.ID) bool {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	ps, exists := pt.stats[peerID]
	if !exists {
		return false
	}
	return ps.IsBanned
}

// GetBackoff calculates exponential backoff delay based on consecutive failure count for peers with negative scores.
func (pt *PeerTracker) GetBackoff(peerID peer.ID) time.Duration {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	ps, exists := pt.stats[peerID]
	if !exists || ps.Score >= 0 || ps.ConsecutiveFailures <= 0 {
		return 0
	}

	exp := ps.ConsecutiveFailures - 1
	if exp > 10 {
		exp = 10
	}
	backoff := pt.BaseBackoff * time.Duration(1<<exp)
	if backoff > pt.MaxBackoff {
		backoff = pt.MaxBackoff
	}
	return backoff
}

// GetStats returns a copy of PeerStats for a peer.
func (pt *PeerTracker) GetStats(peerID peer.ID) PeerStats {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	ps, exists := pt.stats[peerID]
	if !exists {
		return PeerStats{PeerID: peerID}
	}
	return *ps
}

// GetAllStats returns a snapshot map of all tracked peer stats.
func (pt *PeerTracker) GetAllStats() map[peer.ID]PeerStats {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	result := make(map[peer.ID]PeerStats, len(pt.stats))
	for k, v := range pt.stats {
		result[k] = *v
	}
	return result
}
