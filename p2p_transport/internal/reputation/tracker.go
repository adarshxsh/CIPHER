package reputation

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

// TrackerConfig holds configuration settings for PeerReputationTracker.
type TrackerConfig struct {
	InitialScore     float64 // Initial score for untracked peers (default: 0.0)
	SuccessReward    float64 // Score added on chunk/manifest success (default: 1.0)
	FailurePenalty   float64 // Score subtracted on connection error (default: 1.0)
	IntegrityPenalty float64 // Score subtracted on hash mismatch (default: 5.0)
	BanThreshold     float64 // Score threshold below which a peer is banned/evicted (default: 0.0)
	MaxScore         float64 // Maximum cap for reputation score (default: 100.0)
}

// DefaultTrackerConfig returns standard default configuration values.
func DefaultTrackerConfig() TrackerConfig {
	return TrackerConfig{
		InitialScore:     0.0,
		SuccessReward:    1.0,
		FailurePenalty:   1.0,
		IntegrityPenalty: 5.0,
		BanThreshold:     0.0,
		MaxScore:         100.0,
	}
}

// PeerReputationTracker provides thread-safe in-memory tracking of peer reputation scores.
type PeerReputationTracker struct {
	mu     sync.RWMutex
	scores map[peer.ID]float64
	cfg    TrackerConfig
}

// NewPeerReputationTracker creates a new thread-safe PeerReputationTracker instance.
func NewPeerReputationTracker(cfg ...TrackerConfig) *PeerReputationTracker {
	c := DefaultTrackerConfig()
	if len(cfg) > 0 {
		c = cfg[0]
	}
	return &PeerReputationTracker{
		scores: make(map[peer.ID]float64),
		cfg:    c,
	}
}

// GetScore returns the current reputation score for a peer.
func (t *PeerReputationTracker) GetScore(p peer.ID) float64 {
	if t == nil {
		return 0.0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	score, ok := t.scores[p]
	if !ok {
		return t.cfg.InitialScore
	}
	return score
}

// IsBanned returns true if the peer's reputation score is strictly below the BanThreshold.
func (t *PeerReputationTracker) IsBanned(p peer.ID) bool {
	if t == nil {
		return false
	}
	return t.GetScore(p) < t.cfg.BanThreshold
}

// RecordSuccess increases the peer's reputation score upon successful chunk or manifest transfer.
func (t *PeerReputationTracker) RecordSuccess(p peer.ID) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	score, ok := t.scores[p]
	if !ok {
		score = t.cfg.InitialScore
	}
	score += t.cfg.SuccessReward
	if t.cfg.MaxScore > 0 && score > t.cfg.MaxScore {
		score = t.cfg.MaxScore
	}
	t.scores[p] = score
}

// RecordConnectionFailure decreases the peer's reputation score on network or connection errors.
func (t *PeerReputationTracker) RecordConnectionFailure(p peer.ID) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	score, ok := t.scores[p]
	if !ok {
		score = t.cfg.InitialScore
	}
	score -= t.cfg.FailurePenalty
	t.scores[p] = score
}

// RecordIntegrityFault decreases the peer's reputation score on checksum or payload integrity mismatches.
func (t *PeerReputationTracker) RecordIntegrityFault(p peer.ID) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	score, ok := t.scores[p]
	if !ok {
		score = t.cfg.InitialScore
	}
	score -= t.cfg.IntegrityPenalty
	t.scores[p] = score
}

// ResetPeer resets a peer's tracked reputation score.
func (t *PeerReputationTracker) ResetPeer(p peer.ID) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.scores, p)
}

// Clear removes all peer reputation scores.
func (t *PeerReputationTracker) Clear() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.scores = make(map[peer.ID]float64)
}

// Config returns a copy of the tracker configuration.
func (t *PeerReputationTracker) Config() TrackerConfig {
	if t == nil {
		return DefaultTrackerConfig()
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.cfg
}
