package scheduler

import (
	"math"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// PeerReputation tracks session-scoped reliability metrics and cooldown state for a single peer.
type PeerReputation struct {
	PeerID            peer.ID   `json:"peer_id"`
	Score             float64   `json:"score"`              // Reputation score (0.0 to 1.0)
	FailureCount      int       `json:"failure_count"`      // Total failures in session
	ConsecutiveErrors int       `json:"consecutive_errors"` // Current consecutive failure streak
	SuccessCount      int       `json:"success_count"`      // Total successful transfers
	LastFailure       time.Time `json:"last_failure"`       // Timestamp of last failure
	LastSuccess       time.Time `json:"last_success"`       // Timestamp of last success
	BackoffUntil      time.Time `json:"backoff_until"`      // Cooldown quarantine expiry
}

// ReputationConfig defines parameters for calculating backoff penalties and score updates.
type ReputationConfig struct {
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	InitialScore float64
	MaxScore     float64
	MinScore     float64
}

// DefaultReputationConfig returns default configuration for peer reputation tracking.
func DefaultReputationConfig() ReputationConfig {
	return ReputationConfig{
		BaseBackoff:  50 * time.Millisecond,
		MaxBackoff:   5 * time.Second,
		InitialScore: 1.0,
		MaxScore:     1.0,
		MinScore:     0.0,
	}
}

// ReputationManager manages thread-safe, in-memory peer reputation tracking.
type ReputationManager struct {
	mu      sync.RWMutex
	records map[peer.ID]*PeerReputation
	config  ReputationConfig
}

// NewReputationManager creates a new session-scoped reputation manager.
func NewReputationManager(cfg ...ReputationConfig) *ReputationManager {
	config := DefaultReputationConfig()
	if len(cfg) > 0 {
		if cfg[0].BaseBackoff > 0 {
			config.BaseBackoff = cfg[0].BaseBackoff
		}
		if cfg[0].MaxBackoff > 0 {
			config.MaxBackoff = cfg[0].MaxBackoff
		}
		if cfg[0].InitialScore > 0 {
			config.InitialScore = cfg[0].InitialScore
		}
		if cfg[0].MaxScore > 0 {
			config.MaxScore = cfg[0].MaxScore
		}
		config.MinScore = cfg[0].MinScore
	}
	return &ReputationManager{
		records: make(map[peer.ID]*PeerReputation),
		config:  config,
	}
}

// GetReputation returns a copy of the peer's reputation record.
func (rm *ReputationManager) GetReputation(p peer.ID) PeerReputation {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rec, exists := rm.records[p]; exists {
		return *rec
	}
	return PeerReputation{
		PeerID: p,
		Score:  rm.config.InitialScore,
	}
}

// RecordFailure records a transfer/integrity error for the peer, updates metrics, and returns the backoff duration.
func (rm *ReputationManager) RecordFailure(p peer.ID) time.Duration {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rec, exists := rm.records[p]
	if !exists {
		rec = &PeerReputation{
			PeerID: p,
			Score:  rm.config.InitialScore,
		}
		rm.records[p] = rec
	}

	rec.FailureCount++
	rec.ConsecutiveErrors++
	rec.LastFailure = time.Now()

	// Decrement reputation score on failure
	rec.Score = math.Max(rm.config.MinScore, rec.Score-0.2)

	backoff := rm.calculateBackoff(rec.ConsecutiveErrors)
	rec.BackoffUntil = rec.LastFailure.Add(backoff)
	return backoff
}

// RecordSuccess records a successful chunk transfer, resetting consecutive errors and restoring score.
func (rm *ReputationManager) RecordSuccess(p peer.ID) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rec, exists := rm.records[p]
	if !exists {
		rec = &PeerReputation{
			PeerID: p,
			Score:  rm.config.InitialScore,
		}
		rm.records[p] = rec
	}

	rec.SuccessCount++
	rec.ConsecutiveErrors = 0
	rec.LastSuccess = time.Now()

	// Incrementally restore reputation score on success
	rec.Score = math.Min(rm.config.MaxScore, rec.Score+0.1)
}

// IsInBackoff checks whether the given peer is currently undergoing exponential backoff quarantine.
func (rm *ReputationManager) IsInBackoff(p peer.ID) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	rec, exists := rm.records[p]
	if !exists {
		return false
	}
	return time.Now().Before(rec.BackoffUntil)
}

// GetBackoffRemaining returns the remaining cooldown duration for a peer in backoff, or 0 if active.
func (rm *ReputationManager) GetBackoffRemaining(p peer.ID) time.Duration {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	rec, exists := rm.records[p]
	if !exists {
		return 0
	}
	remaining := time.Until(rec.BackoffUntil)
	if remaining < 0 {
		return 0
	}
	return remaining
}

func (rm *ReputationManager) calculateBackoff(consecutiveErrors int) time.Duration {
	if consecutiveErrors <= 0 {
		return 0
	}
	shift := uint(consecutiveErrors - 1)
	if shift >= 30 {
		return rm.config.MaxBackoff
	}
	backoff := rm.config.BaseBackoff * (1 << shift)
	if backoff > rm.config.MaxBackoff || backoff <= 0 {
		return rm.config.MaxBackoff
	}
	return backoff
}
