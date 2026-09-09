package reputation

import (
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// ReputationConfig defines the parameters for tracking peer reputation and backoff.
type ReputationConfig struct {
	BaseBackoff         time.Duration // Base backoff duration (default 5s)
	MaxBackoff          time.Duration // Maximum backoff duration (default 45s)
	BackoffMultiplier   float64       // Backoff multiplier (default 3.0)
	QuarantineThreshold int           // Consecutive integrity failures before quarantine (default 3)
	PenaltyIntegrity    int           // Penalty score increment for integrity failures (default 10)
	PenaltyTransient    int           // Penalty score increment for transient failures (default 1)
}

// DefaultConfig returns the default configuration for ReputationManager.
func DefaultConfig() ReputationConfig {
	return ReputationConfig{
		BaseBackoff:         5 * time.Second,
		MaxBackoff:          45 * time.Second,
		BackoffMultiplier:   3.0,
		QuarantineThreshold: 3,
		PenaltyIntegrity:    10,
		PenaltyTransient:    1,
	}
}

// PeerState holds the in-memory reputation metrics and backoff/quarantine status for a peer.
type PeerState struct {
	PeerID              peer.ID
	PenaltyScore        int
	ConsecutiveFailures int
	IsQuarantined       bool
	BackoffUntil        time.Time
	LastFailureTime     time.Time
	IntegrityFailures   int
	TransientFailures   int
	Successes           int
}

// ReputationManager provides thread-safe in-memory peer reputation tracking.
type ReputationManager struct {
	mu     sync.RWMutex
	config ReputationConfig
	peers  map[peer.ID]*PeerState
}

// NewReputationManager creates a new ReputationManager instance.
func NewReputationManager(config ...ReputationConfig) *ReputationManager {
	cfg := DefaultConfig()
	if len(config) > 0 {
		cfg = config[0]
	}
	return &ReputationManager{
		config: cfg,
		peers:  make(map[peer.ID]*PeerState),
	}
}

func (rm *ReputationManager) getOrCreatePeerStateLocked(peerID peer.ID) *PeerState {
	state, ok := rm.peers[peerID]
	if !ok {
		state = &PeerState{
			PeerID: peerID,
		}
		rm.peers[peerID] = state
	}
	return state
}

func (rm *ReputationManager) calculateBackoff(consecutiveFailures int) time.Duration {
	if consecutiveFailures <= 0 {
		return 0
	}
	multiplier := 1.0
	for i := 1; i < consecutiveFailures; i++ {
		multiplier *= rm.config.BackoffMultiplier
	}
	backoff := time.Duration(float64(rm.config.BaseBackoff) * multiplier)
	if backoff > rm.config.MaxBackoff {
		backoff = rm.config.MaxBackoff
	}
	return backoff
}

// RecordIntegrityFailure records an integrity mismatch failure for a peer.
func (rm *ReputationManager) RecordIntegrityFailure(peerID peer.ID) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	state := rm.getOrCreatePeerStateLocked(peerID)
	state.PenaltyScore += rm.config.PenaltyIntegrity
	state.ConsecutiveFailures++
	state.IntegrityFailures++
	state.LastFailureTime = time.Now()

	if state.ConsecutiveFailures >= rm.config.QuarantineThreshold {
		state.IsQuarantined = true
	} else {
		backoff := rm.calculateBackoff(state.ConsecutiveFailures)
		state.BackoffUntil = time.Now().Add(backoff)
	}
}

// RecordTransientFailure records a transient (e.g. network/timeout) failure for a peer.
func (rm *ReputationManager) RecordTransientFailure(peerID peer.ID) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	state := rm.getOrCreatePeerStateLocked(peerID)
	state.PenaltyScore += rm.config.PenaltyTransient
	state.ConsecutiveFailures++
	state.TransientFailures++
	state.LastFailureTime = time.Now()

	backoff := rm.calculateBackoff(state.ConsecutiveFailures)
	state.BackoffUntil = time.Now().Add(backoff)
}

// RecordSuccess records a successful chunk fetch from a peer, resetting consecutive failures for non-quarantined peers.
func (rm *ReputationManager) RecordSuccess(peerID peer.ID) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	state, ok := rm.peers[peerID]
	if !ok {
		return
	}
	state.Successes++
	if !state.IsQuarantined {
		state.ConsecutiveFailures = 0
	}
}

// RecordError records a failure based on the error type (integrity mismatch vs transient).
func (rm *ReputationManager) RecordError(peerID peer.ID, err error) {
	if err == nil {
		return
	}
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "corrupted") || strings.Contains(errStr, "integrity") || strings.Contains(errStr, "mismatch") {
		rm.RecordIntegrityFailure(peerID)
	} else {
		rm.RecordTransientFailure(peerID)
	}
}

// IsQuarantined checks if a peer is in quarantine isolation.
func (rm *ReputationManager) IsQuarantined(peerID peer.ID) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if state, ok := rm.peers[peerID]; ok {
		return state.IsQuarantined
	}
	return false
}

// IsBackedOff checks if a peer is currently in backoff or quarantine.
func (rm *ReputationManager) IsBackedOff(peerID peer.ID) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if state, ok := rm.peers[peerID]; ok {
		if state.IsQuarantined {
			return true
		}
		return time.Now().Before(state.BackoffUntil)
	}
	return false
}

// ShouldSkip checks whether a peer should be bypassed (either quarantined or currently backed off).
func (rm *ReputationManager) ShouldSkip(peerID peer.ID) bool {
	return rm.IsBackedOff(peerID)
}

// BackoffRemaining returns the duration remaining for a peer's backoff state.
func (rm *ReputationManager) BackoffRemaining(peerID peer.ID) time.Duration {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if state, ok := rm.peers[peerID]; ok {
		if state.IsQuarantined {
			return time.Duration(1<<63 - 1) // Maximum duration for quarantined peers
		}
		rem := time.Until(state.BackoffUntil)
		if rem > 0 {
			return rem
		}
	}
	return 0
}

// GetScore returns the current numerical penalty score of a peer.
func (rm *ReputationManager) GetScore(peerID peer.ID) int {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if state, ok := rm.peers[peerID]; ok {
		return state.PenaltyScore
	}
	return 0
}

// GetPeerState returns a copy of the PeerState for a given peer ID.
func (rm *ReputationManager) GetPeerState(peerID peer.ID) (PeerState, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if state, ok := rm.peers[peerID]; ok {
		return *state, true
	}
	return PeerState{}, false
}

// ResetPeer clears the recorded reputation state for a peer.
func (rm *ReputationManager) ResetPeer(peerID peer.ID) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	delete(rm.peers, peerID)
}
