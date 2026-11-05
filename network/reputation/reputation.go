package reputation

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Config defines configuration settings for the PeerReputationManager.
type Config struct {
	BanThreshold   float64       // Score at or below which a peer is considered banned (default: -30.0)
	SuccessReward  float64       // Score bonus added on chunk validation/storage success (default: 5.0)
	FailurePenalty float64       // Score penalty subtracted on failure/corrupt chunk (default: 10.0)
	DecayInterval  time.Duration // Time interval between score decays (default: 5 minutes)
	DecayFactor    float64       // Decay factor applied to negative scores (default: 0.5, halving penalties)
	MaxScore       float64       // Maximum reputation score cap (default: 100.0)
}

// DefaultConfig returns the standard reputation management configuration.
func DefaultConfig() Config {
	return Config{
		BanThreshold:   -30.0,
		SuccessReward:  5.0,
		FailurePenalty: 10.0,
		DecayInterval:  5 * time.Minute,
		DecayFactor:    0.5,
		MaxScore:       100.0,
	}
}

// PeerReputationManager maintains in-memory reputation scores for peers,
// supporting score updates, decay, and O(1) ban status checks.
type PeerReputationManager struct {
	mu       sync.RWMutex
	scores   map[peer.ID]float64
	config   Config
	stopChan chan struct{}
}

// NewPeerReputationManager creates a new manager with default configuration.
func NewPeerReputationManager() *PeerReputationManager {
	return NewPeerReputationManagerWithConfig(DefaultConfig())
}

// NewPeerReputationManagerWithConfig creates a new manager with custom configuration.
func NewPeerReputationManagerWithConfig(cfg Config) *PeerReputationManager {
	if cfg.DecayFactor <= 0 || cfg.DecayFactor >= 1 {
		cfg.DecayFactor = 0.5
	}
	if cfg.MaxScore <= 0 {
		cfg.MaxScore = 100.0
	}
	if cfg.FailurePenalty <= 0 {
		cfg.FailurePenalty = 10.0
	}
	if cfg.SuccessReward <= 0 {
		cfg.SuccessReward = 5.0
	}
	if cfg.BanThreshold >= 0 {
		cfg.BanThreshold = -30.0
	}

	return &PeerReputationManager{
		scores:   make(map[peer.ID]float64),
		config:   cfg,
		stopChan: make(chan struct{}),
	}
}

// RecordSuccess rewards a peer score on successful chunk validation and storage.
func (m *PeerReputationManager) RecordSuccess(p peer.ID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	score := m.scores[p]
	score += m.config.SuccessReward
	if score > m.config.MaxScore {
		score = m.config.MaxScore
	}
	m.scores[p] = score
}

// RecordFailure penalizes a peer score on error or corrupted chunk delivery.
func (m *PeerReputationManager) RecordFailure(p peer.ID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	score := m.scores[p]
	score -= m.config.FailurePenalty
	m.scores[p] = score
}

// IsBanned checks if a peer's score is at or below the ban threshold in O(1) time.
func (m *PeerReputationManager) IsBanned(p peer.ID) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	score, exists := m.scores[p]
	if !exists {
		return false
	}
	return score <= m.config.BanThreshold
}

// GetScore returns the current score of a peer.
func (m *PeerReputationManager) GetScore(p peer.ID) float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.scores[p]
}

// SetScore directly sets a peer's score (primarily for testing).
func (m *PeerReputationManager) SetScore(p peer.ID, score float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.scores[p] = score
}

// Decay applies score decay by decaying negative score penalties toward 0.
func (m *PeerReputationManager) Decay() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for p, score := range m.scores {
		if score < 0 {
			score *= m.config.DecayFactor
			if score > -0.001 {
				score = 0
			}
			m.scores[p] = score
		}
	}
}

// StartDecayLoop starts a background ticker for periodic score decay until the context is canceled or Close is called.
func (m *PeerReputationManager) StartDecayLoop(ctx context.Context) {
	if m.config.DecayInterval <= 0 {
		return
	}
	ticker := time.NewTicker(m.config.DecayInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-m.stopChan:
				return
			case <-ticker.C:
				m.Decay()
			}
		}
	}()
}

// Close stops any background decay loops.
func (m *PeerReputationManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case <-m.stopChan:
		// Already closed
	default:
		close(m.stopChan)
	}
}
