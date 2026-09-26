package scheduler

import (
	"errors"
	"math"
	"strings"
	"sync"
	"time"

	"cipher/internal/protocol/chunk"
)

type PeerStatus string

const (
	StatusActive      PeerStatus = "Active"
	StatusCooldown    PeerStatus = "Cooldown"
	StatusBlacklisted PeerStatus = "Blacklisted"
)

type PeerReputation struct {
	PeerID            string
	Score             float64
	Status            PeerStatus
	SuccessCount      int64
	FailureCount      int64
	ChecksumFailures  int64
	NetworkTimeouts   int64
	ConsecutiveErrors int
	BackoffUntil      time.Time
	LastSuccess       time.Time
	LastFailure       time.Time
}

type ReputationConfig struct {
	InitialScore           float64
	MaxScore               float64
	SuccessReward          float64
	ChecksumPenalty        float64
	TimeoutPenalty         float64
	CooldownThreshold      float64
	BlacklistThreshold     float64
	MaxConsecutiveFailures int
	BaseBackoff            time.Duration
	MaxBackoff             time.Duration
}

func DefaultReputationConfig() ReputationConfig {
	return ReputationConfig{
		InitialScore:           100.0,
		MaxScore:               100.0,
		SuccessReward:          5.0,
		ChecksumPenalty:        35.0, // 3 consecutive failures = -105, dropping below BlacklistThreshold (0.0)
		TimeoutPenalty:         15.0,
		CooldownThreshold:      50.0,
		BlacklistThreshold:     0.0,
		MaxConsecutiveFailures: 3,
		BaseBackoff:            500 * time.Millisecond,
		MaxBackoff:             30 * time.Second,
	}
}

type ReputationManager struct {
	mu     sync.RWMutex
	config ReputationConfig
	peers  map[string]*PeerReputation
}

func NewReputationManager(cfg ...ReputationConfig) *ReputationManager {
	c := DefaultReputationConfig()
	if len(cfg) > 0 {
		c = cfg[0]
	}
	return &ReputationManager{
		config: c,
		peers:  make(map[string]*PeerReputation),
	}
}

func (rm *ReputationManager) getOrCreatePeerLocked(peerID string) *PeerReputation {
	p, exists := rm.peers[peerID]
	if !exists {
		p = &PeerReputation{
			PeerID: peerID,
			Score:  rm.config.InitialScore,
			Status: StatusActive,
		}
		rm.peers[peerID] = p
	}
	return p
}

func (rm *ReputationManager) RecordSuccess(peerID string) {
	if peerID == "" {
		return
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	p := rm.getOrCreatePeerLocked(peerID)
	p.SuccessCount++
	p.ConsecutiveErrors = 0
	p.LastSuccess = time.Now()

	p.Score += rm.config.SuccessReward
	if p.Score > rm.config.MaxScore {
		p.Score = rm.config.MaxScore
	}

	if p.Status == StatusCooldown && time.Now().After(p.BackoffUntil) && p.Score >= rm.config.CooldownThreshold {
		p.Status = StatusActive
	}
}

func (rm *ReputationManager) RecordChecksumFailure(peerID string) {
	if peerID == "" {
		return
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	p := rm.getOrCreatePeerLocked(peerID)
	p.FailureCount++
	p.ChecksumFailures++
	p.ConsecutiveErrors++
	p.LastFailure = time.Now()

	p.Score -= rm.config.ChecksumPenalty

	backoff := rm.calculateBackoffLocked(p.ConsecutiveErrors)
	p.BackoffUntil = time.Now().Add(backoff)

	if p.ConsecutiveErrors >= rm.config.MaxConsecutiveFailures || p.Score <= rm.config.BlacklistThreshold {
		p.Status = StatusBlacklisted
	} else if p.Score < rm.config.CooldownThreshold || p.ConsecutiveErrors > 0 {
		p.Status = StatusCooldown
	}
}

func (rm *ReputationManager) RecordTimeout(peerID string) {
	if peerID == "" {
		return
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	p := rm.getOrCreatePeerLocked(peerID)
	p.FailureCount++
	p.NetworkTimeouts++
	p.ConsecutiveErrors++
	p.LastFailure = time.Now()

	p.Score -= rm.config.TimeoutPenalty

	backoff := rm.calculateBackoffLocked(p.ConsecutiveErrors)
	p.BackoffUntil = time.Now().Add(backoff)

	if p.ConsecutiveErrors >= rm.config.MaxConsecutiveFailures || p.Score <= rm.config.BlacklistThreshold {
		p.Status = StatusBlacklisted
	} else if p.Score < rm.config.CooldownThreshold || p.ConsecutiveErrors > 0 {
		p.Status = StatusCooldown
	}
}

func (rm *ReputationManager) RecordFailure(peerID string, err error) {
	if err == nil {
		rm.RecordSuccess(peerID)
		return
	}
	if errors.Is(err, chunk.ErrChunkIntegrityMismatch) ||
		strings.Contains(err.Error(), "corrupted chunk") ||
		strings.Contains(err.Error(), "hash mismatch") ||
		strings.Contains(err.Error(), "integrity mismatch") {
		rm.RecordChecksumFailure(peerID)
	} else {
		rm.RecordTimeout(peerID)
	}
}

func (rm *ReputationManager) GetStatus(peerID string) PeerStatus {
	rm.mu.RLock()
	p, exists := rm.peers[peerID]
	if !exists {
		rm.mu.RUnlock()
		return StatusActive
	}
	status := p.Status
	backoffUntil := p.BackoffUntil
	rm.mu.RUnlock()

	if status == StatusCooldown && time.Now().After(backoffUntil) {
		rm.mu.Lock()
		if p.Status == StatusCooldown && time.Now().After(p.BackoffUntil) {
			if p.Score >= rm.config.CooldownThreshold {
				p.Status = StatusActive
			}
			status = p.Status
		}
		rm.mu.Unlock()
	}

	return status
}

func (rm *ReputationManager) GetScore(peerID string) float64 {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	p, exists := rm.peers[peerID]
	if !exists {
		return rm.config.InitialScore
	}
	return p.Score
}

func (rm *ReputationManager) IsBlacklisted(peerID string) bool {
	return rm.GetStatus(peerID) == StatusBlacklisted
}

func (rm *ReputationManager) IsInCooldown(peerID string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	p, exists := rm.peers[peerID]
	if !exists {
		return false
	}
	return p.Status == StatusCooldown && time.Now().Before(p.BackoffUntil)
}

func (rm *ReputationManager) IsExcluded(peerID string) bool {
	status := rm.GetStatus(peerID)
	if status == StatusBlacklisted {
		return true
	}
	if status == StatusCooldown {
		return rm.IsInCooldown(peerID)
	}
	return false
}

func (rm *ReputationManager) IsAllowed(peerID string) bool {
	return !rm.IsExcluded(peerID)
}

func (rm *ReputationManager) GetStats(peerID string) (PeerReputation, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	p, exists := rm.peers[peerID]
	if !exists {
		return PeerReputation{}, false
	}
	return *p, true
}

func (rm *ReputationManager) ResetPeer(peerID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	delete(rm.peers, peerID)
}

func (rm *ReputationManager) calculateBackoffLocked(attempts int) time.Duration {
	if attempts <= 0 {
		return 0
	}
	factor := math.Pow(2, float64(attempts-1))
	backoff := time.Duration(float64(rm.config.BaseBackoff) * factor)
	if backoff > rm.config.MaxBackoff {
		backoff = rm.config.MaxBackoff
	}
	return backoff
}
