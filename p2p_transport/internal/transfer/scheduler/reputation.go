package scheduler

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"time"

	"cipher/internal/protocol/chunk"
)

type ReputationConfig struct {
	CorruptionPenalty  float64
	TimeoutPenalty     float64
	DefaultPenalty     float64
	SuccessReward      float64
	IsolationThreshold float64
	BanThreshold       float64
}

func DefaultReputationConfig() ReputationConfig {
	return ReputationConfig{
		CorruptionPenalty:  50.0,
		TimeoutPenalty:     25.0,
		DefaultPenalty:     10.0,
		SuccessReward:      5.0,
		IsolationThreshold: 50.0,
		BanThreshold:       50.0,
	}
}

type PeerReputation struct {
	PeerID            string    `json:"peer_id"`
	FaultScore        float64   `json:"fault_score"`
	FailureCount      int       `json:"failure_count"`
	ConsecutiveErrors int       `json:"consecutive_errors"`
	CorruptionCount   int       `json:"corruption_count"`
	TimeoutCount      int       `json:"timeout_count"`
	OtherErrorCount   int       `json:"other_error_count"`
	SuccessCount      int       `json:"success_count"`
	LastFailure       time.Time `json:"last_failure,omitempty"`
	IsIsolated        bool      `json:"is_isolated"`
}

type ReputationManager struct {
	mu     sync.RWMutex
	peers  map[string]*PeerReputation
	config ReputationConfig
}

func NewReputationManager(cfg ...ReputationConfig) *ReputationManager {
	c := DefaultReputationConfig()
	if len(cfg) > 0 {
		c = cfg[0]
	}
	return &ReputationManager{
		peers:  make(map[string]*PeerReputation),
		config: c,
	}
}

func (rm *ReputationManager) RecordSuccess(peerID string) {
	if peerID == "" {
		return
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rep, ok := rm.peers[peerID]
	if !ok {
		rep = &PeerReputation{PeerID: peerID}
		rm.peers[peerID] = rep
	}

	rep.SuccessCount++
	rep.ConsecutiveErrors = 0
	rep.FaultScore = math.Max(0.0, rep.FaultScore-rm.config.SuccessReward)
	if rep.FaultScore < rm.config.IsolationThreshold && rep.FaultScore < rm.config.BanThreshold {
		rep.IsIsolated = false
	}
}

func (rm *ReputationManager) RecordError(peerID string, err error) {
	if peerID == "" || err == nil {
		return
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rep, ok := rm.peers[peerID]
	if !ok {
		rep = &PeerReputation{PeerID: peerID}
		rm.peers[peerID] = rep
	}

	rep.FailureCount++
	rep.ConsecutiveErrors++
	rep.LastFailure = time.Now()

	errStr := strings.ToLower(err.Error())
	if errors.Is(err, chunk.ErrChunkCorrupted) || strings.Contains(errStr, "corrupted") || strings.Contains(errStr, "hash mismatch") {
		rep.CorruptionCount++
		rep.FaultScore += rm.config.CorruptionPenalty
	} else if errors.Is(err, context.DeadlineExceeded) || strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline") {
		rep.TimeoutCount++
		rep.FaultScore += rm.config.TimeoutPenalty
	} else {
		rep.OtherErrorCount++
		rep.FaultScore += rm.config.DefaultPenalty
	}

	if rep.FaultScore >= rm.config.IsolationThreshold || rep.FaultScore >= rm.config.BanThreshold {
		rep.IsIsolated = true
	}
}

func (rm *ReputationManager) IsExcluded(peerID string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	rep, ok := rm.peers[peerID]
	if !ok {
		return false
	}
	return rep.IsIsolated || rep.FaultScore >= rm.config.IsolationThreshold || rep.FaultScore >= rm.config.BanThreshold
}

func (rm *ReputationManager) IsIsolated(peerID string) bool {
	return rm.IsExcluded(peerID)
}

func (rm *ReputationManager) GetScore(peerID string) float64 {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rep, ok := rm.peers[peerID]; ok {
		return rep.FaultScore
	}
	return 0.0
}

func (rm *ReputationManager) GetReputation(peerID string) PeerReputation {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rep, ok := rm.peers[peerID]; ok {
		return *rep
	}
	return PeerReputation{PeerID: peerID}
}

func (rm *ReputationManager) GetAllReputations() map[string]PeerReputation {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	result := make(map[string]PeerReputation, len(rm.peers))
	for id, rep := range rm.peers {
		result[id] = *rep
	}
	return result
}
