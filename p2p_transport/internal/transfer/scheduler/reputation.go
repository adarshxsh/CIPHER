package scheduler

import (
	"errors"
	"sync"

	"cipher/internal/protocol/chunk"
)

const (
	DefaultQuarantineThreshold = 50.0
	DefaultCorruptionPenalty   = 50.0
	DefaultGeneralPenalty      = 25.0
	DefaultSuccessReward       = 5.0
)

type PeerReputation struct {
	FaultScore  float64
	Failures    int
	Successes   int
	Quarantined bool
}

type ReputationTrackerConfig struct {
	QuarantineThreshold float64
	CorruptionPenalty   float64
	GeneralPenalty      float64
	SuccessReward       float64
}

func DefaultReputationTrackerConfig() ReputationTrackerConfig {
	return ReputationTrackerConfig{
		QuarantineThreshold: DefaultQuarantineThreshold,
		CorruptionPenalty:   DefaultCorruptionPenalty,
		GeneralPenalty:      DefaultGeneralPenalty,
		SuccessReward:       DefaultSuccessReward,
	}
}

type ReputationTracker struct {
	mu    sync.RWMutex
	cfg   ReputationTrackerConfig
	peers map[string]*PeerReputation
}

func NewReputationTracker(cfg ...ReputationTrackerConfig) *ReputationTracker {
	c := DefaultReputationTrackerConfig()
	if len(cfg) > 0 {
		if cfg[0].QuarantineThreshold > 0 {
			c.QuarantineThreshold = cfg[0].QuarantineThreshold
		}
		if cfg[0].CorruptionPenalty > 0 {
			c.CorruptionPenalty = cfg[0].CorruptionPenalty
		}
		if cfg[0].GeneralPenalty > 0 {
			c.GeneralPenalty = cfg[0].GeneralPenalty
		}
		if cfg[0].SuccessReward > 0 {
			c.SuccessReward = cfg[0].SuccessReward
		}
	}
	return &ReputationTracker{
		cfg:   c,
		peers: make(map[string]*PeerReputation),
	}
}

func (r *ReputationTracker) getOrCreate(peerID string) *PeerReputation {
	rep, ok := r.peers[peerID]
	if !ok {
		rep = &PeerReputation{}
		r.peers[peerID] = rep
	}
	return rep
}

func (r *ReputationTracker) RecordSuccess(peerID string) {
	if peerID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	rep := r.getOrCreate(peerID)
	rep.Successes++
	rep.FaultScore -= r.cfg.SuccessReward
	if rep.FaultScore < 0.0 {
		rep.FaultScore = 0.0
	}
}

func (r *ReputationTracker) RecordFailure(peerID string, err error) bool {
	if peerID == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	rep := r.getOrCreate(peerID)
	rep.Failures++

	var penalty float64
	if errors.Is(err, chunk.ErrChunkCorrupted) {
		penalty = r.cfg.CorruptionPenalty
	} else {
		penalty = r.cfg.GeneralPenalty
	}

	rep.FaultScore += penalty
	if rep.FaultScore >= r.cfg.QuarantineThreshold {
		rep.Quarantined = true
	}
	return rep.Quarantined
}

func (r *ReputationTracker) IsQuarantined(peerID string) bool {
	if peerID == "" {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	rep, ok := r.peers[peerID]
	if !ok {
		return false
	}
	return rep.Quarantined
}

func (r *ReputationTracker) GetFaultScore(peerID string) float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	rep, ok := r.peers[peerID]
	if !ok {
		return 0.0
	}
	return rep.FaultScore
}

func (r *ReputationTracker) GetReputation(peerID string) (PeerReputation, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	rep, ok := r.peers[peerID]
	if !ok {
		return PeerReputation{}, false
	}
	return *rep, true
}
