package scheduler

import (
	"math"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// ReputationConfig holds configuration parameters for peer reputation scoring and banning.
type ReputationConfig struct {
	// BanThreshold is the fault score or consecutive error count threshold that triggers a ban. Default is 3.0.
	BanThreshold float64
	// BanDuration is how long a peer remains banned when exceeding BanThreshold. Default is 5 minutes.
	BanDuration time.Duration
	// FaultWeight is the score added when a fault occurs. Default is 1.0.
	FaultWeight float64
	// DecayHalfLife is the half-life duration for exponential score decay over time. Default is 10 minutes.
	DecayHalfLife time.Duration
}

// DefaultReputationConfig returns the default configuration for peer reputation.
func DefaultReputationConfig() ReputationConfig {
	return ReputationConfig{
		BanThreshold:  3.0,
		BanDuration:   5 * time.Minute,
		FaultWeight:   1.0,
		DecayHalfLife: 10 * time.Minute,
	}
}

// PeerScore contains metrics and state for a tracked peer.
type PeerScore struct {
	FaultScore        float64
	ConsecutiveErrors int
	TotalFaults       int
	TotalSuccesses    int
	LastUpdated       time.Time
	BannedUntil       time.Time
}

// ReputationManager tracks peer fault scores and handles temporary peer banning.
type ReputationManager struct {
	config ReputationConfig
	scores map[string]*PeerScore
	mu     sync.RWMutex
}

// NewReputationManager creates a new ReputationManager with the given configuration options.
func NewReputationManager(configs ...ReputationConfig) *ReputationManager {
	cfg := DefaultReputationConfig()
	if len(configs) > 0 {
		cfg = configs[0]
	}
	return &ReputationManager{
		config: cfg,
		scores: make(map[string]*PeerScore),
	}
}

func (rm *ReputationManager) applyDecay(s *PeerScore, now time.Time) {
	if s.LastUpdated.IsZero() {
		s.LastUpdated = now
		return
	}
	if now.Before(s.LastUpdated) {
		return
	}
	elapsed := now.Sub(s.LastUpdated)
	if rm.config.DecayHalfLife > 0 && elapsed > 0 {
		decayFactor := math.Pow(0.5, float64(elapsed)/float64(rm.config.DecayHalfLife))
		s.FaultScore *= decayFactor
	}
	s.LastUpdated = now
}

// RecordFaultAt records a chunk validation error or fault for a peer at a specific time.
func (rm *ReputationManager) RecordFaultAt(peerID string, now time.Time) {
	if peerID == "" {
		return
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	s, ok := rm.scores[peerID]
	if !ok {
		s = &PeerScore{LastUpdated: now}
		rm.scores[peerID] = s
	}

	rm.applyDecay(s, now)

	s.FaultScore += rm.config.FaultWeight
	s.ConsecutiveErrors++
	s.TotalFaults++

	if s.FaultScore >= rm.config.BanThreshold-1e-6 || s.ConsecutiveErrors >= int(rm.config.BanThreshold) {
		s.BannedUntil = now.Add(rm.config.BanDuration)
	}
}

// RecordFault records a fault for a peer using current time.
func (rm *ReputationManager) RecordFault(peerID string) {
	rm.RecordFaultAt(peerID, time.Now())
}

// RecordFaultForPeer records a fault for a libp2p peer ID.
func (rm *ReputationManager) RecordFaultForPeer(p peer.ID) {
	rm.RecordFaultAt(p.String(), time.Now())
}

// RecordSuccessAt records a successful transfer for a peer at a specific time.
func (rm *ReputationManager) RecordSuccessAt(peerID string, now time.Time) {
	if peerID == "" {
		return
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	s, ok := rm.scores[peerID]
	if !ok {
		s = &PeerScore{LastUpdated: now}
		rm.scores[peerID] = s
	}

	rm.applyDecay(s, now)
	s.ConsecutiveErrors = 0
	s.FaultScore = math.Max(0, s.FaultScore-rm.config.FaultWeight)
	s.TotalSuccesses++
}

// RecordSuccess records a successful transfer for a peer using current time.
func (rm *ReputationManager) RecordSuccess(peerID string) {
	rm.RecordSuccessAt(peerID, time.Now())
}

// RecordSuccessForPeer records a successful transfer for a libp2p peer ID.
func (rm *ReputationManager) RecordSuccessForPeer(p peer.ID) {
	rm.RecordSuccessAt(p.String(), time.Now())
}

// IsBannedAt checks if a peer is banned at a specific time.
func (rm *ReputationManager) IsBannedAt(peerID string, now time.Time) bool {
	if peerID == "" {
		return false
	}
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	s, ok := rm.scores[peerID]
	if !ok {
		return false
	}

	if s.BannedUntil.IsZero() {
		return false
	}

	return now.Before(s.BannedUntil)
}

// IsBanned checks if a peer is currently banned.
func (rm *ReputationManager) IsBanned(peerID string) bool {
	return rm.IsBannedAt(peerID, time.Now())
}

// IsBannedForPeer checks if a libp2p peer ID is currently banned.
func (rm *ReputationManager) IsBannedForPeer(p peer.ID) bool {
	return rm.IsBannedAt(p.String(), time.Now())
}

// GetScoreAt calculates the decayed fault score of a peer at a specific time.
func (rm *ReputationManager) GetScoreAt(peerID string, now time.Time) float64 {
	if peerID == "" {
		return 0
	}
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	s, ok := rm.scores[peerID]
	if !ok {
		return 0
	}

	faultScore := s.FaultScore
	if !s.LastUpdated.IsZero() && now.After(s.LastUpdated) && rm.config.DecayHalfLife > 0 {
		elapsed := now.Sub(s.LastUpdated)
		decayFactor := math.Pow(0.5, float64(elapsed)/float64(rm.config.DecayHalfLife))
		faultScore *= decayFactor
	}
	return faultScore
}

// GetScore returns the decayed fault score of a peer at current time.
func (rm *ReputationManager) GetScore(peerID string) float64 {
	return rm.GetScoreAt(peerID, time.Now())
}

// GetScoreForPeer returns the decayed fault score of a libp2p peer ID at current time.
func (rm *ReputationManager) GetScoreForPeer(p peer.ID) float64 {
	return rm.GetScoreAt(p.String(), time.Now())
}

// GetPeerScoreAt returns a copy of PeerScore for a peer at a specific time.
func (rm *ReputationManager) GetPeerScoreAt(peerID string, now time.Time) PeerScore {
	if peerID == "" {
		return PeerScore{}
	}
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	s, ok := rm.scores[peerID]
	if !ok {
		return PeerScore{}
	}

	scoreCopy := *s
	scoreCopy.FaultScore = rm.GetScoreAt(peerID, now)
	return scoreCopy
}

// GetPeerScore returns a copy of PeerScore for a peer at current time.
func (rm *ReputationManager) GetPeerScore(peerID string) PeerScore {
	return rm.GetPeerScoreAt(peerID, time.Now())
}

// Unban removes the ban for a peer.
func (rm *ReputationManager) Unban(peerID string) {
	if peerID == "" {
		return
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if s, ok := rm.scores[peerID]; ok {
		s.BannedUntil = time.Time{}
		s.ConsecutiveErrors = 0
	}
}

// Reset clears all recorded scores for a peer.
func (rm *ReputationManager) Reset(peerID string) {
	if peerID == "" {
		return
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	delete(rm.scores, peerID)
}
