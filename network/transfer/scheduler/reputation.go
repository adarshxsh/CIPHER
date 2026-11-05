package scheduler

import (
	"math"
	"sync"
	"time"
)

type EventType int

const (
	EventSuccess EventType = iota
	EventHashMismatch
	EventTimeout
)

type Event struct {
	Type      EventType
	Timestamp time.Time
}

type PeerHistory struct {
	Events     []Event
	LastActive time.Time
}

type ReputationConfig struct {
	Window             time.Duration // Sliding window duration (default 5m)
	MaxPeers           int           // Max active peers (default 10000)
	ExclusionThreshold float64       // Exclusion threshold (default 30.0)
}

type ReputationTracker struct {
	mu     sync.RWMutex
	peers  map[string]*PeerHistory
	config ReputationConfig
}

type ReputationOption func(*ReputationConfig)

func WithWindow(d time.Duration) ReputationOption {
	return func(c *ReputationConfig) {
		c.Window = d
	}
}

func WithMaxPeers(n int) ReputationOption {
	return func(c *ReputationConfig) {
		c.MaxPeers = n
	}
}

func WithExclusionThreshold(t float64) ReputationOption {
	return func(c *ReputationConfig) {
		c.ExclusionThreshold = t
	}
}

func NewReputationTracker(opts ...ReputationOption) *ReputationTracker {
	cfg := ReputationConfig{
		Window:             5 * time.Minute,
		MaxPeers:           10000,
		ExclusionThreshold: 30.0,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &ReputationTracker{
		peers:  make(map[string]*PeerHistory),
		config: cfg,
	}
}

func (rt *ReputationTracker) RecordEvent(peerID string, eventType EventType) {
	rt.RecordEventAt(peerID, eventType, time.Now())
}

func (rt *ReputationTracker) RecordEventAt(peerID string, eventType EventType, timestamp time.Time) {
	if rt == nil {
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()

	hist, exists := rt.peers[peerID]
	if !exists {
		// Check capacity limit
		if len(rt.peers) >= rt.config.MaxPeers {
			rt.pruneLocked(timestamp)
			if len(rt.peers) >= rt.config.MaxPeers {
				rt.evictOldestLocked()
			}
		}
		hist = &PeerHistory{}
		rt.peers[peerID] = hist
	}

	hist.Events = append(hist.Events, Event{
		Type:      eventType,
		Timestamp: timestamp,
	})
	hist.LastActive = timestamp
}

func (rt *ReputationTracker) GetScore(peerID string) float64 {
	return rt.GetScoreAt(peerID, time.Now())
}

func (rt *ReputationTracker) GetScoreAt(peerID string, now time.Time) float64 {
	if rt == nil {
		return 100.0
	}
	rt.mu.RLock()
	hist, exists := rt.peers[peerID]
	if !exists {
		rt.mu.RUnlock()
		return 100.0
	}

	// Filter events within window
	cutoff := now.Add(-rt.config.Window)
	var windowEvents []Event
	for _, evt := range hist.Events {
		if !evt.Timestamp.Before(cutoff) {
			windowEvents = append(windowEvents, evt)
		}
	}
	rt.mu.RUnlock()

	if len(windowEvents) == 0 {
		return 100.0
	}

	var successes, hashMismatches, timeouts int
	consecutiveErrors := 0
	inConsecutiveErrorStreak := true

	// Iterate backwards from most recent event
	for i := len(windowEvents) - 1; i >= 0; i-- {
		evt := windowEvents[i]
		switch evt.Type {
		case EventSuccess:
			successes++
			inConsecutiveErrorStreak = false
		case EventHashMismatch:
			hashMismatches++
			if inConsecutiveErrorStreak {
				consecutiveErrors++
			}
		case EventTimeout:
			timeouts++
			if inConsecutiveErrorStreak {
				consecutiveErrors++
			}
		}
	}

	// Calculate weighted failure score
	const weightHash = 4.0
	const weightTimeout = 4.0

	totalSuccess := float64(successes)
	totalWeightedFailures := float64(hashMismatches)*weightHash + float64(timeouts)*weightTimeout

	if totalSuccess+totalWeightedFailures == 0 {
		return 100.0
	}

	baseRatio := totalSuccess / (totalSuccess + totalWeightedFailures)
	baseScore := baseRatio * 100.0

	// Apply consecutive error penalty modifier
	var modifier float64
	switch {
	case consecutiveErrors == 0:
		modifier = 1.0
	case consecutiveErrors == 1:
		modifier = 0.5
	case consecutiveErrors == 2:
		modifier = 0.25
	default: // 3 or more consecutive errors
		modifier = 0.0
	}

	score := baseScore * modifier
	if consecutiveErrors >= 3 && score >= rt.config.ExclusionThreshold {
		score = rt.config.ExclusionThreshold - 1.0
	}

	return math.Max(0.0, math.Min(100.0, score))
}

func (rt *ReputationTracker) IsExcluded(peerID string) bool {
	return rt.GetScore(peerID) < rt.config.ExclusionThreshold
}

func (rt *ReputationTracker) PruneStalePeers(now time.Time) {
	if rt == nil {
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.pruneLocked(now)
}

func (rt *ReputationTracker) pruneLocked(now time.Time) {
	cutoff := now.Add(-rt.config.Window)
	for peerID, hist := range rt.peers {
		var activeEvents []Event
		for _, evt := range hist.Events {
			if !evt.Timestamp.Before(cutoff) {
				activeEvents = append(activeEvents, evt)
			}
		}
		if len(activeEvents) == 0 {
			delete(rt.peers, peerID)
		} else {
			hist.Events = activeEvents
		}
	}
}

func (rt *ReputationTracker) evictOldestLocked() {
	var oldestPeer string
	var oldestTime time.Time
	first := true

	for peerID, hist := range rt.peers {
		if first || hist.LastActive.Before(oldestTime) {
			oldestTime = hist.LastActive
			oldestPeer = peerID
			first = false
		}
	}

	if oldestPeer != "" {
		delete(rt.peers, oldestPeer)
	}
}

func (rt *ReputationTracker) CountPeers() int {
	if rt == nil {
		return 0
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return len(rt.peers)
}
