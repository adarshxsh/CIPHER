package reputation

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type FaultType int

const (
	FaultHashMismatch FaultType = iota
	FaultProtocol
	FaultTransport
)

type PeerMetrics struct {
	PeerID          peer.ID
	HashFailures    int
	ProtocolErrors  int
	TransportErrors int
	Successes       int
	FaultScore      float64
	IsQuarantined   bool
	QuarantinedAt   time.Time
	LastUpdated     time.Time
}

type Config struct {
	QuarantineThreshold float64
	HashMismatchPenalty float64
	ProtocolPenalty     float64
	TransportPenalty    float64
	DecayInterval       time.Duration
	DecayFactor         float64
}

func DefaultConfig() Config {
	return Config{
		QuarantineThreshold: 10.0,
		HashMismatchPenalty: 10.0,
		ProtocolPenalty:     5.0,
		TransportPenalty:    2.5,
		DecayInterval:       1 * time.Minute,
		DecayFactor:         0.9,
	}
}

type Tracker struct {
	mu    sync.RWMutex
	cfg   Config
	peers map[peer.ID]*PeerMetrics
}

func NewTracker(cfgs ...Config) *Tracker {
	cfg := DefaultConfig()
	if len(cfgs) > 0 {
		cfg = cfgs[0]
	}
	return &Tracker{
		cfg:   cfg,
		peers: make(map[peer.ID]*PeerMetrics),
	}
}

func (t *Tracker) getOrCreatePeerLocked(peerID peer.ID) *PeerMetrics {
	m, exists := t.peers[peerID]
	if !exists {
		m = &PeerMetrics{
			PeerID:      peerID,
			LastUpdated: time.Now(),
		}
		t.peers[peerID] = m
	}
	return m
}

func (t *Tracker) RecordFault(peerID peer.ID, fault FaultType) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	m := t.getOrCreatePeerLocked(peerID)
	m.LastUpdated = time.Now()

	var penalty float64
	switch fault {
	case FaultHashMismatch:
		m.HashFailures++
		penalty = t.cfg.HashMismatchPenalty
	case FaultProtocol:
		m.ProtocolErrors++
		penalty = t.cfg.ProtocolPenalty
	case FaultTransport:
		m.TransportErrors++
		penalty = t.cfg.TransportPenalty
	default:
		penalty = 1.0
	}

	m.FaultScore += penalty

	wasQuarantined := m.IsQuarantined
	if !m.IsQuarantined && m.FaultScore >= t.cfg.QuarantineThreshold {
		m.IsQuarantined = true
		m.QuarantinedAt = time.Now()
		return true
	}
	return !wasQuarantined && m.IsQuarantined
}

func (t *Tracker) RecordSuccess(peerID peer.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	m := t.getOrCreatePeerLocked(peerID)
	m.Successes++
	m.LastUpdated = time.Now()

	if m.FaultScore > 0 && !m.IsQuarantined {
		m.FaultScore -= 0.5
		if m.FaultScore < 0 {
			m.FaultScore = 0
		}
	}
}

func (t *Tracker) IsQuarantined(peerID peer.ID) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	m, exists := t.peers[peerID]
	if !exists {
		return false
	}
	return m.IsQuarantined
}

func (t *Tracker) GetScore(peerID peer.ID) float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	m, exists := t.peers[peerID]
	if !exists {
		return 0
	}
	return m.FaultScore
}

func (t *Tracker) GetMetrics(peerID peer.ID) PeerMetrics {
	t.mu.RLock()
	defer t.mu.RUnlock()

	m, exists := t.peers[peerID]
	if !exists {
		return PeerMetrics{PeerID: peerID}
	}
	return *m
}

func (t *Tracker) Quarantine(peerID peer.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	m := t.getOrCreatePeerLocked(peerID)
	m.IsQuarantined = true
	m.QuarantinedAt = time.Now()
	m.FaultScore = t.cfg.QuarantineThreshold
}

func (t *Tracker) Unquarantine(peerID peer.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	m, exists := t.peers[peerID]
	if exists {
		m.IsQuarantined = false
		m.FaultScore = 0
	}
}

func (t *Tracker) ApplyDecay() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, m := range t.peers {
		if !m.IsQuarantined && m.FaultScore > 0 {
			m.FaultScore *= t.cfg.DecayFactor
			if m.FaultScore < 0.01 {
				m.FaultScore = 0
			}
		}
	}
}
