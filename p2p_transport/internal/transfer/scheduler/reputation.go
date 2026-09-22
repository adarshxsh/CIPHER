package scheduler

import (
	"errors"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/protocol/chunk"
)

type PeerMetrics struct {
	Successes         int
	Failures          int
	IntegrityFailures int
	Penalized         bool
	Backoff           time.Duration
}

type PeerTracker struct {
	mu                   sync.RWMutex
	metrics              map[peer.ID]*PeerMetrics
	maxIntegrityFailures int
	maxPeerFailures      int
	initialBackoff       time.Duration
	maxBackoff           time.Duration
}

func NewPeerTracker(maxIntegrityFailures, maxPeerFailures int, initialBackoff, maxBackoff time.Duration) *PeerTracker {
	if maxIntegrityFailures <= 0 {
		maxIntegrityFailures = 3
	}
	if maxPeerFailures <= 0 {
		maxPeerFailures = 5
	}
	if initialBackoff <= 0 {
		initialBackoff = 50 * time.Millisecond
	}
	if maxBackoff <= 0 {
		maxBackoff = 2 * time.Second
	}
	return &PeerTracker{
		metrics:              make(map[peer.ID]*PeerMetrics),
		maxIntegrityFailures: maxIntegrityFailures,
		maxPeerFailures:      maxPeerFailures,
		initialBackoff:       initialBackoff,
		maxBackoff:           maxBackoff,
	}
}

func (pt *PeerTracker) getOrCreate(p peer.ID) *PeerMetrics {
	m, exists := pt.metrics[p]
	if !exists {
		m = &PeerMetrics{}
		pt.metrics[p] = m
	}
	return m
}

func (pt *PeerTracker) RecordSuccess(p peer.ID) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	m := pt.getOrCreate(p)
	m.Successes++
	m.Backoff = 0
}

func (pt *PeerTracker) RecordFailure(p peer.ID, err error) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	m := pt.getOrCreate(p)
	m.Failures++

	if errors.Is(err, chunk.ErrIntegrityMismatch) {
		m.IntegrityFailures++
	}

	if m.IntegrityFailures >= pt.maxIntegrityFailures || m.Failures >= pt.maxPeerFailures {
		m.Penalized = true
	} else {
		if m.Backoff == 0 {
			m.Backoff = pt.initialBackoff
		} else {
			m.Backoff *= 2
			if m.Backoff > pt.maxBackoff {
				m.Backoff = pt.maxBackoff
			}
		}
	}
}

func (pt *PeerTracker) IsPenalized(p peer.ID) bool {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	m, exists := pt.metrics[p]
	if !exists {
		return false
	}
	return m.Penalized
}

func (pt *PeerTracker) GetBackoff(p peer.ID) time.Duration {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	m, exists := pt.metrics[p]
	if !exists {
		return 0
	}
	return m.Backoff
}

func (pt *PeerTracker) GetMetrics(p peer.ID) PeerMetrics {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	m, exists := pt.metrics[p]
	if !exists {
		return PeerMetrics{}
	}
	return *m
}
