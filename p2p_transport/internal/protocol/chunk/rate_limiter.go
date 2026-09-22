package chunk

import (
	"log"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"
)

// Default per-peer rate limiting parameters for error logs:
// Max 5 error logs per minute with burst allowance of 5.
const (
	DefaultErrorLogRate  = rate.Limit(5.0 / 60.0)
	DefaultErrorLogBurst = 5
	DefaultMaxPeers      = 1000
	DefaultPeerTTL       = 10 * time.Minute
)

type peerLogState struct {
	limiter      *rate.Limiter
	droppedCount int
	lastActive   time.Time
}

type PeerRateLimiter struct {
	mu       sync.Mutex
	peers    map[peer.ID]*peerLogState
	limit    rate.Limit
	burst    int
	maxPeers int
	ttl      time.Duration
	nowFunc  func() time.Time
}

func NewPeerRateLimiter(limit rate.Limit, burst int, maxPeers int, ttl time.Duration) *PeerRateLimiter {
	return &PeerRateLimiter{
		peers:    make(map[peer.ID]*peerLogState),
		limit:    limit,
		burst:    burst,
		maxPeers: maxPeers,
		ttl:      ttl,
		nowFunc:  time.Now,
	}
}

func DefaultPeerRateLimiter() *PeerRateLimiter {
	return NewPeerRateLimiter(DefaultErrorLogRate, DefaultErrorLogBurst, DefaultMaxPeers, DefaultPeerTTL)
}

// Allow checks whether an error log for peerID is permitted by the rate limiter.
// If allowed and previous error logs were dropped, it logs a rate limit summary line.
func (prl *PeerRateLimiter) Allow(peerID peer.ID) bool {
	if prl == nil {
		return true
	}
	prl.mu.Lock()
	defer prl.mu.Unlock()

	now := prl.nowFunc()
	prl.cleanupLocked(now)

	state, ok := prl.peers[peerID]
	if !ok {
		if prl.maxPeers > 0 && len(prl.peers) >= prl.maxPeers {
			prl.evictOldestLocked()
		}
		state = &peerLogState{
			limiter:    rate.NewLimiter(prl.limit, prl.burst),
			lastActive: now,
		}
		prl.peers[peerID] = state
	}
	state.lastActive = now

	if state.limiter.AllowN(now, 1) {
		if state.droppedCount > 0 {
			log.Printf("[Chunk Protocol] Peer %s error log rate limit exceeded (%d error logs dropped)", peerID, state.droppedCount)
			state.droppedCount = 0
		}
		return true
	}

	state.droppedCount++
	return false
}

func (prl *PeerRateLimiter) cleanupLocked(now time.Time) {
	if prl.ttl <= 0 {
		return
	}
	for id, state := range prl.peers {
		if now.Sub(state.lastActive) > prl.ttl {
			delete(prl.peers, id)
		}
	}
}

func (prl *PeerRateLimiter) evictOldestLocked() {
	var oldestID peer.ID
	var oldestTime time.Time
	first := true

	for id, state := range prl.peers {
		if first || state.lastActive.Before(oldestTime) {
			oldestID = id
			oldestTime = state.lastActive
			first = false
		}
	}

	if !first {
		delete(prl.peers, oldestID)
	}
}
