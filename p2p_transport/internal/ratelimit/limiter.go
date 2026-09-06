package ratelimit

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"
)

// DefaultRate is the default sustained error log rate limit per peer (messages per second).
const DefaultRate rate.Limit = 5.0

// DefaultBurst is the default maximum error log burst count per peer.
const DefaultBurst int = 10

// PeerRateLimiter enforces token-bucket rate limits independently for each remote peer.
type PeerRateLimiter struct {
	mu       sync.RWMutex
	limiters map[peer.ID]*rate.Limiter
	r        rate.Limit
	b        int
}

// NewPeerRateLimiter constructs a new per-peer token-bucket rate limiter.
func NewPeerRateLimiter(r rate.Limit, b int) *PeerRateLimiter {
	return &PeerRateLimiter{
		limiters: make(map[peer.ID]*rate.Limiter),
		r:        r,
		b:        b,
	}
}

// DefaultPeerRateLimiter constructs a rate limiter using default rate (5 msgs/sec) and burst (10 msgs).
func DefaultPeerRateLimiter() *PeerRateLimiter {
	return NewPeerRateLimiter(DefaultRate, DefaultBurst)
}

// Allow reports whether an error log event for peerID is permitted under its rate limit.
func (p *PeerRateLimiter) Allow(peerID peer.ID) bool {
	p.mu.RLock()
	lim, exists := p.limiters[peerID]
	p.mu.RUnlock()

	if !exists {
		p.mu.Lock()
		lim, exists = p.limiters[peerID]
		if !exists {
			lim = rate.NewLimiter(p.r, p.b)
			p.limiters[peerID] = lim
		}
		p.mu.Unlock()
	}

	return lim.Allow()
}

// SetLimit updates the rate limit parameters for all existing and future peer limiters.
func (p *PeerRateLimiter) SetLimit(r rate.Limit, b int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.r = r
	p.b = b
	for _, lim := range p.limiters {
		lim.SetLimit(r)
		lim.SetBurst(b)
	}
}

// RemovePeer removes tracking for a peer.
func (p *PeerRateLimiter) RemovePeer(peerID peer.ID) {
	p.mu.Lock()
	delete(p.limiters, peerID)
	p.mu.Unlock()
}
