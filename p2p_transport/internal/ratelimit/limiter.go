package ratelimit

import (
	"sync"

	lru "github.com/hashicorp/golang-lru"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"
)

// Default settings for rate limiting
const (
	DefaultRate     = 5.0  // 5 events per second
	DefaultBurst    = 5    // max burst of 5 events
	DefaultCapacity = 1000 // track up to 1000 peers in LRU
)

// PeerRateLimiter limits events per peer ID using an LRU cache of token bucket limiters.
type PeerRateLimiter struct {
	mu    sync.Mutex
	cache *lru.Cache
	r     rate.Limit
	b     int
}

// NewPeerRateLimiter creates a new rate limiter with specified rate, burst, and LRU cache capacity.
func NewPeerRateLimiter(r rate.Limit, b int, capacity int) (*PeerRateLimiter, error) {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	if r <= 0 {
		r = rate.Limit(DefaultRate)
	}
	if b <= 0 {
		b = DefaultBurst
	}

	c, err := lru.New(capacity)
	if err != nil {
		return nil, err
	}

	return &PeerRateLimiter{
		cache: c,
		r:     r,
		b:     b,
	}, nil
}

// DefaultPeerRateLimiter returns a PeerRateLimiter configured with default parameters.
func DefaultPeerRateLimiter() *PeerRateLimiter {
	limiter, _ := NewPeerRateLimiter(rate.Limit(DefaultRate), DefaultBurst, DefaultCapacity)
	return limiter
}

// Allow reports whether an event for the given peer ID should be allowed.
func (p *PeerRateLimiter) Allow(peerID peer.ID) bool {
	if p == nil || p.cache == nil {
		return true
	}

	key := string(peerID)
	p.mu.Lock()
	val, ok := p.cache.Get(key)
	var lim *rate.Limiter
	if ok {
		lim = val.(*rate.Limiter)
	} else {
		lim = rate.NewLimiter(p.r, p.b)
		p.cache.Add(key, lim)
	}
	p.mu.Unlock()

	return lim.Allow()
}
