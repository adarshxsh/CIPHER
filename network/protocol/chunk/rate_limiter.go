package chunk

import (
	"math"
	"sync"
	"time"
)

// RateLimiter implements a per-stream token-bucket rate limiter for log emissions.
type RateLimiter struct {
	mu         sync.Mutex
	capacity   float64
	tokens     float64
	refillRate float64 // tokens per second
	lastRefill time.Time
	suppressed int64
	nowFunc    func() time.Time
}

// NewRateLimiter creates a new Token Bucket RateLimiter with the specified capacity and refill rate (tokens/sec).
func NewRateLimiter(capacity float64, refillRate float64) *RateLimiter {
	return &RateLimiter{
		capacity:   capacity,
		tokens:     capacity,
		refillRate: refillRate,
		lastRefill: time.Now(),
		nowFunc:    time.Now,
	}
}

// DefaultStreamRateLimiter returns a rate limiter with standard defaults for stream error log throttling.
// Default capacity = 5 tokens, refill = 1 token/sec.
func DefaultStreamRateLimiter() *RateLimiter {
	return NewRateLimiter(5.0, 1.0)
}

// SetNowFunc overrides time provider (primarily for deterministic unit testing).
func (rl *RateLimiter) SetNowFunc(nowFunc func() time.Time) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.nowFunc = nowFunc
	rl.lastRefill = nowFunc()
}

// Allow checks if a log emission is allowed under the token bucket policy.
// Returns (allowed, suppressedCount). If allowed is true and suppressedCount > 0,
// suppressedCount indicates how many prior events were suppressed since the last allowed event.
func (rl *RateLimiter) Allow() (bool, int64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := rl.nowFunc()
	elapsed := now.Sub(rl.lastRefill).Seconds()
	if elapsed > 0 {
		rl.tokens = math.Min(rl.capacity, rl.tokens+elapsed*rl.refillRate)
		rl.lastRefill = now
	}

	if rl.tokens >= 1.0 {
		rl.tokens -= 1.0
		prevSuppressed := rl.suppressed
		rl.suppressed = 0
		return true, prevSuppressed
	}

	rl.suppressed++
	return false, 0
}

// FlushSuppressed returns the current number of suppressed events and resets the counter.
func (rl *RateLimiter) FlushSuppressed() int64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	prev := rl.suppressed
	rl.suppressed = 0
	return prev
}
