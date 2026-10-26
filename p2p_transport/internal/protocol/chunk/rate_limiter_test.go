package chunk_test

import (
	"testing"
	"time"

	"cipher/internal/protocol/chunk"
)

func TestRateLimiter_BurstAndRefill(t *testing.T) {
	limiter := chunk.NewRateLimiter(3.0, 1.0)

	now := time.Now()
	limiter.SetNowFunc(func() time.Time {
		return now
	})

	// Burst up to capacity (3 calls allowed)
	for i := 1; i <= 3; i++ {
		allowed, suppressed := limiter.Allow()
		if !allowed {
			t.Fatalf("Call %d should be allowed under burst capacity", i)
		}
		if suppressed != 0 {
			t.Fatalf("Call %d expected 0 suppressed, got %d", i, suppressed)
		}
	}

	// 4th call should be throttled
	allowed, _ := limiter.Allow()
	if allowed {
		t.Fatal("4th call should be throttled")
	}

	// 5th call should be throttled
	allowed, _ = limiter.Allow()
	if allowed {
		t.Fatal("5th call should be throttled")
	}

	// Advance mock time by 1 second -> 1 token refilled
	now = now.Add(1 * time.Second)

	allowed, suppressed := limiter.Allow()
	if !allowed {
		t.Fatal("Call after 1 second refill should be allowed")
	}
	if suppressed != 2 {
		t.Errorf("Expected 2 suppressed messages reported on refill allow, got %d", suppressed)
	}

	// Immediate follow-up call should be throttled again
	allowed, _ = limiter.Allow()
	if allowed {
		t.Fatal("Call immediately after consuming refilled token should be throttled")
	}
}

func TestRateLimiter_FlushSuppressed(t *testing.T) {
	limiter := chunk.NewRateLimiter(1.0, 1.0)

	now := time.Now()
	limiter.SetNowFunc(func() time.Time { return now })

	// 1st call consumes 1 token
	limiter.Allow()

	// Next 5 calls are suppressed
	for i := 0; i < 5; i++ {
		limiter.Allow()
	}

	flushed := limiter.FlushSuppressed()
	if flushed != 5 {
		t.Errorf("Expected 5 flushed suppressed messages, got %d", flushed)
	}

	// Second flush should return 0
	if second := limiter.FlushSuppressed(); second != 0 {
		t.Errorf("Expected 0 on second flush, got %d", second)
	}
}
