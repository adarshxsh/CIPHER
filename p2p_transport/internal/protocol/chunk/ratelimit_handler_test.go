package chunk_test

import (
	"context"
	"testing"

	"cipher/internal/protocol/chunk"
	"cipher/internal/ratelimit"
	"cipher/internal/transport"
	"golang.org/x/time/rate"
)

func TestStreamHandler_RateLimiting(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Create a rate limiter allowing max burst of 2 logs
	limiter, err := ratelimit.NewPeerRateLimiter(rate.Limit(1), 2, 100)
	if err != nil {
		t.Fatalf("failed to create limiter: %v", err)
	}

	chunk.NewStreamHandler(h1, eng1, chunk.WithRateLimiter(limiter))

	peer1ID := h2.ID()

	// Check rate limiter before flood
	if !limiter.Allow(peer1ID) || !limiter.Allow(peer1ID) {
		t.Fatalf("first 2 events should be allowed")
	}

	// 3rd event should be suppressed by rate limiter
	if limiter.Allow(peer1ID) {
		t.Fatalf("3rd event should be suppressed")
	}

	// Send requests to server to ensure handling works fine under rate limits
	client, err := chunk.NewClient(context.Background(), transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	var dummyID [32]byte
	_, err = client.Resolve(context.Background(), dummyID)
	if err == nil {
		t.Fatalf("expected error for non-existent content")
	}
}
