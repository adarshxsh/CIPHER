package ratelimit_test

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"

	"cipher/internal/ratelimit"
)

func TestPeerRateLimiter_ThrottlesBursts(t *testing.T) {
	peerA := peer.ID("peer-A")
	peerB := peer.ID("peer-B")

	// Create rate limiter with 1 event/sec and burst of 3
	limiter := ratelimit.NewPeerRateLimiter(rate.Limit(1.0), 3)

	// Peer A sends 5 rapid events
	allowedA := 0
	for i := 0; i < 5; i++ {
		if limiter.Allow(peerA) {
			allowedA++
		}
	}

	if allowedA != 3 {
		t.Errorf("Expected 3 allowed events for Peer A, got %d", allowedA)
	}

	// Peer B sends 2 rapid events - should NOT be throttled by Peer A's burst
	allowedB := 0
	for i := 0; i < 2; i++ {
		if limiter.Allow(peerB) {
			allowedB++
		}
	}

	if allowedB != 2 {
		t.Errorf("Expected 2 allowed events for Peer B, got %d", allowedB)
	}
}

func TestPeerRateLimiter_SetLimit(t *testing.T) {
	peer1 := peer.ID("peer-1")
	peer2 := peer.ID("peer-2")
	limiter := ratelimit.NewPeerRateLimiter(rate.Limit(1.0), 1)

	if !limiter.Allow(peer1) {
		t.Fatal("First event should be allowed")
	}
	if limiter.Allow(peer1) {
		t.Fatal("Second event should be throttled")
	}

	// Update limit to burst 5 for future tokens/peers
	limiter.SetLimit(rate.Limit(100.0), 5)
	time.Sleep(10 * time.Millisecond) // Allow tokens to accumulate at 100/sec

	if !limiter.Allow(peer1) {
		t.Errorf("Expected event to be allowed after token refill at new rate")
	}

	// New peer gets burst 5
	allowed2 := 0
	for i := 0; i < 5; i++ {
		if limiter.Allow(peer2) {
			allowed2++
		}
	}
	if allowed2 != 5 {
		t.Errorf("Expected 5 allowed events for new peer, got %d", allowed2)
	}
}
