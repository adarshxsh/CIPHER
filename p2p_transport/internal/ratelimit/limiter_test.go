package ratelimit

import (
	"strconv"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"
)

func TestPeerRateLimiter_BurstAndLimit(t *testing.T) {
	limiter, err := NewPeerRateLimiter(rate.Limit(5), 5, 100)
	if err != nil {
		t.Fatalf("failed to create limiter: %v", err)
	}

	peerID := peer.ID("test-peer-1")

	// First 5 events should be allowed (burst = 5)
	for i := 0; i < 5; i++ {
		if !limiter.Allow(peerID) {
			t.Errorf("event %d should be allowed", i+1)
		}
	}

	// 6th event immediately after should be suppressed
	if limiter.Allow(peerID) {
		t.Errorf("6th event should be suppressed under rate limit")
	}

	// Wait 250ms (allows 1.25 events at rate 5/sec)
	time.Sleep(250 * time.Millisecond)

	// 7th event should be allowed now
	if !limiter.Allow(peerID) {
		t.Errorf("event after wait should be allowed")
	}
}

func TestPeerRateLimiter_MultiplePeersIndependent(t *testing.T) {
	limiter, err := NewPeerRateLimiter(rate.Limit(2), 2, 100)
	if err != nil {
		t.Fatalf("failed to create limiter: %v", err)
	}

	peer1 := peer.ID("peer-1")
	peer2 := peer.ID("peer-2")

	// Exhaust peer1 limit
	limiter.Allow(peer1)
	limiter.Allow(peer1)
	if limiter.Allow(peer1) {
		t.Errorf("peer1 should be rate limited")
	}

	// peer2 should still have full burst available
	if !limiter.Allow(peer2) || !limiter.Allow(peer2) {
		t.Errorf("peer2 should be allowed")
	}
}

func TestPeerRateLimiter_LRUCapacity(t *testing.T) {
	capacity := 5
	limiter, err := NewPeerRateLimiter(rate.Limit(1), 1, capacity)
	if err != nil {
		t.Fatalf("failed to create limiter: %v", err)
	}

	// Add capacity + 5 peers
	for i := 0; i < capacity+5; i++ {
		pID := peer.ID("ephemeral-peer-" + strconv.Itoa(i))
		limiter.Allow(pID)
	}

	if limiter.cache.Len() > capacity {
		t.Errorf("expected LRU cache length <= %d, got %d", capacity, limiter.cache.Len())
	}
}
