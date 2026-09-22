package transport

import (
	"context"
	"testing"

	"cipher/internal/ratelimit"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestNewNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer kdht.Close()

	if host == nil {
		t.Fatalf("Expected a host, got nil")
	}

	if len(host.Addrs()) == 0 {
		t.Fatalf("Expected at least one listen address")
	}

	host.Close()
}

func TestHost_NetworkNotificationRateLimiting(t *testing.T) {
	limiter, err := ratelimit.NewPeerRateLimiter(1, 2, 100)
	if err != nil {
		t.Fatalf("failed to create rate limiter: %v", err)
	}

	peerID := peer.ID("reconnecting-peer-id")

	// Verify burst behavior: 2 allowed, 3rd rate-limited
	if !limiter.Allow(peerID) || !limiter.Allow(peerID) {
		t.Fatalf("expected first 2 connection events to be allowed")
	}

	if limiter.Allow(peerID) {
		t.Fatalf("expected 3rd connection event in rapid reconnect to be suppressed")
	}
}
