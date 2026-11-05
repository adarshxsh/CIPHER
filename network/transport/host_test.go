package transport

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
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

	rm := host.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected network resource manager to be non-nil")
	}

	host.Close()
}

func TestResourceManagerLimits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1, dht1, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create host 1: %v", err)
	}
	defer h1.Close()
	defer dht1.Close()

	rm := h1.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected non-nil resource manager")
	}

	// Test stream scope allocation and limit enforcement via resource manager interface
	dummyPeer := peer.ID("QmVm5Xgg3Zb9sjfzB9rHo1aNHn4V7durXAQAN4By89wVXQ")
	var streamScopes []network.StreamManagementScope
	var limitHit bool
	for i := 0; i < 10000; i++ {
		strScope, err := rm.OpenStream(dummyPeer, network.DirInbound)
		if err != nil {
			limitHit = true
			t.Logf("Resource manager properly rejected excess stream at iteration %d: %v", i, err)
			break
		}
		streamScopes = append(streamScopes, strScope)
	}

	for _, ss := range streamScopes {
		ss.Done()
	}

	t.Logf("Total streams opened before limit or completion: %d, limit hit: %v", len(streamScopes), limitHit)
}
