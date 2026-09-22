package transport

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
)

func TestNewNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	if host == nil {
		t.Fatalf("Expected a host, got nil")
	}

	if len(host.Addrs()) == 0 {
		t.Fatalf("Expected at least one listen address")
	}

	if host.ConnManager() == nil {
		t.Fatalf("Expected default ConnManager, got nil")
	}

	if host.Network().ResourceManager() == nil {
		t.Fatalf("Expected default ResourceManager, got nil")
	}
}

func TestNewNode_WithConnLimits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false, WithConnLimits(10, 20, 30*time.Second))
	if err != nil {
		t.Fatalf("Expected no error with custom conn limits, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	if host.ConnManager() == nil {
		t.Fatalf("Expected ConnManager to be initialized, got nil")
	}
}

func TestNewNode_WithMemoryLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var limitBytes int64 = 1024
	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false, WithMemoryLimit(limitBytes))
	if err != nil {
		t.Fatalf("Expected no error creating host with memory limit, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	rm := host.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected ResourceManager, got nil")
	}

	scope, err := rm.OpenConnection(network.DirInbound, false, nil)
	if err != nil {
		t.Fatalf("Failed to open connection scope: %v", err)
	}
	defer scope.Done()

	err = scope.ReserveMemory(100, network.ReservationPriorityAlways)
	if err != nil {
		t.Fatalf("Expected small memory reservation to succeed, got %v", err)
	}
	scope.ReleaseMemory(100)

	err = scope.ReserveMemory(int(limitBytes*1000), network.ReservationPriorityAlways)
	if err == nil {
		t.Fatalf("Expected memory reservation exceeding limit (%d bytes) to fail, but it succeeded", limitBytes*1000)
	}
}

func TestNewNode_WithResourceManager(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limiter := rcmgr.NewFixedLimiter(rcmgr.DefaultLimits.Scale(10*1024*1024, 1024))
	customRM, err := rcmgr.NewResourceManager(limiter)
	if err != nil {
		t.Fatalf("Failed to create custom resource manager: %v", err)
	}
	defer customRM.Close()

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false, WithResourceManager(customRM))
	if err != nil {
		t.Fatalf("Expected no error creating host with custom RM, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	if host.Network().ResourceManager() != customRM {
		t.Fatalf("Expected host to use custom ResourceManager")
	}
}

func TestNewNode_PeerConnectionAndResourceEnforcement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1, dht1, err := NewNode(ctx, 0, 0, nil, "", false, WithConnLimits(5, 10, time.Minute))
	if err != nil {
		t.Fatalf("Failed to create host 1: %v", err)
	}
	defer dht1.Close()
	defer h1.Close()

	h2, dht2, err := NewNode(ctx, 0, 0, nil, "", false, WithConnLimits(5, 10, time.Minute))
	if err != nil {
		t.Fatalf("Failed to create host 2: %v", err)
	}
	defer dht2.Close()
	defer h2.Close()

	h1Info := peer.AddrInfo{
		ID:    h1.ID(),
		Addrs: h1.Addrs(),
	}
	if err := h2.Connect(ctx, h1Info); err != nil {
		t.Fatalf("Failed to connect h2 to h1: %v", err)
	}

	if len(h2.Network().ConnsToPeer(h1.ID())) == 0 {
		t.Fatalf("Expected active connection between h2 and h1")
	}
}
