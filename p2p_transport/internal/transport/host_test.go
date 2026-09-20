package transport

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	p2pconnmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
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
		t.Fatalf("Expected ConnManager to be initialized on host")
	}

	if host.Network().ResourceManager() == nil {
		t.Fatalf("Expected ResourceManager to be initialized on host network")
	}
}

func TestNewNodeWithOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	customCM, err := p2pconnmgr.NewConnManager(10, 30, p2pconnmgr.WithGracePeriod(5*time.Second))
	if err != nil {
		t.Fatalf("failed to create custom conn manager: %v", err)
	}
	defer customCM.Close()

	// 1. Test custom conn limits, memory limits, peer stream limits
	h1, kdht1, err := NewNode(ctx, 0, 0, nil, "", false,
		WithConnLimits(15, 40, 10*time.Second),
		WithMemoryLimit(512*1024*1024),
		WithPeerStreamLimit(20),
	)
	if err != nil {
		t.Fatalf("NewNode with limits failed: %v", err)
	}
	defer kdht1.Close()
	defer h1.Close()

	if h1.ConnManager() == nil {
		t.Fatalf("Expected ConnManager")
	}

	// 2. Test passing explicit ConnManager & PartialLimits
	partial := rcmgr.PartialLimitConfig{
		System: rcmgr.ResourceLimits{
			Streams: 50,
		},
	}
	h2, kdht2, err := NewNode(ctx, 0, 0, nil, "", false,
		WithConnManager(customCM),
		WithPartialLimits(partial),
		WithLibp2pOptions(libp2p.DisableRelay()),
	)
	if err != nil {
		t.Fatalf("NewNode with custom CM failed: %v", err)
	}
	defer kdht2.Close()
	defer h2.Close()

	if h2.ConnManager() != customCM {
		t.Fatalf("Expected host to use custom ConnManager instance")
	}

	// 3. Test passing custom ResourceManager
	limiter := rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits)
	customRM, err := rcmgr.NewResourceManager(limiter)
	if err != nil {
		t.Fatalf("failed to create custom resource manager: %v", err)
	}
	defer customRM.Close()

	h3, kdht3, err := NewNode(ctx, 0, 0, nil, "", false,
		WithResourceManager(customRM),
	)
	if err != nil {
		t.Fatalf("NewNode with custom RM failed: %v", err)
	}
	defer kdht3.Close()
	defer h3.Close()

	if h3.Network().ResourceManager() != customRM {
		t.Fatalf("Expected host to use custom ResourceManager instance")
	}
}

func TestConnManagerPruning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Host A with low watermark = 1, high watermark = 2, grace period = 0
	hA, kdhtA, err := NewNode(ctx, 0, 0, nil, "", false, WithConnLimits(1, 2, 0))
	if err != nil {
		t.Fatalf("Failed to create hA: %v", err)
	}
	defer kdhtA.Close()
	defer hA.Close()

	// Create 3 remote hosts
	hB, kdhtB, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create hB: %v", err)
	}
	defer kdhtB.Close()
	defer hB.Close()

	hC, kdhtC, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create hC: %v", err)
	}
	defer kdhtC.Close()
	defer hC.Close()

	hD, kdhtD, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create hD: %v", err)
	}
	defer kdhtD.Close()
	defer hD.Close()

	// Connect hA to hB, hC, hD
	for _, target := range []peer.AddrInfo{
		{ID: hB.ID(), Addrs: hB.Addrs()},
		{ID: hC.ID(), Addrs: hC.Addrs()},
		{ID: hD.ID(), Addrs: hD.Addrs()},
	} {
		if err := hA.Connect(ctx, target); err != nil {
			t.Fatalf("Failed to connect hA to %s: %v", target.ID, err)
		}
	}

	if len(hA.Network().Conns()) < 3 {
		t.Fatalf("Expected at least 3 connections before trimming, got %d", len(hA.Network().Conns()))
	}

	// Trigger connection trimming on hA
	hA.ConnManager().TrimOpenConns(ctx)

	// Allow goroutines to process disconnection
	time.Sleep(100 * time.Millisecond)

	// Verify connection count was pruned down towards low watermark (1)
	if len(hA.Network().Conns()) > 2 {
		t.Fatalf("Expected connection count to be pruned to <= 2 (high watermark), got %d", len(hA.Network().Conns()))
	}
}
