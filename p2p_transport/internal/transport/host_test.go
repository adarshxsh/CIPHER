package transport

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p"
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

	if host == nil {
		t.Fatalf("Expected a host, got nil")
	}

	if len(host.Addrs()) == 0 {
		t.Fatalf("Expected at least one listen address")
	}

	rm := host.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected resource manager to be attached to host network")
	}
	if _, isNull := rm.(*network.NullResourceManager); isNull {
		t.Fatalf("Expected real resource manager, got NullResourceManager")
	}

	host.Close()
}

func TestResourceManager_PeerSwarmRejection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configure host with low inbound connection limit (max 2 inbound connections)
	limits := rcmgr.DefaultLimits
	limits.SystemBaseLimit.ConnsInbound = 2
	limits.SystemBaseLimit.Conns = 2

	limiter := rcmgr.NewFixedLimiter(limits.Scale(128<<20, 256))
	rm, err := rcmgr.NewResourceManager(limiter)
	if err != nil {
		t.Fatalf("Failed to create test resource manager: %v", err)
	}
	defer rm.Close()

	targetHost, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.ResourceManager(rm),
	)
	if err != nil {
		t.Fatalf("Failed to create target host: %v", err)
	}
	defer targetHost.Close()

	targetInfo := peer.AddrInfo{
		ID:    targetHost.ID(),
		Addrs: targetHost.Addrs(),
	}

	// Create 5 separate dialer nodes to swarm the target
	dialers := make([]interface{ Close() error }, 5)
	successCount := 0
	rejectedCount := 0

	for i := 0; i < 5; i++ {
		dialer, err := libp2p.New(
			libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		)
		if err != nil {
			t.Fatalf("Failed to create dialer host %d: %v", i, err)
		}
		dialers[i] = dialer
		defer dialer.Close()

		err = dialer.Connect(ctx, targetInfo)
		if err == nil {
			successCount++
		} else {
			rejectedCount++
		}
	}

	t.Logf("Swarm connection test: %d succeeded, %d rejected", successCount, rejectedCount)

	// Verify that at least 1 connection attempt was rejected due to resource manager limit
	if rejectedCount == 0 {
		t.Fatalf("Expected at least 1 connection rejection when exceeding connection limit, but all %d succeeded", successCount)
	}

	if successCount == 0 {
		t.Fatalf("Expected at least 1 initial connection to succeed within limit")
	}
}
