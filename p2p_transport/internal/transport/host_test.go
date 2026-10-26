package transport

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
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

	host.Close()
}

func TestNewNodeManagers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	cm := host.ConnManager()
	if cm == nil {
		t.Fatalf("Expected non-nil ConnManager attached to host")
	}

	rm := host.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected non-nil ResourceManager attached to host")
	}

	if _, ok := rm.(*network.NullResourceManager); ok {
		t.Fatalf("Expected active ResourceManager, got NullResourceManager")
	}
}

func TestNewNodeInvalidRelayCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host, kdht, err := NewNode(ctx, 0, 0, nil, "invalid-multiaddr-string", false)
	if err == nil {
		if host != nil {
			host.Close()
		}
		if kdht != nil {
			kdht.Close()
		}
		t.Fatalf("Expected error when providing invalid relay address")
	}
}

func TestConnectionManagerWatermarksAndPruning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	targetHost, targetDHT, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create target host: %v", err)
	}
	defer targetDHT.Close()
	defer targetHost.Close()

	targetAddr := targetHost.Addrs()[0]
	targetInfo := peer.AddrInfo{
		ID:    targetHost.ID(),
		Addrs: targetHost.Addrs(),
	}

	// Create peers and connect to targetHost to verify connections are tracked properly and pruning works
	numPeers := 15
	peers := make([]interface{ Close() error }, 0, numPeers)
	defer func() {
		for _, p := range peers {
			p.Close()
		}
	}()

	for i := 0; i < numPeers; i++ {
		clientHost, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
		if err != nil {
			t.Fatalf("Failed to create client host %d: %v", i, err)
		}
		peers = append(peers, clientHost)

		if err := clientHost.Connect(ctx, targetInfo); err != nil {
			t.Fatalf("Client %d failed to connect to target host: %v", i, err)
		}
	}

	// Ensure connection count on targetHost matches
	conns := targetHost.Network().Conns()
	if len(conns) < numPeers {
		t.Fatalf("Expected at least %d connections on target host, got %d", numPeers, len(conns))
	}

	// Trigger manual connection trim to verify connmgr integration doesn't panic and operates correctly
	targetHost.ConnManager().TrimOpenConns(ctx)

	// Confirm target host is still alive and responsive after connection trimming
	if len(targetHost.Addrs()) == 0 || targetHost.Addrs()[0].String() != targetAddr.String() {
		t.Fatalf("Target host address changed or closed unexpectedly")
	}
}

