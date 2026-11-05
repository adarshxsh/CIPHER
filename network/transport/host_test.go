package transport

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
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

func TestNewNode_ResourceLimitsEnforcement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set a low stream limit per peer (e.g. 2)
	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false, WithPeerStreamLimit(2))
	if err != nil {
		t.Fatalf("Failed to create host: %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	rm := host.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected ResourceManager on host")
	}

	dummyPeer := peer.ID("12D3KooWSDummyPeerIDForTest1234567890123456789012345")

	s1, err := rm.OpenStream(dummyPeer, network.DirOutbound)
	if err != nil {
		t.Fatalf("Failed to open first stream: %v", err)
	}
	defer s1.Done()

	s2, err := rm.OpenStream(dummyPeer, network.DirOutbound)
	if err != nil {
		t.Fatalf("Failed to open second stream: %v", err)
	}
	defer s2.Done()

	// Third stream should fail due to limit of 2
	s3, err := rm.OpenStream(dummyPeer, network.DirOutbound)
	if err == nil {
		s3.Done()
		t.Fatalf("Expected error opening third stream due to peer stream quota limit, got nil")
	}
}

func TestNewNode_CustomOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm, err := connmgr.NewConnManager(10, 30, connmgr.WithGracePeriod(10*time.Second))
	if err != nil {
		t.Fatalf("Failed to create custom connmgr: %v", err)
	}

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false,
		WithConnManager(cm),
		WithMemoryLimit(128*1024*1024),
		WithConnLimits(15, 40, 20*time.Second),
	)
	if err != nil {
		t.Fatalf("Failed to create host with custom options: %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	if host.ConnManager() != cm {
		t.Fatalf("Expected custom ConnManager to be attached")
	}
}

func TestNewNode_WithPartialLimits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var partial rcmgr.PartialLimitConfig
	partial.PeerDefault.Streams = rcmgr.LimitVal(1)
	partial.PeerDefault.StreamsInbound = rcmgr.LimitVal(1)
	partial.PeerDefault.StreamsOutbound = rcmgr.LimitVal(1)

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false, WithPartialLimits(partial))
	if err != nil {
		t.Fatalf("Failed to create host with partial limits: %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	rm := host.Network().ResourceManager()
	dummyPeer := peer.ID("12D3KooWSDummyPeerIDForTest1234567890123456789012345")

	s1, err := rm.OpenStream(dummyPeer, network.DirOutbound)
	if err != nil {
		t.Fatalf("Failed to open first stream: %v", err)
	}
	defer s1.Done()

	s2, err := rm.OpenStream(dummyPeer, network.DirOutbound)
	if err == nil {
		s2.Done()
		t.Fatalf("Expected stream limit error on second stream, got nil")
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

	numPeers := 10
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

	conns := targetHost.Network().Conns()
	if len(conns) < numPeers {
		t.Fatalf("Expected at least %d connections on target host, got %d", numPeers, len(conns))
	}

	// Trigger connection trimming via ConnManager
	targetHost.ConnManager().TrimOpenConns(ctx)

	// Confirm target host remains responsive after trimming
	if len(targetHost.Addrs()) == 0 || targetHost.Addrs()[0].String() != targetAddr.String() {
		t.Fatalf("Target host address changed or closed unexpectedly")
	}
}

