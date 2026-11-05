package transport

import (
	"context"
	"testing"
	"time"

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
		t.Fatalf("Expected ConnManager to be initialized")
	}

	if host.Network().ResourceManager() == nil {
		t.Fatalf("Expected ResourceManager to be initialized")
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

