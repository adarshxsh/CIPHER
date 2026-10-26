package transport

import (
	"cipher/internal/protocol"
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
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

	// Verify Resource Manager is active and attached to the host network
	rm := host.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected ResourceManager to be attached to host network, got nil")
	}

	if _, ok := rm.(*network.NullResourceManager); ok {
		t.Fatalf("Expected active ResourceManager, got NullResourceManager")
	}

	host.Close()
}

func TestNewNode_PerPeerConnectionLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	rm := host.Network().ResourceManager()
	dummyPeer := peer.ID("12D3KooWDdummyPeerIDForTestingPerPeerLimits1")
	maddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")

	var connScopes []network.ConnManagementScope
	defer func() {
		for _, cs := range connScopes {
			cs.Done()
		}
	}()

	// Open 16 connections for dummyPeer (the allowed per-peer limit)
	for i := 0; i < 16; i++ {
		cs, err := rm.OpenConnection(network.DirInbound, false, maddr)
		if err != nil {
			t.Fatalf("Expected connection %d to succeed, got error: %v", i+1, err)
		}
		if err := cs.SetPeer(dummyPeer); err != nil {
			cs.Done()
			t.Fatalf("Expected SetPeer for connection %d to succeed, got error: %v", i+1, err)
		}
		connScopes = append(connScopes, cs)
	}

	// Attempting to open the 17th connection for dummyPeer must be rejected
	cs17, err := rm.OpenConnection(network.DirInbound, false, maddr)
	if err == nil {
		errSet := cs17.SetPeer(dummyPeer)
		if errSet == nil {
			cs17.Done()
			t.Fatalf("Expected 17th connection from peer to be rejected, but it succeeded")
		} else {
			cs17.Done()
			t.Logf("17th connection rejected at SetPeer with error: %v", errSet)
		}
	} else {
		t.Logf("17th connection rejected at OpenConnection with error: %v", err)
	}
}

func TestNewNode_ProtocolLimits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	rm := host.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected active ResourceManager")
	}

	dummyPeer := peer.ID("12D3KooWDdummyPeerIDForTestingProtocolLimits1")

	var streamScopes []network.StreamManagementScope
	defer func() {
		for _, ss := range streamScopes {
			ss.Done()
		}
	}()

	// Open 64 stream scopes for dummyPeer on ChunkTransportProtocolID (allowed limit)
	for i := 0; i < 64; i++ {
		ss, err := rm.OpenStream(dummyPeer, network.DirInbound)
		if err != nil {
			t.Fatalf("Expected stream %d to succeed, got error: %v", i+1, err)
		}
		if err := ss.SetProtocol(protocol.ChunkTransportProtocolID); err != nil {
			ss.Done()
			t.Fatalf("Expected SetProtocol for stream %d to succeed, got error: %v", i+1, err)
		}
		streamScopes = append(streamScopes, ss)
	}

	// Attempting to attach 65th stream to ChunkTransportProtocolID for dummyPeer must be rejected
	ss65, err := rm.OpenStream(dummyPeer, network.DirInbound)
	if err == nil {
		errProto := ss65.SetProtocol(protocol.ChunkTransportProtocolID)
		if errProto == nil {
			ss65.Done()
			t.Fatalf("Expected 65th stream for peer on chunk protocol to be rejected, but it succeeded")
		} else {
			ss65.Done()
			t.Logf("65th stream on chunk protocol rejected with error: %v", errProto)
		}
	} else {
		t.Logf("65th stream rejected at OpenStream with error: %v", err)
	}
}

func TestNewNode_SystemMemoryAndStreamLimits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	rm := host.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected active ResourceManager")
	}

	// Verify system memory reservation exceeding 256MB is rejected
	streamScope, err := rm.OpenStream(peer.ID("12D3KooWDdummyPeerIDForTestingMemory1"), network.DirInbound)
	if err != nil {
		t.Fatalf("Failed to open stream scope: %v", err)
	}
	defer streamScope.Done()

	// Attempt to reserve 300MB (exceeds 256MB limit)
	err = streamScope.ReserveMemory(300*1024*1024, network.ReservationPriorityAlways)
	if err == nil {
		t.Fatalf("Expected memory reservation of 300MB to fail exceeding 256MB global limit, but succeeded")
	}
}
