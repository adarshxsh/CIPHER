package transport

import (
	"context"
	"strings"
	"testing"
	"time"

	"cipher/internal/protocol"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestNewNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer kdht.Close()
	defer h.Close()

	if h == nil {
		t.Fatalf("Expected a host, got nil")
	}

	if len(h.Addrs()) == 0 {
		t.Fatalf("Expected at least one listen address")
	}

	// Verify ResourceManager and ConnManager are attached
	if h.Network().ResourceManager() == nil {
		t.Fatalf("Expected Network().ResourceManager() to be configured")
	}

	if h.ConnManager() == nil {
		t.Fatalf("Expected ConnManager() to be configured")
	}
}

func TestProtocolStreamScopeQuota(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create node A and node B
	hostA, dhtA, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create hostA: %v", err)
	}
	defer dhtA.Close()
	defer hostA.Close()

	hostB, dhtB, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create hostB: %v", err)
	}
	defer dhtB.Close()
	defer hostB.Close()

	// Set a stream handler on hostB for ChunkTransportProtocolID
	hostB.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		buf := make([]byte, 1)
		s.Read(buf)
		s.Close()
	})

	// Connect hostA to hostB
	infoB := peer.AddrInfo{
		ID:    hostB.ID(),
		Addrs: hostB.Addrs(),
	}
	if err := hostA.Connect(ctx, infoB); err != nil {
		t.Fatalf("Failed to connect hostA to hostB: %v", err)
	}

	// Attempt to open streams up to the quota (8 streams)
	streams := make([]network.Stream, 0, 10)
	for i := 0; i < 8; i++ {
		s, err := hostA.NewStream(ctx, hostB.ID(), protocol.ChunkTransportProtocolID)
		if err != nil {
			t.Fatalf("Stream %d should have succeeded, got: %v", i+1, err)
		}
		streams = append(streams, s)
	}

	// The 9th stream must be rejected due to protocol stream scope limit (max 8 streams per peer)
	s9, err := hostA.NewStream(ctx, hostB.ID(), protocol.ChunkTransportProtocolID)
	if err == nil {
		s9.Reset()
		t.Fatalf("Expected 9th stream to be rejected by ResourceManager, but it succeeded")
	}

	errStr := strings.ToLower(err.Error())
	if !strings.Contains(errStr, "resource limit") && !strings.Contains(errStr, "limit exceeded") && !strings.Contains(errStr, "stream") {
		t.Logf("Got expected error on 9th stream: %v", err)
	}

	// Reset one stream and verify a new stream can be opened
	streams[0].Reset()
	time.Sleep(100 * time.Millisecond)

	sNew, err := hostA.NewStream(ctx, hostB.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Expected new stream to succeed after resetting stream 0, got: %v", err)
	}
	sNew.Reset()

	for _, s := range streams[1:] {
		s.Reset()
	}
}

func TestResourceScopeLimits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer kdht.Close()
	defer h.Close()

	rm := h.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected non-nil ResourceManager")
	}

	// 1. Test System Scope Memory Limit (512 MB)
	err = rm.ViewSystem(func(s network.ResourceScope) error {
		err := s.ReserveMemory(512<<20+1, network.ReservationPriorityAlways)
		if err == nil {
			s.ReleaseMemory(512<<20 + 1)
			t.Fatalf("Expected system memory reservation > 512MB to fail")
		}
		err = s.ReserveMemory(512<<20, network.ReservationPriorityAlways)
		if err != nil {
			t.Fatalf("Expected 512MB reservation in system scope to succeed, got: %v", err)
		}
		s.ReleaseMemory(512 << 20)
		return nil
	})
	if err != nil {
		t.Fatalf("ViewSystem returned error: %v", err)
	}

	// 2. Test Protocol Scope Memory Limit (64 MB)
	err = rm.ViewProtocol(protocol.ChunkTransportProtocolID, func(s network.ProtocolScope) error {
		err := s.ReserveMemory(64<<20+1, network.ReservationPriorityAlways)
		if err == nil {
			s.ReleaseMemory(64<<20 + 1)
			t.Fatalf("Expected protocol memory reservation > 64MB to fail")
		}
		err = s.ReserveMemory(64<<20, network.ReservationPriorityAlways)
		if err != nil {
			t.Fatalf("Expected 64MB reservation in protocol scope to succeed, got: %v", err)
		}
		s.ReleaseMemory(64 << 20)
		return nil
	})
	if err != nil {
		t.Fatalf("ViewProtocol returned error: %v", err)
	}

	// 3. Test Peer Scope Memory Limit (16 MB)
	dummyPeer := peer.ID("12D3KooWDpj9mMNmJJBso4vxrmGLy15Sh4gP8fcM4r23VyN8E32m")
	err = rm.ViewPeer(dummyPeer, func(s network.PeerScope) error {
		err := s.ReserveMemory(16<<20+1, network.ReservationPriorityAlways)
		if err == nil {
			s.ReleaseMemory(16<<20 + 1)
			t.Fatalf("Expected peer memory reservation > 16MB to fail")
		}
		err = s.ReserveMemory(16<<20, network.ReservationPriorityAlways)
		if err != nil {
			t.Fatalf("Expected 16MB reservation in peer scope to succeed, got: %v", err)
		}
		s.ReleaseMemory(16 << 20)
		return nil
	})
	if err != nil {
		t.Fatalf("ViewPeer returned error: %v", err)
	}
}
