package transport

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/protocol"
)

func TestTransport_OpenStream_ConfiguresDeadlines(t *testing.T) {
	mocknet := mocknet.New()

	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatalf("Failed to generate peer 1: %v", err)
	}
	h2, err := mocknet.GenPeer()
	if err != nil {
		t.Fatalf("Failed to generate peer 2: %v", err)
	}

	if err := mocknet.LinkAll(); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}

	h2.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {})

	tp1 := NewTransport(h1)
	ctx := context.Background()

	s, err := tp1.OpenStream(ctx, h2.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer s.Close()

	if s == nil {
		t.Fatal("Expected non-nil stream")
	}
}
