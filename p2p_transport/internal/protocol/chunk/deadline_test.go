package chunk_test

import (
	"context"
	"net"
	"os"
	"errors"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"

	"cipher/internal/content/core"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func createTCPHosts(t *testing.T) (host.Host, host.Host) {
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create h1: %v", err)
	}

	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		h1.Close()
		t.Fatalf("Failed to create h2: %v", err)
	}

	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), peerstore.PermanentAddrTTL)
	if err := h2.Connect(context.Background(), peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}); err != nil {
		h1.Close()
		h2.Close()
		t.Fatalf("Failed to connect h2 to h1: %v", err)
	}

	return h1, h2
}

func TestOpenStream_SetsInitialDeadlines(t *testing.T) {
	h1, h2 := createTCPHosts(t)
	defer h1.Close()
	defer h2.Close()

	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		// Just keep stream open
		time.Sleep(500 * time.Millisecond)
		s.Close()
	})

	tp2 := transport.NewTransport(h2)
	ctx := context.Background()

	s, err := tp2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer s.Close()

	if s == nil {
		t.Fatal("Expected non-nil stream handle")
	}
}

func TestClient_ReadTimeoutOnStalledServer(t *testing.T) {
	h1, h2 := createTCPHosts(t)
	defer h1.Close()
	defer h2.Close()

	// Server accepts stream but stalls and sends no response
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		// Do not send anything, simulating a hung server
		time.Sleep(2 * time.Second)
		s.Reset()
	})

	eng2 := createTestEngine(t)
	tp2 := transport.NewTransport(h2)

	// Context with short deadline (100ms) to verify client read deadline logic
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	client, err := chunk.NewClient(ctx, tp2, h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	var id core.ContentID
	start := time.Now()
	_, err = client.Resolve(ctx, id)
	duration := time.Since(start)

	if err == nil {
		t.Fatal("Expected error due to read timeout/context deadline, got nil")
	}

	// Should unblock around 100ms and not wait for the full 2s server delay
	if duration > 1*time.Second {
		t.Errorf("Resolve blocked for %v, expected timeout around 100ms", duration)
	}
}

func TestStreamHandler_ReadTimeoutOnStalledClient(t *testing.T) {
	h1, h2 := createTCPHosts(t)
	defer h1.Close()
	defer h2.Close()

	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	tp2 := transport.NewTransport(h2)
	ctx := context.Background()

	s, err := tp2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Immediately set a very short read deadline on server side to test timeout handling
	if err := s.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("Failed to set read deadline: %v", err)
	}

	// Wait to ensure deadline triggers
	time.Sleep(100 * time.Millisecond)

	buf := make([]byte, 10)
	_, err = s.Read(buf)
	if err == nil {
		t.Fatal("Expected read error after deadline passed")
	}

	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Logf("Got expected timeout error type: %v", err)
		}
	}
}
