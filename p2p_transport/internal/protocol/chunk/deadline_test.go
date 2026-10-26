package chunk_test

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peerstore"

	"cipher/internal/content/core"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func setupTCPHosts(t testing.TB) (host.Host, host.Host) {
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create host 1: %v", err)
	}
	t.Cleanup(func() { h1.Close() })

	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create host 2: %v", err)
	}
	t.Cleanup(func() { h2.Close() })

	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), peerstore.PermanentAddrTTL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := h2.Connect(ctx, h1.Peerstore().PeerInfo(h1.ID())); err != nil {
		t.Fatalf("Failed to connect hosts: %v", err)
	}

	return h1, h2
}

func TestChunkClient_ContextDeadline_Resolve(t *testing.T) {
	h1, h2 := setupTCPHosts(t)

	// Server registers handler that stalls without responding
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		time.Sleep(2 * time.Second)
	})

	eng2 := createTestEngine(t)
	t2 := transport.NewTransport(h2)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	client, err := chunk.NewClient(ctx, t2, h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	var id core.ContentID
	start := time.Now()
	_, err = client.Resolve(ctx, id)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Expected error due to context deadline expiration, got nil")
	}

	if elapsed > 1*time.Second {
		t.Fatalf("Resolve took %v, expected timeout near 100ms", elapsed)
	}
}

func TestChunkClient_ContextDeadline_FetchChunk(t *testing.T) {
	h1, h2 := setupTCPHosts(t)

	// Server registers handler that stalls without responding
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		time.Sleep(2 * time.Second)
	})

	eng2 := createTestEngine(t)
	t2 := transport.NewTransport(h2)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	client, err := chunk.NewClient(ctx, t2, h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	var chunkID core.ChunkID
	start := time.Now()
	_, err = client.FetchChunk(ctx, chunkID)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Expected error due to context deadline expiration, got nil")
	}

	if elapsed > 1*time.Second {
		t.Fatalf("FetchChunk took %v, expected timeout near 100ms", elapsed)
	}
}
