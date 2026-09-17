package chunk_test

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"

	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func setupRealHosts(t *testing.T) (host.Host, host.Host) {
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create host 1: %v", err)
	}

	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		h1.Close()
		t.Fatalf("Failed to create host 2: %v", err)
	}

	if err := h1.Connect(context.Background(), *host.InfoFromHost(h2)); err != nil {
		h1.Close()
		h2.Close()
		t.Fatalf("Failed to connect hosts: %v", err)
	}

	t.Cleanup(func() {
		h1.Close()
		h2.Close()
	})

	return h1, h2
}

func TestChunkProtocol_IdleStreamTimeout(t *testing.T) {
	h1, h2 := setupRealHosts(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	shortPolicy := transport.StreamPolicy{
		ReadTimeout:  150 * time.Millisecond,
		WriteTimeout: 150 * time.Millisecond,
		IdleTimeout:  150 * time.Millisecond,
	}

	// Server uses short policy
	chunk.NewStreamHandlerWithPolicy(h1, eng1, shortPolicy)

	// Client transport uses short policy
	tr2 := transport.NewTransportWithPolicy(h2, shortPolicy)
	client, err := chunk.NewClient(context.Background(), tr2, h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Idle wait longer than the 150ms timeout
	time.Sleep(300 * time.Millisecond)

	// Attempting an I/O operation on the idle stream should now fail due to timeout/stream closure
	var badID [32]byte
	_, err = client.Resolve(context.Background(), badID)
	if err == nil {
		t.Fatalf("Expected error when attempting operation on idle stream after timeout expired")
	}
}
