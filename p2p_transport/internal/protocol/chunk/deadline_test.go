package chunk_test

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/content/core"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func setupRealNetwork(t testing.TB) (host.Host, host.Host) {
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		h1.Close()
		h2.Close()
	})

	ctx := context.Background()
	h2Info := host.InfoFromHost(h2)
	if err := h1.Connect(ctx, *h2Info); err != nil {
		t.Fatal(err)
	}
	return h1, h2
}

func TestChunkProtocol_IdleReadDeadline(t *testing.T) {
	origTimeout := chunk.StreamReadTimeout
	chunk.StreamReadTimeout = 100 * time.Millisecond
	defer func() { chunk.StreamReadTimeout = origTimeout }()

	h1, h2 := setupRealNetwork(t)
	eng1 := createTestEngine(t)

	// Setup StreamHandler on server h1
	chunk.NewStreamHandler(h1, eng1)

	// Open stream from h2 to h1 without sending any messages
	t2 := transport.NewTransport(h2)
	stream, err := t2.OpenStream(context.Background(), h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// Wait for idle read deadline to expire on server
	time.Sleep(250 * time.Millisecond)

	// Attempting to read on the stream should indicate closure/timeout
	readBuf := make([]byte, 10)
	_, err = stream.Read(readBuf)
	if err == nil {
		t.Errorf("Expected stream to be closed after idle read deadline expiration")
	}
}

func TestChunkProtocol_ClientReadDeadline(t *testing.T) {
	origTimeout := chunk.StreamReadTimeout
	chunk.StreamReadTimeout = 100 * time.Millisecond
	defer func() { chunk.StreamReadTimeout = origTimeout }()

	h1, h2 := setupRealNetwork(t)
	eng2 := createTestEngine(t)

	// Register a dummy stream handler on h1 that accepts stream but stalls without writing responses
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		// Stall indefinitely
		time.Sleep(1 * time.Second)
	})

	client, err := chunk.NewClient(context.Background(), transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	var id core.ContentID
	start := time.Now()
	_, err = client.Resolve(context.Background(), id)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("Expected client Resolve to fail due to read deadline timeout")
	}

	if elapsed > 500*time.Millisecond {
		t.Errorf("Client took too long to time out: %v", elapsed)
	}
}

func TestChunkProtocol_ClientWriteDeadline(t *testing.T) {
	origTimeout := chunk.StreamWriteTimeout
	chunk.StreamWriteTimeout = -1 * time.Second // Expired write deadline
	defer func() { chunk.StreamWriteTimeout = origTimeout }()

	h1, h2 := setupRealNetwork(t)
	eng2 := createTestEngine(t)

	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
	})

	client, err := chunk.NewClient(context.Background(), transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	var id core.ContentID
	_, err = client.Resolve(context.Background(), id)
	if err == nil {
		t.Fatalf("Expected client Resolve to fail when write deadline is exceeded")
	}
}
