package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func setupRealNetwork(t testing.TB) (host.Host, host.Host) {
	ctx := context.Background()
	h1, _, err := transport.NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("failed to create h1: %v", err)
	}
	t.Cleanup(func() { h1.Close() })

	h2, _, err := transport.NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("failed to create h2: %v", err)
	}
	t.Cleanup(func() { h2.Close() })

	err = h2.Connect(ctx, peer.AddrInfo{
		ID:    h1.ID(),
		Addrs: h1.Addrs(),
	})
	if err != nil {
		t.Fatalf("failed to connect h2 to h1: %v", err)
	}
	return h1, h2
}

func TestChunkProtocol_TimeoutOnIdleStream(t *testing.T) {
	h1, h2 := setupRealNetwork(t)
	eng1 := createTestEngine(t)

	// Register handler on h1 with 100ms idle timeout
	chunk.NewStreamHandler(h1, eng1, transport.WithIdleTimeout(100*time.Millisecond))

	// h2 opens stream to h1 and then goes idle
	tr2 := transport.NewTransport(h2)
	ctx := context.Background()
	s, err := tr2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}
	defer s.Close()

	// Wrap s with a short timeout on client side too
	ts := transport.NewTimeoutStream(s, transport.WithIdleTimeout(100*time.Millisecond))

	// Try reading from idle stream after deadline passes
	buf := make([]byte, 100)
	_, err = ts.Read(buf)
	if err == nil {
		t.Fatal("expected read to fail on timeout from idle stream")
	}
}

func TestChunkProtocol_TimeoutOnMissingACK(t *testing.T) {
	h1, h2 := setupRealNetwork(t)
	eng1 := createTestEngine(t)

	// Register handler on h1 with 100ms idle timeout
	chunk.NewStreamHandler(h1, eng1, transport.WithIdleTimeout(100*time.Millisecond))

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest data: %v", err)
	}

	tr2 := transport.NewTransport(h2)
	s, err := tr2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}
	defer s.Close()

	// Decorate client stream with short timeout
	ts := transport.NewTimeoutStream(s, transport.WithIdleTimeout(150*time.Millisecond))

	// Send REQUEST_CHUNK
	req := chunk.BuildRequestChunk(m.ChunkIDs[0])
	if err := chunk.WriteMessage(ts, req); err != nil {
		t.Fatalf("failed to write request chunk: %v", err)
	}

	// Read CHUNK response
	resp, err := chunk.ReadMessage(ts)
	if err != nil {
		t.Fatalf("failed to read chunk response: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("expected CHUNK response, got type %d", resp.Type)
	}

	// Intentionally DO NOT send ACK, letting server ACK wait loop time out!
	// Server stream handler should time out on ACK read and exit cleanly.
	time.Sleep(250 * time.Millisecond)

	// Future reads on this stream should now fail as server reset/closed stream on ACK timeout
	_, err = chunk.ReadMessage(ts)
	if err == nil {
		t.Fatal("expected stream read to fail after missing ACK timeout")
	}
}
