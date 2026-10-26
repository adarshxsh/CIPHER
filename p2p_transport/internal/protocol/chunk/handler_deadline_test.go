package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func setupRealNetwork(t testing.TB) (host.Host, host.Host) {
	ctx := context.Background()
	h1, kdht1, err := transport.NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatal(err)
	}
	h2, kdht2, err := transport.NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatal(err)
	}

	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), time.Hour)
	t.Cleanup(func() {
		kdht1.Close()
		kdht2.Close()
		h1.Close()
		h2.Close()
	})
	return h1, h2
}

func TestStreamHandler_IdleStreamTimeout(t *testing.T) {
	oldTimeout := chunk.StreamTimeout
	chunk.StreamTimeout = 150 * time.Millisecond
	defer func() { chunk.StreamTimeout = oldTimeout }()

	h1, h2 := setupRealNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	tr := transport.NewTransport(h2)
	s, err := tr.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Idle stream: client opens stream but does not send any data.
	// After server's read deadline expires (150ms), server closes/resets stream.
	buf := make([]byte, 10)
	_, err = s.Read(buf)
	if err == nil {
		t.Fatal("Expected error/EOF on idle stream after deadline timeout, got nil")
	}
}

func TestStreamHandler_MaxTransactionsPerStream_AutomaticClosure(t *testing.T) {
	h1, h2 := setupRealNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	tr := transport.NewTransport(h2)
	s, err := tr.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Transaction 1: Request Manifest
	req := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req); err != nil {
		t.Fatalf("Failed to write request: %v", err)
	}

	resp, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read manifest response: %v", err)
	}
	if resp.Type != chunk.MsgManifest {
		t.Fatalf("Expected MsgManifest, got %d", resp.Type)
	}

	// Server should close the stream after MaxTransactionsPerStream = 1 is reached.
	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatalf("Expected stream to be closed after transaction limit reached, but ReadMessage succeeded")
	}
}

func TestStreamHandler_ExceedTransactionLimit(t *testing.T) {
	h1, h2 := setupRealNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	tr := transport.NewTransport(h2)
	s, err := tr.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Send Request 1
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req1); err != nil {
		t.Fatalf("Failed to write request 1: %v", err)
	}

	// Read response 1
	_, err = chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read response 1: %v", err)
	}

	// Immediately send Request 2 on the same stream
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	_ = chunk.WriteMessage(s, req2)

	resp2, err2 := chunk.ReadMessage(s)
	if err2 == nil {
		if resp2.Type == chunk.MsgError {
			code, msg, _ := chunk.ParseError(resp2.Payload)
			t.Logf("Received expected error frame: [%d] %s", code, msg)
		} else {
			t.Fatalf("Expected error frame or closed stream on second request, got type %d", resp2.Type)
		}
	}
}
