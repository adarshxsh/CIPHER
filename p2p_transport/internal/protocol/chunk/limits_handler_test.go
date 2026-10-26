package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestStreamLimits_TransactionLimitEnforced(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

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

	// Create client with multiple transactions
	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Perform 1st transaction: Resolve manifest
	_, err = client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	// Perform 2nd transaction: Fetch chunk (client should automatically open a new stream and succeed)
	_, err = client.FetchChunk(ctx, m.ChunkIDs[0])
	if err != nil {
		t.Fatalf("FetchChunk failed on renewed stream: %v", err)
	}
}

func TestStreamLimits_ServerRejectsExcessTransactionsOnSameStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	t2 := transport.NewTransport(h2)

	// Open a raw libp2p stream manually to test single-stream transaction limit enforcement
	s, err := t2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open raw stream: %v", err)
	}
	defer s.Close()

	// Transaction 1: REQUEST_MANIFEST
	var id core.ContentID
	req1 := chunk.BuildRequestManifest(id)
	if err := chunk.WriteMessage(s, req1); err != nil {
		t.Fatalf("WriteMessage 1 failed: %v", err)
	}
	resp1, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("ReadMessage 1 failed: %v", err)
	}
	if resp1.Type != chunk.MsgError {
		t.Fatalf("Expected MsgError for unknown ContentID, got %d", resp1.Type)
	}

	// Transaction 2 on the SAME stream: REQUEST_MANIFEST again
	req2 := chunk.BuildRequestManifest(id)
	if err := chunk.WriteMessage(s, req2); err != nil {
		t.Fatalf("WriteMessage 2 failed: %v", err)
	}
	resp2, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("ReadMessage 2 failed: %v", err)
	}
	if resp2.Type != chunk.MsgError {
		t.Fatalf("Expected MsgError for exceeding max transactions, got %d", resp2.Type)
	}
	code, msg, err := chunk.ParseError(resp2.Payload)
	if err != nil {
		t.Fatalf("ParseError failed: %v", err)
	}
	if code != chunk.ErrBadRequest || msg != "exceeded maximum transactions per stream" {
		t.Errorf("Unexpected error code/msg: [%d] %s", code, msg)
	}
}

func TestStreamLimits_ServerReadDeadline(t *testing.T) {
	// Set short read timeout for test
	oldTimeout := chunk.StreamReadTimeout
	chunk.StreamReadTimeout = 50 * time.Millisecond
	defer func() { chunk.StreamReadTimeout = oldTimeout }()

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

	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	if err := h2.Connect(ctx, peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}); err != nil {
		t.Fatal(err)
	}

	t2 := transport.NewTransport(h2)

	s, err := t2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Write 1 byte to force stream handler initiation on server
	if _, err := s.Write([]byte{0x00}); err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	// Server handleStream is now active and waiting on ReadMessage with 50ms readTimer.
	// Sleep to let read deadline expire and reset stream.
	time.Sleep(150 * time.Millisecond)

	var id core.ContentID
	req := chunk.BuildRequestManifest(id)
	_ = chunk.WriteMessage(s, req)

	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatal("Expected error on read from stream reset due to timeout")
	}
}
