package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
)

func setupRealNetwork(t testing.TB) (host.Host, host.Host) {
	ctx := context.Background()
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create host 1: %v", err)
	}
	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		h1.Close()
		t.Fatalf("Failed to create host 2: %v", err)
	}

	t.Cleanup(func() {
		h1.Close()
		h2.Close()
	})

	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), peerstore.PermanentAddrTTL)
	if err := h2.Connect(ctx, peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}); err != nil {
		t.Fatalf("Failed to connect hosts: %v", err)
	}

	return h1, h2
}

func TestHandler_ReadDeadline(t *testing.T) {
	origTimeout := chunk.StreamTimeout
	chunk.StreamTimeout = 100 * time.Millisecond
	defer func() { chunk.StreamTimeout = origTimeout }()

	h1, h2 := setupRealNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Peer 2 opens stream but sends no message. Server deadline expires.
	time.Sleep(250 * time.Millisecond)

	// Attempting to read should fail because server closed the stream on deadline expiration.
	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatalf("Expected error when reading from stream after read deadline expired, got nil")
	}
}

func TestHandler_TransactionLimitExceeded(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()

	// Ingest test manifest
	data := make([]byte, 1024)
	rand.Read(data)
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Transaction 1: REQUEST_MANIFEST
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req1); err != nil {
		t.Fatalf("Failed to write first request: %v", err)
	}

	resp1, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read first response: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MANIFEST response, got %d", resp1.Type)
	}

	// Transaction 2 on same stream: REQUEST_MANIFEST again
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req2); err != nil {
		t.Fatalf("Failed to write second request: %v", err)
	}

	resp2, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read second response: %v", err)
	}

	if resp2.Type != chunk.MsgError {
		t.Fatalf("Expected MsgError for transaction limit violation, got type %d", resp2.Type)
	}

	code, errMsg, err := chunk.ParseError(resp2.Payload)
	if err != nil {
		t.Fatalf("Failed to parse error payload: %v", err)
	}

	if code != chunk.ErrBadRequest || errMsg != "transaction limit exceeded" {
		t.Errorf("Unexpected error response: code=%d, msg=%s", code, errMsg)
	}
}

func TestHandler_MessageLimitExceeded(t *testing.T) {
	origLimit := chunk.MaxMessagesPerStream
	chunk.MaxMessagesPerStream = 1
	defer func() { chunk.MaxMessagesPerStream = origLimit }()

	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Message 1: REQUEST_CHUNK (msgCount becomes 1, which equals MaxMessagesPerStream)
	req := chunk.BuildRequestChunk(m.ChunkIDs[0])
	if err := chunk.WriteMessage(s, req); err != nil {
		t.Fatalf("Failed to send REQUEST_CHUNK: %v", err)
	}

	resp, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read CHUNK response: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("Expected MsgChunk, got type %d", resp.Type)
	}

	// Message 2: ACK (msgCount becomes 2, exceeding MaxMessagesPerStream = 1)
	ack := chunk.BuildAck(m.ChunkIDs[0], 0)
	if err := chunk.WriteMessage(s, ack); err != nil {
		t.Fatalf("Failed to send ACK: %v", err)
	}

	resp2, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read error response: %v", err)
	}

	if resp2.Type != chunk.MsgError {
		t.Fatalf("Expected MsgError when message limit exceeded on ACK, got type %d", resp2.Type)
	}

	code, errMsg, err := chunk.ParseError(resp2.Payload)
	if err != nil {
		t.Fatalf("Failed to parse error payload: %v", err)
	}

	if code != chunk.ErrBadRequest || errMsg != "message limit exceeded" {
		t.Errorf("Unexpected error response: code=%d, msg=%s", code, errMsg)
	}
}

func TestHandler_AckSubReadDeadlineExceeded(t *testing.T) {
	origTimeout := chunk.StreamTimeout
	chunk.StreamTimeout = 100 * time.Millisecond
	defer func() { chunk.StreamTimeout = origTimeout }()

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

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Send REQUEST_CHUNK
	req := chunk.BuildRequestChunk(m.ChunkIDs[0])
	if err := chunk.WriteMessage(s, req); err != nil {
		t.Fatalf("Failed to send REQUEST_CHUNK: %v", err)
	}

	// Read CHUNK response
	resp, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read CHUNK response: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("Expected MsgChunk, got type %d", resp.Type)
	}

	// Sleep longer than StreamTimeout before sending ACK
	time.Sleep(250 * time.Millisecond)

	ack := chunk.BuildAck(m.ChunkIDs[0], 0)
	_ = chunk.WriteMessage(s, ack)

	// Stream should be closed by server due to ACK read deadline expiration
	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatalf("Expected stream to be closed after ACK read deadline expired")
	}
}
