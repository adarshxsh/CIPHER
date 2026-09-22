package chunk_test

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
)

func setupRealNetwork(t testing.TB) (host.Host, host.Host) {
	ctx := context.Background()
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create h1: %v", err)
	}
	t.Cleanup(func() { h1.Close() })

	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create h2: %v", err)
	}
	t.Cleanup(func() { h2.Close() })

	peerInfo := peer.AddrInfo{
		ID:    h1.ID(),
		Addrs: h1.Addrs(),
	}
	if err := h2.Connect(ctx, peerInfo); err != nil {
		t.Fatalf("Failed to connect h2 to h1: %v", err)
	}
	return h1, h2
}

func TestStreamLimits_ReadDeadlineTimeout(t *testing.T) {
	h1, h2 := setupRealNetwork(t)
	eng1 := createTestEngine(t)

	// Set short timeout of 150ms for read deadline test
	chunk.NewStreamHandler(h1, eng1, chunk.WithStreamTimeout(150*time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	start := time.Now()
	// Read from stream without sending anything. Handler should hit read deadline timeout and reset/close stream.
	buf := make([]byte, 100)
	_, err = s.Read(buf)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Expected error/EOF on idle stream deadline timeout, got nil")
	}

	if elapsed > 3*time.Second {
		t.Errorf("Read deadline took too long to expire: %v", elapsed)
	}
}

func TestStreamLimits_MaxTransactionsPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	var dummyID core.ContentID
	req1 := chunk.BuildRequestManifest(dummyID)

	// Transaction 1: Send request
	if err := chunk.WriteMessage(s, req1); err != nil {
		t.Fatalf("Failed to write first request: %v", err)
	}

	// Read response 1 (error: manifest not found)
	resp1, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read response 1: %v", err)
	}
	if resp1.Type != chunk.MsgError {
		t.Fatalf("Expected MsgError, got %d", resp1.Type)
	}

	// Stream should now be closed by server because MaxTransactionsPerStream (1) was reached.
	// Transaction 2: Try to send second request on the same stream.
	req2 := chunk.BuildRequestManifest(dummyID)
	_ = chunk.WriteMessage(s, req2)

	// Attempting to read response 2 should fail because stream was closed after transaction 1.
	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatal("Expected error reading 2nd transaction response on same stream, got nil")
	}
}

func TestStreamLimits_MaxMessagesPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Send unsupported message type
	badMsg := &chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    0xFF, // unsupported
		Payload: []byte("test"),
	}

	if err := chunk.WriteMessage(s, badMsg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	// Read response
	_, _ = chunk.ReadMessage(s)

	// Server resets stream on unsupported message type
	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatal("Expected stream to be closed after unsupported message type")
	}
}
