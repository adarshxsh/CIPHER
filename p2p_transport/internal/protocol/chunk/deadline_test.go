package chunk_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func setupRealNetworkForDeadline(t testing.TB) (host.Host, host.Host) {
	ctx := context.Background()
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h1.Close() })

	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h2.Close() })

	if err := h1.Connect(ctx, h2.Peerstore().PeerInfo(h2.ID())); err != nil {
		t.Fatal(err)
	}
	return h1, h2
}

func TestStreamDeadlines_StalledStreamCloses(t *testing.T) {
	origTimeout := chunk.StreamTimeout
	chunk.StreamTimeout = 150 * time.Millisecond
	t.Cleanup(func() {
		chunk.StreamTimeout = origTimeout
	})

	h1, h2 := setupRealNetworkForDeadline(t)
	eng1 := createTestEngine(t)

	// Setup handler on h1 (server)
	chunk.NewStreamHandler(h1, eng1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Open a stream from h2 to h1 without sending anything
	stream, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// Wait for stream timeout (150ms + margin)
	time.Sleep(300 * time.Millisecond)

	// Attempting to read on stream from client should fail because server closed stream on deadline expiration
	buf := make([]byte, 10)
	_, err = stream.Read(buf)
	if err == nil {
		t.Fatalf("Expected read error on stalled stream after deadline expired, got nil")
	}
}

func TestStreamDeadlines_ActiveTransferSucceeds(t *testing.T) {
	origTimeout := chunk.StreamTimeout
	chunk.StreamTimeout = 500 * time.Millisecond
	t.Cleanup(func() {
		chunk.StreamTimeout = origTimeout
	})

	h1, h2 := setupRealNetworkForDeadline(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := []byte("active transfer test content")
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	tr2 := transport.NewTransport(h2)
	client, err := chunk.NewClient(ctx, tr2, h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// First operation
	resolvedData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	// Delay 200ms (less than 500ms timeout) before second operation
	time.Sleep(200 * time.Millisecond)

	m2, err := manifest.Deserialize(resolvedData)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// Download chunks with extended deadline per frame
	if err := client.Download(ctx, m2.ChunkIDs); err != nil {
		t.Fatalf("Download failed: %v", err)
	}
}

func TestStreamDeadlines_TimeoutWaitingForAck(t *testing.T) {
	origTimeout := chunk.StreamTimeout
	chunk.StreamTimeout = 150 * time.Millisecond
	t.Cleanup(func() {
		chunk.StreamTimeout = origTimeout
	})

	h1, h2 := setupRealNetworkForDeadline(t)
	eng1 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := []byte("sample chunk data for ack timeout test")
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Open stream directly to simulate a client that requests a chunk but never sends ACK
	stream, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// Send REQUEST_CHUNK message from client
	req := chunk.BuildRequestChunk(m.ChunkIDs[0])
	if err := chunk.WriteMessage(stream, req); err != nil {
		t.Fatalf("Failed to send chunk request: %v", err)
	}

	// Read CHUNK response from server
	resp, err := chunk.ReadMessage(stream)
	if err != nil {
		t.Fatalf("Failed to read chunk response: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("Expected CHUNK response, got %d", resp.Type)
	}

	// Now client intentionally stalls and DOES NOT send ACK back
	time.Sleep(300 * time.Millisecond)

	// Verify server closed stream due to ACK timeout
	var buf [10]byte
	_, err = stream.Read(buf[:])
	if err == nil {
		t.Fatalf("Expected error when reading from server-closed stream, got nil")
	}
}
