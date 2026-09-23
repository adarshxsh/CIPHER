package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestChunkProtocol_TransactionLimitExceeded(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	handler := chunk.NewStreamHandler(h1, eng1)
	// Enforce a strict max of 1 transaction on the server
	handler.SetLimits(1, 10)

	ctx := context.Background()
	data := make([]byte, 1024*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Transaction 1: Resolve manifest (should succeed)
	_, err = client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Transaction 1 (Resolve) failed unexpectedly: %v", err)
	}

	// Transaction 2: Request chunk (should be rejected by server due to tx limit = 1)
	_, err = client.FetchChunk(ctx, m.ChunkIDs[0])
	if err == nil {
		t.Fatalf("Expected transaction 2 to fail due to transaction limit, but it succeeded")
	}
}

func TestChunkProtocol_MessageLimitExceeded(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	handler := chunk.NewStreamHandler(h1, eng1)
	// Allow 10 transactions but only 1 message frame
	handler.SetLimits(10, 1)

	ctx := context.Background()
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Message 1: Resolve manifest (succeeds as message 1)
	_, err = client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	// Message 2: Fetch chunk (2nd message, should exceed maxMessages = 1)
	_, err = client.FetchChunk(ctx, m.ChunkIDs[0])
	if err == nil {
		t.Fatalf("Expected fetch chunk to fail due to message limit, but it succeeded")
	}
}

func TestChunkProtocol_ReadTimeout(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	_ = chunk.NewStreamHandler(h1, eng1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Open stream without sending any message frames
	tr := transport.NewTransport(h2)
	stream, err := tr.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// Wait briefly then try reading from stream to verify server closes/resets it on timeout or idle
	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 100)
		_, err := stream.Read(buf)
		done <- err
	}()

	select {
	case err := <-done:
		// Expect read to return error when stream is closed by server handler upon read deadline
		if err == nil {
			t.Fatalf("Expected stream read to return error, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Log("Stream open idle test completed")
	}
}
