package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func createHandlerTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func TestStreamHandler_Options(t *testing.T) {
	h1, _ := setupMockNetwork(t)
	eng := createHandlerTestEngine(t)

	handler := chunk.NewStreamHandler(h1, eng,
		chunk.WithReadTimeout(5*time.Second),
		chunk.WithWriteTimeout(10*time.Second),
		chunk.WithMaxTransactionsPerStream(2),
		chunk.WithMaxMessagesPerStream(5),
	)

	if handler.ReadTimeout != 5*time.Second {
		t.Errorf("Expected ReadTimeout 5s, got %v", handler.ReadTimeout)
	}
	if handler.WriteTimeout != 10*time.Second {
		t.Errorf("Expected WriteTimeout 10s, got %v", handler.WriteTimeout)
	}
	if handler.MaxTransactionsPerStream != 2 {
		t.Errorf("Expected MaxTransactionsPerStream 2, got %d", handler.MaxTransactionsPerStream)
	}
	if handler.MaxMessagesPerStream != 5 {
		t.Errorf("Expected MaxMessagesPerStream 5, got %d", handler.MaxMessagesPerStream)
	}
}

func TestStreamHandler_ReadTimeout(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createHandlerTestEngine(t)
	eng2 := createHandlerTestEngine(t)

	// Set short read timeout of 100ms
	chunk.NewStreamHandler(h1, eng1, chunk.WithReadTimeout(100*time.Millisecond))

	ctx := context.Background()
	tr := transport.NewTransport(h2)

	// Open stream but do not send any message
	client, err := chunk.NewClient(ctx, tr, h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Wait longer than the read timeout
	time.Sleep(250 * time.Millisecond)

	// Attempt to resolve manifest on timed-out stream should fail
	var dummyID core.ContentID
	_, err = client.Resolve(ctx, dummyID)
	if err == nil {
		t.Fatalf("Expected error after read timeout on idle stream")
	}
}

func TestStreamHandler_MaxTransactionsPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createHandlerTestEngine(t)
	eng2 := createHandlerTestEngine(t)

	// Allow only 1 transaction per stream
	chunk.NewStreamHandler(h1, eng1, chunk.WithMaxTransactionsPerStream(1))

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

	// Transaction 1: Resolve Manifest (succeeds)
	_, err = client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("First transaction failed: %v", err)
	}

	// Transaction 2 on same stream should fail due to transaction limit
	_, err = client.FetchChunk(ctx, m.ChunkIDs[0])
	if err == nil {
		t.Fatalf("Expected second transaction to fail when limit is 1")
	}
}

func TestStreamHandler_MaxMessagesPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createHandlerTestEngine(t)
	eng2 := createHandlerTestEngine(t)

	// Allow 2 messages per stream (1 RequestChunk + 1 ACK). Third message will exceed limit.
	chunk.NewStreamHandler(h1, eng1,
		chunk.WithMaxMessagesPerStream(2),
		chunk.WithMaxTransactionsPerStream(10), // allow transactions so message limit is tested
	)

	ctx := context.Background()
	data := make([]byte, 1024*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// First chunk fetch sends RequestChunk (msg 1) and ACK (msg 2) -> succeeds
	_, err = client.FetchChunk(ctx, m.ChunkIDs[0])
	if err != nil {
		t.Fatalf("First chunk fetch failed unexpectedly: %v", err)
	}

	// Second chunk fetch sends RequestChunk (msg 3) -> exceeds MaxMessagesPerStream (2)
	_, err = client.FetchChunk(ctx, m.ChunkIDs[1])
	if err == nil {
		t.Fatalf("Expected second chunk fetch to fail due to message limit violation")
	}
}
