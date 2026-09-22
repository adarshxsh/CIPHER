package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/transport"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/chunk"
)

func createTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir()) // isolated per engine
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupMockNetwork(t testing.TB) (host.Host, host.Host) {
	mocknet := mocknet.New()

	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	h2, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return h1, h2
}

func TestChunkProtocol_Integration(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Setup Handler on Peer 1 (Server)
	chunk.NewStreamHandler(h1, eng1)
	
	// Ensure Peer 2 has the handler too (symmetric protocol requirement)
	chunk.NewStreamHandler(h2, eng2)

	// Peer 1 ingests 1MB file
	ctx := context.Background()
	dataSize := 1024 * 1024
	data := make([]byte, dataSize)
	rand.Read(data)
	
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Peer 2 wants to fetch it
	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// 1. Resolve Manifest
	resolvedData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Failed to resolve manifest: %v", err)
	}

	m2, err := manifest.Deserialize(resolvedData)
	if err != nil {
		t.Fatalf("Failed to deserialize manifest: %v", err)
	}

	// 2. Download
	if err := client.Download(ctx, m2.ChunkIDs); err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	// NOTE: We must give eng2 the decryption key to reassemble locally, as key transfer is out of scope.
	// Since keys aren't exposed, let's just verify download succeeds!
	
	if len(m2.ChunkIDs) != 4 { // 1MB / 256KB = 4 chunks
		t.Errorf("Expected 4 chunks, got %d", len(m2.ChunkIDs))
	}

	for _, chunkID := range m2.ChunkIDs {
		// verify eng2 has it
		if _, err := eng2.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("eng2 missing chunk %x", chunkID)
		}
	}
}

func TestChunkProtocol_InvalidPeer(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	client, err := chunk.NewClient(context.Background(), transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	var badID core.ContentID
	_, err = client.Resolve(context.Background(), badID)
	if err == nil {
		t.Fatalf("Expected error for invalid ContentID")
	}
	if err.Error() != "remote error (code 1): manifest not found" {
		t.Errorf("Unexpected error msg: %v", err)
	}
}

func TestChunkProtocol_ReadDeadlineTimeout(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	// Set short read timeout of 50ms
	chunk.NewStreamHandler(h1, eng1, chunk.WithReadTimeout(50*time.Millisecond))

	t2 := transport.NewTransport(h2)
	s, err := t2.OpenStream(context.Background(), h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Wait past the deadline
	time.Sleep(100 * time.Millisecond)

	// Attempting to read should fail because stream was closed by server after read deadline
	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatalf("Expected error when reading from stream that timed out")
	}
}

func TestChunkProtocol_TransactionLimitExceeded(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	ctx := context.Background()

	data := []byte("test content")
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Limit server to 1 transaction per stream
	chunk.NewStreamHandler(h1, eng1, chunk.WithMaxTransactions(1))

	t2 := transport.NewTransport(h2)
	s, err := t2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
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
		t.Fatalf("Expected MsgManifest, got %d", resp1.Type)
	}

	// Transaction 2 on same stream should fail with limit error
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req2); err != nil {
		// Writing might fail if stream was already closed
		return
	}

	resp2, err := chunk.ReadMessage(s)
	if err != nil {
		// Closed stream error is also valid enforcement
		return
	}
	if resp2.Type == chunk.MsgError {
		code, msgStr, _ := chunk.ParseError(resp2.Payload)
		if code != chunk.ErrPermissionDenied {
			t.Errorf("Expected ErrPermissionDenied (4), got %d (%s)", code, msgStr)
		}
	} else {
		t.Errorf("Expected MsgError for transaction limit, got type %d", resp2.Type)
	}
}

func TestChunkProtocol_MessageLimitExceeded(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	ctx := context.Background()

	data := []byte("test content")
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Allow unlimited transactions but max 1 message
	chunk.NewStreamHandler(h1, eng1, chunk.WithMaxTransactions(100), chunk.WithMaxMessages(1))

	t2 := transport.NewTransport(h2)
	s, err := t2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Message 1: valid
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req1); err != nil {
		t.Fatalf("Failed to write first request: %v", err)
	}
	resp1, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read first response: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MsgManifest, got %d", resp1.Type)
	}

	// Message 2: exceeds max 1 message
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req2); err != nil {
		return
	}
	resp2, err := chunk.ReadMessage(s)
	if err != nil {
		return
	}
	if resp2.Type == chunk.MsgError {
		code, msgStr, _ := chunk.ParseError(resp2.Payload)
		if code != chunk.ErrPermissionDenied {
			t.Errorf("Expected ErrPermissionDenied (4), got %d (%s)", code, msgStr)
		}
	} else {
		t.Errorf("Expected MsgError for message limit, got type %d", resp2.Type)
	}
}
