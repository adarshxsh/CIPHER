package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

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

func TestChunkProtocol_MultiTransactionRejection(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	var testID core.ContentID
	mBytes := []byte("test manifest content")
	eng1.PutManifestBytes(ctx, testID, mBytes)

	tr2 := transport.NewTransport(h2)
	stream, err := tr2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// Transaction 1: Should succeed
	req1 := chunk.BuildRequestManifest(testID)
	if err := chunk.WriteMessage(stream, req1); err != nil {
		t.Fatalf("Failed to write request 1: %v", err)
	}
	resp1, err := chunk.ReadMessage(stream)
	if err != nil {
		t.Fatalf("Failed to read response 1: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MsgManifest, got %d", resp1.Type)
	}

	// Transaction 2 on same stream: Should be rejected because stream was closed by server
	req2 := chunk.BuildRequestManifest(testID)
	_ = chunk.WriteMessage(stream, req2)

	resp2, err := chunk.ReadMessage(stream)
	if err == nil {
		t.Fatalf("Expected error or EOF on 2nd transaction over same stream, got msg type: %d", resp2.Type)
	}
}

func TestChunkProtocol_MaxMessagesPerStreamExceeded(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	chunkData := &core.Chunk{
		Header: core.ChunkHeader{Index: 0},
		Data:   make([]byte, 1024),
	}
	if err := eng1.PutChunk(ctx, chunkData); err != nil {
		t.Fatalf("Failed to put chunk: %v", err)
	}

	tr2 := transport.NewTransport(h2)
	stream, err := tr2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// Send RequestChunk (message 1)
	req := chunk.BuildRequestChunk(chunkData.Header.ID)
	if err := chunk.WriteMessage(stream, req); err != nil {
		t.Fatalf("Failed to write request: %v", err)
	}
	_, err = chunk.ReadMessage(stream) // receive MsgChunk
	if err != nil {
		t.Fatalf("Failed to read chunk response: %v", err)
	}

	// Send 4 ACK messages (messages 2, 3, 4, 5). messageCount exceeds MaxMessagesPerStream (4).
	ackMsg := chunk.BuildAck(chunkData.Header.ID, 0)
	var streamErr error
	for i := 0; i < 4; i++ {
		if err := chunk.WriteMessage(stream, ackMsg); err != nil {
			streamErr = err
			break
		}
	}

	if streamErr == nil {
		_, streamErr = chunk.ReadMessage(stream)
	}

	if streamErr == nil {
		t.Fatalf("Expected stream to fail after exceeding MaxMessagesPerStream")
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
