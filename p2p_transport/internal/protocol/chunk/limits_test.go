package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestStreamTransactionLimitEnforcement(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	// Setup Handler with strict limit of 1 transaction per stream
	chunk.NewStreamHandlerWithLimits(h1, eng1, 1, 4)

	ctx := context.Background()
	data := make([]byte, 512*1024) // 2 chunks
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	tr := transport.NewTransport(h2)

	// Open a SINGLE raw stream directly to test bypassing client auto-reconnect
	s, err := tr.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Transaction 1: Request Manifest (Allowed)
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(s, req1); err != nil {
		t.Fatalf("Failed to write transaction 1 message: %v", err)
	}

	resp1, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Expected transaction 1 to succeed, got: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MANIFEST, got type %d", resp1.Type)
	}

	// Transaction 2 over SAME stream: Request Chunk (Must be rejected & stream reset by server)
	req2 := chunk.BuildRequestChunk(m.ChunkIDs[0])
	if err := chunk.WriteMessage(s, req2); err != nil {
		// Server might have already closed or reset
		return
	}

	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatalf("Expected transaction 2 over single stream to fail due to transaction limit enforcement")
	}
}

func TestStreamFrameCountLimitEnforcement(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	// Setup Handler with max 2 messages per stream
	chunk.NewStreamHandlerWithLimits(h1, eng1, 10, 2)

	ctx := context.Background()
	tr := transport.NewTransport(h2)

	s, err := tr.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Send 3 messages over same stream (exceeds max 2 messages)
	var contentID core.ContentID
	req := chunk.BuildRequestManifest(contentID)

	// Message 1
	if err := chunk.WriteMessage(s, req); err != nil {
		t.Fatalf("WriteMessage 1 failed: %v", err)
	}
	_, _ = chunk.ReadMessage(s)

	// Message 2
	if err := chunk.WriteMessage(s, req); err != nil {
		// Might fail if server reset
		return
	}

	// Read or write message 3 should fail as server resets stream
	_, err = chunk.ReadMessage(s)
	if err == nil {
		_ = chunk.WriteMessage(s, req)
		_, err = chunk.ReadMessage(s)
		if err == nil {
			t.Fatalf("Expected stream to be reset after exceeding frame count limit")
		}
	}
}

func TestOversizedPayloadRejectedBeforeAllocation(t *testing.T) {
	// Construct a raw wire frame claiming 1MB payload for a MsgRequestChunk (max allowed is 512 bytes)
	var buf bytes.Buffer

	declaredSize := uint32(1024*1024 + 3) // 1MB + 3 bytes header
	_ = binary.Write(&buf, binary.LittleEndian, declaredSize)
	_ = binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion)
	buf.WriteByte(byte(chunk.MsgRequestChunk))

	// ReadMessage must reject this before allocating 1MB
	_, err := chunk.ReadMessage(&buf)
	if err == nil {
		t.Fatalf("Expected error reading oversized payload frame")
	}
}

func TestCryptoKeyValidation(t *testing.T) {
	enc := crypto.NewChaCha20Encryptor()
	chunkData := &core.Chunk{
		Header: core.ChunkHeader{PlainSize: 4},
		Data:   []byte("test"),
	}

	invalidKeys := [][]byte{
		nil,
		make([]byte, 16),
		make([]byte, 31),
		make([]byte, 33),
		make([]byte, 64),
	}

	for _, badKey := range invalidKeys {
		err := enc.EncryptChunk(badKey, chunkData)
		if err == nil {
			t.Errorf("Expected EncryptChunk to fail for key length %d", len(badKey))
		}

		err = enc.DecryptChunk(badKey, chunkData)
		if err == nil {
			t.Errorf("Expected DecryptChunk to fail for key length %d", len(badKey))
		}

		keys := engine.NewLocalKeyProvider()
		var contentID core.ContentID
		err = keys.Put(context.Background(), contentID, badKey)
		if err == nil {
			t.Errorf("Expected KeyProvider.Put to fail for key length %d", len(badKey))
		}
	}
}

func TestHighConcurrencyTransferLoads(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	fileSize := 1024 * 1024 // 1MB
	data := make([]byte, fileSize)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Run concurrent client downloads
	const numConcurrent = 10
	var wg sync.WaitGroup
	errCh := make(chan error, numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			engWorker := createTestEngine(t)
			client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), engWorker)
			if err != nil {
				errCh <- fmt.Errorf("failed to create client: %w", err)
				return
			}
			defer client.Close()

			manifestData, err := client.Resolve(ctx, m.Descriptor.ID)
			if err != nil {
				errCh <- fmt.Errorf("resolve failed: %w", err)
				return
			}

			m2, err := manifest.Deserialize(manifestData)
			if err != nil {
				errCh <- fmt.Errorf("deserialize failed: %w", err)
				return
			}

			if err := client.Download(ctx, m2.ChunkIDs); err != nil {
				errCh <- fmt.Errorf("download failed: %w", err)
				return
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("Concurrent worker failed: %v", err)
	}
}
