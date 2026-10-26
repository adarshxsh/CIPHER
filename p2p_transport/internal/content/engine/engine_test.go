package engine

import (
	"bytes"
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
)

func TestContentEngine_EndToEnd(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{
		ChunkSize: 32 * 1024, // 32KB
	}

	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)

	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	// Create random data larger than one chunk
	originalData := make([]byte, 100*1024+500) // ~100.5 KB
	rand.Seed(time.Now().UnixNano())
	rand.Read(originalData)

	ctx := context.Background()

	// Ingest
	reader := bytes.NewReader(originalData)
	m, err := eng.Ingest(ctx, reader, manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	// Verify Manifest
	if m.Descriptor.Size != uint64(len(originalData)) {
		t.Errorf("manifest size %d != expected %d", m.Descriptor.Size, len(originalData))
	}

	expectedChunks := (len(originalData) + int(config.ChunkSize) - 1) / int(config.ChunkSize)
	if len(m.ChunkIDs) != expectedChunks {
		t.Errorf("manifest chunks %d != expected %d", len(m.ChunkIDs), expectedChunks)
	}

	// Reassemble
	var outBuf bytes.Buffer
	if err := eng.Reassemble(ctx, m, &outBuf); err != nil {
		t.Fatalf("failed to reassemble: %v", err)
	}

	if !bytes.Equal(originalData, outBuf.Bytes()) {
		t.Errorf("reassembled data does not match original data")
	}
}

func TestContentEngine_CorruptedChunkAbort(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-corrupt-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)
	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	originalData := make([]byte, 100*1024)
	rand.Seed(time.Now().UnixNano())
	rand.Read(originalData)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	if len(m.ChunkIDs) < 3 {
		t.Fatalf("expected at least 3 chunks, got %d", len(m.ChunkIDs))
	}

	// Corrupt chunk 1 (second chunk) in store
	corruptChunkID := m.ChunkIDs[1]
	chunk, err := store.GetChunk(ctx, corruptChunkID)
	if err != nil {
		t.Fatalf("failed to get chunk from store: %v", err)
	}
	chunk.Data[0] ^= 0xFF
	if err := store.PutChunk(ctx, chunk); err != nil {
		t.Fatalf("failed to put corrupted chunk in store: %v", err)
	}

	var outBuf bytes.Buffer
	err = eng.Reassemble(ctx, m, &outBuf)
	if err == nil {
		t.Fatal("expected Reassemble to fail on corrupted chunk, but it succeeded")
	}

	// First chunk (32KB) should have been written before reaching corrupted second chunk
	if outBuf.Len() != 32*1024 {
		t.Errorf("expected outBuf length to be 32KB (1 chunk), got %d", outBuf.Len())
	}
}

func TestContentEngine_DecryptionFailureAbort(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-decrypt-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)
	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	originalData := make([]byte, 64*1024)
	rand.Seed(time.Now().UnixNano())
	rand.Read(originalData)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	// Set invalid key in key provider
	wrongKey := make([]byte, 32)
	keys.Put(ctx, m.Descriptor.ID, wrongKey)

	var outBuf bytes.Buffer
	err = eng.Reassemble(ctx, m, &outBuf)
	if err == nil {
		t.Fatal("expected Reassemble to fail due to wrong key, but it succeeded")
	}
}
