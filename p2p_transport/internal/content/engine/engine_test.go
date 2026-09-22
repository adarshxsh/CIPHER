package engine

import (
	"bytes"
	"context"
	crand "crypto/rand"
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

func TestContentEngine_SequentialReassembleMemory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-seq-*")
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

	// Create multi-chunk dataset (1.6 MB -> 50 chunks)
	dataSize := 50 * 32 * 1024
	originalData := make([]byte, dataSize)
	crand.Read(originalData)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	if len(m.ChunkIDs) != 50 {
		t.Fatalf("expected 50 chunks, got %d", len(m.ChunkIDs))
	}

	var outBuf bytes.Buffer
	if err := eng.Reassemble(ctx, m, &outBuf); err != nil {
		t.Fatalf("failed to reassemble: %v", err)
	}

	if !bytes.Equal(originalData, outBuf.Bytes()) {
		t.Fatalf("reassembled data does not match original")
	}
}

func TestContentEngine_CorruptedChunkHalt(t *testing.T) {
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

	// Ingest 5 chunks of data (160 KB)
	chunkSize := 32 * 1024
	originalData := make([]byte, 5*chunkSize)
	crand.Read(originalData)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	// Corrupt chunk 2 (third chunk, index 2) in storage by replacing its stored data
	corruptChunkID := m.ChunkIDs[2]
	corruptChunk, err := store.GetChunk(ctx, corruptChunkID)
	if err != nil {
		t.Fatalf("failed to get chunk to corrupt: %v", err)
	}
	// Tamper with data payload so digest sum fails
	corruptChunk.Data[0] ^= 0xFF
	if err := store.PutChunk(ctx, corruptChunk); err != nil {
		t.Fatalf("failed to overwrite corrupted chunk: %v", err)
	}

	var outBuf bytes.Buffer
	err = eng.Reassemble(ctx, m, &outBuf)
	if err == nil {
		t.Fatal("expected Reassemble to return error for corrupted chunk, but got nil")
	}

	// Output buffer should only contain the first 2 chunks (64 KB), not 3 or 5 chunks
	expectedWrittenBytes := 2 * chunkSize
	if outBuf.Len() != expectedWrittenBytes {
		t.Fatalf("expected output buffer length to be %d bytes (2 chunks written before corruption halt), got %d bytes", expectedWrittenBytes, outBuf.Len())
	}

	if !bytes.Equal(outBuf.Bytes(), originalData[:expectedWrittenBytes]) {
		t.Fatalf("written data prior to corruption halt does not match original prefix")
	}
}
