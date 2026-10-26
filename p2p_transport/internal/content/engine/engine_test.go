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

type trackingWriter struct {
	writes [][]byte
}

func (tw *trackingWriter) Write(p []byte) (n int, err error) {
	cp := make([]byte, len(p))
	copy(cp, p)
	tw.writes = append(tw.writes, cp)
	return len(p), nil
}

func TestContentEngine_Reassemble_SequentialStreaming(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-stream-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{ChunkSize: 10 * 1024} // 10KB chunks
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)
	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	originalData := make([]byte, 35*1024) // 4 chunks (10k, 10k, 10k, 5k)
	rand.Read(originalData)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("ingest failed: %v", err)
	}

	tw := &trackingWriter{}
	if err := eng.Reassemble(ctx, m, tw); err != nil {
		t.Fatalf("reassemble failed: %v", err)
	}

	if len(tw.writes) != len(m.ChunkIDs) {
		t.Fatalf("expected %d sequential writes, got %d", len(m.ChunkIDs), len(tw.writes))
	}

	var assembled []byte
	for _, w := range tw.writes {
		assembled = append(assembled, w...)
	}

	if !bytes.Equal(originalData, assembled) {
		t.Fatal("assembled data mismatch")
	}
}

func TestContentEngine_Reassemble_CorruptedChunkStopsOutput(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-corrupt-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{ChunkSize: 10 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)
	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	originalData := make([]byte, 30*1024) // 3 chunks
	rand.Read(originalData)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("ingest failed: %v", err)
	}

	// Corrupt second chunk in store
	corruptID := m.ChunkIDs[1]
	chunk, err := store.GetChunk(ctx, corruptID)
	if err != nil {
		t.Fatalf("failed to get chunk to corrupt: %v", err)
	}
	chunk.Data[len(chunk.Data)-1] ^= 0xFF
	if err := store.PutChunk(ctx, chunk); err != nil {
		t.Fatalf("failed to put corrupted chunk: %v", err)
	}

	tw := &trackingWriter{}
	err = eng.Reassemble(ctx, m, tw)
	if err == nil {
		t.Fatal("expected error reassembling corrupted chunk, got nil")
	}

	// Only 1 chunk should have been written before the second corrupted chunk failed verification
	if len(tw.writes) != 1 {
		t.Fatalf("expected exactly 1 chunk written before failure, got %d", len(tw.writes))
	}
}
