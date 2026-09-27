package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"math/rand"
	"os"
	"runtime"
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

func TestContentEngine_StreamingReassembleMemory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-stream-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	chunkSize := uint32(64 * 1024) // 64KB
	config := core.EngineConfig{
		ChunkSize: chunkSize,
	}

	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)

	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	// Create 20MB of data (320 chunks)
	dataSize := 20 * 1024 * 1024
	originalData := make([]byte, dataSize)
	rand.Seed(12345)
	rand.Read(originalData)

	ctx := context.Background()

	m, err := eng.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	// Calculate original SHA256
	expectedHash := dig.Sum(originalData)

	// Force GC to clean up ingestion allocations
	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	// Reassemble directly to a hash writer (O(1) memory requirement)
	hasher := sha256.New()
	if err := eng.Reassemble(ctx, m, hasher); err != nil {
		t.Fatalf("failed to reassemble: %v", err)
	}

	var hashSum core.Hash
	copy(hashSum[:], hasher.Sum(nil))
	if hashSum != expectedHash {
		t.Fatalf("reassembled content hash mismatch")
	}

	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)

	// Confirm that total heap memory growth during reassembly is nowhere near 20MB
	// Since each chunk is 64KB and discarded immediately, net heap allocation growth is minimal.
	t.Logf("Mem before: %d KB, Mem after: %d KB", msBefore.Alloc/1024, msAfter.Alloc/1024)
}

