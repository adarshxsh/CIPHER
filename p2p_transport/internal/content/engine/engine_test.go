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

func TestContentEngine_RestartPersistenceWithFSKeyProvider(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "restart-engine-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{
		ChunkSize: 32 * 1024,
	}

	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()

	fsKeys1, err := NewFSKeyProvider(tmpDir)
	if err != nil {
		t.Fatalf("failed to create FSKeyProvider: %v", err)
	}

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store1 := storage.NewFSStore(tmpDir)

	eng1 := NewContentEngine(config, enc, dig, store1, store1, fsKeys1, store1)

	originalData := []byte("Hello, persistent keystore cross-restart test content!")
	ctx := context.Background()

	m, err := eng1.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	// Simulate node/daemon restart: create new key provider and content engine from same store directory
	fsKeys2, err := NewFSKeyProvider(tmpDir)
	if err != nil {
		t.Fatalf("failed to reload FSKeyProvider: %v", err)
	}
	store2 := storage.NewFSStore(tmpDir)
	eng2 := NewContentEngine(config, enc, dig, store2, store2, fsKeys2, store2)

	var reassembled bytes.Buffer
	if err := eng2.Reassemble(ctx, m, &reassembled); err != nil {
		t.Fatalf("failed to reassemble after engine restart: %v", err)
	}

	if !bytes.Equal(originalData, reassembled.Bytes()) {
		t.Errorf("reassembled data mismatch after process restart")
	}
}
