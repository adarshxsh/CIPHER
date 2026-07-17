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

	enc := crypto.NewXChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)

	eng := NewContentEngine(config, enc, dig, store, store, keys)

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

func TestContentEngine_BufferPooling(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-pooling-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{
		ChunkSize: 1024, // 1KB
	}

	enc := crypto.NewXChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()
	store := storage.NewFSStore(tmpDir)

	eng := NewContentEngine(config, enc, dig, store, store, keys)

	// Create a large payload to span many chunks (e.g., 50 chunks)
	originalData := make([]byte, 50*1024)
	rand.Read(originalData)

	ctx := context.Background()
	reader := bytes.NewReader(originalData)
	_, err = eng.Ingest(ctx, reader, manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	// Verify that the pools have at least 1 buffer and haven't bloated unboundedly
	eng.plainPool.mu.Lock()
	plainPoolLen := len(eng.plainPool.buffers)
	eng.plainPool.mu.Unlock()

	eng.cipherPool.mu.Lock()
	cipherPoolLen := len(eng.cipherPool.buffers)
	eng.cipherPool.mu.Unlock()

	// Since it's an unbuffered channel between chunker and ingester, 
	// plainPool may have at most 2 buffers (one in channel, one being read).
	if plainPoolLen == 0 || plainPoolLen > 2 {
		t.Errorf("expected plainPool length to be 1 or 2, got %d", plainPoolLen)
	}

	// cipherPool should only have 1 buffer since encryption and storage are sequential in Ingest
	if cipherPoolLen != 1 {
		t.Errorf("expected cipherPool length to be 1, got %d", cipherPoolLen)
	}
}
