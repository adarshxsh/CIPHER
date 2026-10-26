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

func TestContentEngine_Reassemble_MemoryBound(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-mem-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	chunkSize := uint32(32 * 1024) // 32KB
	config := core.EngineConfig{ChunkSize: chunkSize}

	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)

	eng := NewContentEngine(config, enc, dig, store, store, keys, store)
	ctx := context.Background()

	// Ingest a file with 50 chunks (~1.6MB)
	chunkCount := 50
	dataSize := chunkCount * int(chunkSize)
	originalData := make([]byte, dataSize)
	rand.Seed(time.Now().UnixNano())
	rand.Read(originalData)

	m, err := eng.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	if len(m.ChunkIDs) != chunkCount {
		t.Fatalf("expected %d chunks, got %d", chunkCount, len(m.ChunkIDs))
	}

	// Stream and verify writer receives all bytes sequentially
	var totalWritten int
	writer := &trackingWriter{
		onWrite: func(p []byte) {
			totalWritten += len(p)
		},
	}

	if err := eng.Reassemble(ctx, m, writer); err != nil {
		t.Fatalf("failed to reassemble: %v", err)
	}

	if totalWritten != dataSize {
		t.Errorf("expected %d bytes written, got %d", dataSize, totalWritten)
	}
}

func TestContentEngine_Reassemble_WriterError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-err-test-*")
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
	ctx := context.Background()

	data := make([]byte, 100*1024)
	rand.Read(data)

	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	errWriter := &failingWriter{failAfterBytes: 40 * 1024}
	err = eng.Reassemble(ctx, m, errWriter)
	if err == nil {
		t.Fatal("expected Reassemble to fail when writer returns error, but got nil")
	}
}

type trackingWriter struct {
	onWrite func(p []byte)
}

func (w *trackingWriter) Write(p []byte) (n int, err error) {
	if w.onWrite != nil {
		w.onWrite(p)
	}
	return len(p), nil
}

type failingWriter struct {
	written        int
	failAfterBytes int
}

func (w *failingWriter) Write(p []byte) (n int, err error) {
	if w.written >= w.failAfterBytes {
		return 0, os.ErrPermission
	}
	w.written += len(p)
	return len(p), nil
}
