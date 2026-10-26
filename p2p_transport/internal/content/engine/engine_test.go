package engine

import (
	"bytes"
	"context"
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

func TestContentEngine_Reassemble_Streaming(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-stream-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{
		ChunkSize: 64 * 1024, // 64KB chunks
	}

	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)

	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	// Create 5 MB of data (~80 chunks)
	dataSize := 5 * 1024 * 1024
	originalData := make([]byte, dataSize)
	rand.Read(originalData)

	ctx := context.Background()

	m, err := eng.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	// Reassemble to counting writer
	var bytesWritten int64
	cw := &countingWriter{onWrite: func(n int) { bytesWritten += int64(n) }}

	runtime.GC()
	var mBefore runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	if err := eng.Reassemble(ctx, m, cw); err != nil {
		t.Fatalf("failed to reassemble stream: %v", err)
	}

	var mAfter runtime.MemStats
	runtime.ReadMemStats(&mAfter)

	if bytesWritten != int64(dataSize) {
		t.Errorf("written bytes %d != expected %d", bytesWritten, dataSize)
	}

	heapAlloc := int64(mAfter.Alloc) - int64(mBefore.Alloc)
	t.Logf("Reassemble heap allocation delta: %d bytes for %d bytes content", heapAlloc, dataSize)
}

type countingWriter struct {
	onWrite func(n int)
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.onWrite(len(p))
	return len(p), nil
}

