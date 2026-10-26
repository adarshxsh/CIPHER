package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
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

func TestContentEngine_ReassembleMemoryBound(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-mem-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{ChunkSize: 256 * 1024} // 256KB chunks
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)
	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	// Create 10MB of data (~40 chunks)
	dataSize := 10 * 1024 * 1024
	data := make([]byte, dataSize)
	rand.Read(data)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Reassemble to io.Discard while measuring heap allocation
	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	if err := eng.Reassemble(ctx, m, io.Discard); err != nil {
		t.Fatalf("Reassemble failed: %v", err)
	}

	runtime.GC()
	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)

	// Check net retained heap memory growth is flat
	heapDiff := int64(msAfter.HeapAlloc) - int64(msBefore.HeapAlloc)
	// Heap growth after GC should be well under 2MB (2 * 1024 * 1024 bytes)
	if heapDiff > 2*1024*1024 {
		t.Errorf("heap memory grew by %d bytes, expected < 2MB", heapDiff)
	}
}

type errWriter struct {
	failAfterBytes int
	written        int
}

func (w *errWriter) Write(p []byte) (int, error) {
	if w.written+len(p) > w.failAfterBytes {
		return 0, fmt.Errorf("write error simulated")
	}
	w.written += len(p)
	return len(p), nil
}

func TestContentEngine_ReassembleWriteErrorPropagation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-write-err-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	chunkSize := uint32(32 * 1024)
	config := core.EngineConfig{ChunkSize: chunkSize}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)
	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	// Create 100KB data (3 chunks)
	data := make([]byte, 100*1024)
	rand.Read(data)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Writer fails after 1 chunk
	ew := &errWriter{failAfterBytes: int(chunkSize)}
	err = eng.Reassemble(ctx, m, ew)
	if err == nil {
		t.Fatal("Expected Reassemble to fail when write fails, but succeeded")
	}
	if ew.written != int(chunkSize) {
		t.Errorf("Expected writer to have received exactly 1 chunk (%d bytes), got %d bytes", chunkSize, ew.written)
	}
}

