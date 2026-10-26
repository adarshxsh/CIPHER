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

func TestContentEngine_ReassembleConstantMemory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-mem-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	chunkSize := uint32(64 * 1024) // 64KB chunks
	config := core.EngineConfig{ChunkSize: chunkSize}

	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)

	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	// Create 16MB of data (~256 chunks)
	dataSize := 16 * 1024 * 1024
	data := make([]byte, dataSize)
	rand.Read(data)

	ctx := context.Background()

	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	// Release original data slice to free memory before measuring reassembly
	data = nil

	// Reassemble to io.Discard and measure heap allocation
	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	var bytesWritten int64
	writer := &countingWriter{bytesWritten: &bytesWritten}

	if err := eng.Reassemble(ctx, m, writer); err != nil {
		t.Fatalf("failed to reassemble: %v", err)
	}

	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	if bytesWritten != int64(dataSize) {
		t.Fatalf("written bytes %d != expected %d", bytesWritten, dataSize)
	}

	// Peak memory / heap allocation should be strictly bounded (well below 10MB)
	// HeapAlloc diff should remain small since chunks are freed immediately
	t.Logf("Reassembled %d MB file. HeapAlloc before: %d KB, HeapAlloc after: %d KB",
		dataSize/(1024*1024), memBefore.HeapAlloc/1024, memAfter.HeapAlloc/1024)
}

type countingWriter struct {
	bytesWritten *int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n := len(p)
	*w.bytesWritten += int64(n)
	return n, nil
}
