package engine

import (
	"bytes"
	"context"
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

type failWriter struct{}

func (f *failWriter) Write(p []byte) (int, error) {
	return 0, os.ErrPermission
}

func TestContentEngine_Reassemble_WriteError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{ChunkSize: 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()
	store := storage.NewFSStore(tmpDir)
	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	data := make([]byte, 2048)
	rand.Read(data)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	fw := &failWriter{}
	if err := eng.Reassemble(ctx, m, fw); err == nil {
		t.Fatal("expected error on failing writer, got nil")
	}
}

func TestContentEngine_Reassemble_ConstantMemory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-memtest-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 64 KB chunks, ingest 10 MB total
	chunkSize := uint32(64 * 1024)
	config := core.EngineConfig{ChunkSize: chunkSize}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()
	store := storage.NewFSStore(tmpDir)
	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	chunkCount := 100
	totalSize := int(chunkSize) * chunkCount
	data := make([]byte, totalSize)
	rand.Read(data)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	// Reassemble directly to io.Discard (unseekable writer)
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	if err := eng.Reassemble(ctx, m, io.Discard); err != nil {
		t.Fatalf("failed to reassemble to io.Discard: %v", err)
	}

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	// Peak heap alloc increase should be minimal (< 5 MB)
	if allocDiff := int64(m2.HeapAlloc) - int64(m1.HeapAlloc); allocDiff > 5*1024*1024 {
		t.Errorf("memory growth during reassembly exceeded limit: %d bytes", allocDiff)
	}

	// Verify correctness
	var outBuf bytes.Buffer
	if err := eng.Reassemble(ctx, m, &outBuf); err != nil {
		t.Fatalf("failed to reassemble: %v", err)
	}

	if !bytes.Equal(data, outBuf.Bytes()) {
		t.Fatal("reassembled data mismatch")
	}
}

