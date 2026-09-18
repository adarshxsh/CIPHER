package engine

import (
	"bytes"
	"context"
	"errors"
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

type mockStreamingSource struct {
	chunk *core.Chunk
}

func (m *mockStreamingSource) HasChunk(ctx context.Context, id core.ChunkID) (bool, error) {
	return true, nil
}

func (m *mockStreamingSource) GetChunk(ctx context.Context, id core.ChunkID) (*core.Chunk, error) {
	dataCopy := make([]byte, len(m.chunk.Data))
	copy(dataCopy, m.chunk.Data)
	return &core.Chunk{
		Header: m.chunk.Header,
		Data:   dataCopy,
	}, nil
}

func TestContentEngine_ReassembleConstantMemory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-mem-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{ChunkSize: 256 * 1024} // 256KB
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	store := storage.NewFSStore(tmpDir)
	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	// Ingest 1 chunk to setup valid encrypted storage and key
	chunkData := make([]byte, 256*1024)
	rand.Read(chunkData)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(chunkData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	singleChunkID := m.ChunkIDs[0]
	storedChunk, err := store.GetChunk(ctx, singleChunkID)
	if err != nil {
		t.Fatalf("failed to get stored chunk: %v", err)
	}

	// Construct manifest representing a 2.5GB file (10,000 chunks x 256KB)
	const numChunks = 10000
	largeChunkIDs := make([]core.ChunkID, numChunks)
	for i := 0; i < numChunks; i++ {
		largeChunkIDs[i] = singleChunkID
	}

	largeManifest := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   m.Descriptor.ID,
			Type: manifest.TypeFile,
			Size: uint64(numChunks) * 256 * 1024,
		},
		ChunkIDs: largeChunkIDs,
	}

	mockSource := &mockStreamingSource{chunk: storedChunk}
	engStream := NewContentEngine(config, enc, dig, mockSource, store, keys, store)

	runtime.GC()

	if err := engStream.Reassemble(ctx, largeManifest, io.Discard); err != nil {
		t.Fatalf("Reassemble failed: %v", err)
	}

	runtime.GC()
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Heap allocation should remain far below 64MB (e.g. < 20MB)
	const maxAllowedHeap = 64 * 1024 * 1024
	if memStats.Alloc > maxAllowedHeap {
		t.Fatalf("Heap allocation %d bytes exceeded max limit of %d bytes", memStats.Alloc, maxAllowedHeap)
	}
}

type syncTrackingWriter struct {
	synced bool
	errOnWrite bool
}

func (w *syncTrackingWriter) Write(p []byte) (int, error) {
	if w.errOnWrite {
		return 0, errors.New("disk write failure")
	}
	return len(p), nil
}

func (w *syncTrackingWriter) Sync() error {
	w.synced = true
	return nil
}

func TestContentEngine_ReassembleSyncAndFlush(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-sync-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	store := storage.NewFSStore(tmpDir)
	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader([]byte("test data for sync")), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	writer := &syncTrackingWriter{}
	if err := eng.Reassemble(ctx, m, writer); err != nil {
		t.Fatalf("Reassemble failed: %v", err)
	}

	if !writer.synced {
		t.Errorf("expected Sync() to be called on writer after reassembly")
	}
}

func TestContentEngine_ReassembleWriteError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-err-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	store := storage.NewFSStore(tmpDir)
	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader([]byte("test write error")), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	writer := &syncTrackingWriter{errOnWrite: true}
	err = eng.Reassemble(ctx, m, writer)
	if err == nil {
		t.Fatal("expected error on write failure, got nil")
	}
}

