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

func TestContentEngine_StreamingDiskWriter(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-writer-test-*")
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

	originalData := make([]byte, 256*1024+123) // ~256 KB
	rand.Seed(time.Now().UnixNano())
	rand.Read(originalData)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	outPath := tmpDir + "/reassembled.dat"
	outF, err := os.Create(outPath)
	if err != nil {
		t.Fatalf("failed to create output file: %v", err)
	}

	sdw := NewStreamingDiskWriter(outF, 64*1024)
	if err := eng.Reassemble(ctx, m, sdw); err != nil {
		sdw.Close()
		t.Fatalf("failed to reassemble via StreamingDiskWriter: %v", err)
	}
	if err := sdw.Close(); err != nil {
		t.Fatalf("failed to close StreamingDiskWriter: %v", err)
	}

	reassembledData, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read reassembled file: %v", err)
	}

	if !bytes.Equal(originalData, reassembledData) {
		t.Errorf("reassembled file content does not match original source")
	}
}

func TestContentEngine_ReassembleChunkAt_SparseWriter(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-sparse-test-*")
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

	originalData := make([]byte, 128*1024) // 4 chunks of 32KB
	rand.Seed(12345)
	rand.Read(originalData)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	outPath := tmpDir + "/sparse_reassembled.dat"
	outF, err := os.Create(outPath)
	if err != nil {
		t.Fatalf("failed to create output file: %v", err)
	}
	defer outF.Close()

	// Write chunks out of order: chunk 3, chunk 1, chunk 0, chunk 2
	order := []int{3, 1, 0, 2}
	for _, idx := range order {
		chunkID := m.ChunkIDs[idx]
		chunk, err := eng.GetChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("failed to get chunk %d: %v", idx, err)
		}
		if err := eng.ReassembleChunkAt(ctx, m.Descriptor.ID, chunk, outF); err != nil {
			t.Fatalf("failed to write sparse chunk %d: %v", idx, err)
		}
	}

	if err := outF.Sync(); err != nil {
		t.Fatalf("failed to sync output file: %v", err)
	}

	reassembledData, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read sparse reassembled file: %v", err)
	}

	if !bytes.Equal(originalData, reassembledData) {
		t.Errorf("sparse reassembled file content does not match original source")
	}
}

func TestContentEngine_StreamingReassembleMemoryProfile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-mem-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Small chunk size to produce many chunks (e.g. 500 chunks)
	config := core.EngineConfig{
		ChunkSize: 4 * 1024, // 4KB
	}

	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)

	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	chunkCount := 500
	originalData := make([]byte, chunkCount*4096)
	rand.Seed(999)
	rand.Read(originalData)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	outPath := tmpDir + "/mem_profile_out.dat"
	outF, err := os.Create(outPath)
	if err != nil {
		t.Fatalf("failed to create output file: %v", err)
	}

	// Force GC prior to reassembly measurement
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	if err := eng.Reassemble(ctx, m, outF); err != nil {
		outF.Close()
		t.Fatalf("failed to reassemble: %v", err)
	}
	outF.Close()

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	heapAllocDiff := int64(memAfter.HeapAlloc) - int64(memBefore.HeapAlloc)
	t.Logf("HeapAlloc before: %d bytes, after: %d bytes, diff: %d bytes", memBefore.HeapAlloc, memAfter.HeapAlloc, heapAllocDiff)

	// Memory usage during streaming reassembly must be flat and under 5MB (far below 50MB limit)
	const maxMemoryOverhead = 5 * 1024 * 1024
	if heapAllocDiff > maxMemoryOverhead {
		t.Errorf("Heap allocation diff %d bytes exceeded maximum allowed overhead %d bytes", heapAllocDiff, maxMemoryOverhead)
	}

	reassembledData, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read reassembled output file: %v", err)
	}
	if !bytes.Equal(originalData, reassembledData) {
		t.Errorf("reassembled file content does not match original source hash/bytes")
	}
}
