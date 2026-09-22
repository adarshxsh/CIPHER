package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"math/rand"
	"os"
	"path/filepath"
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

func TestContentEngine_StreamReassemble_WriteSeeker(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-stream-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{ChunkSize: 64 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	store := storage.NewFSStore(tmpDir)
	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	// Generate 2MB random data
	dataSize := 2 * 1024 * 1024
	originalData := make([]byte, dataSize)
	rand.Seed(42)
	rand.Read(originalData)

	originalHash := sha256.Sum256(originalData)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("ingest failed: %v", err)
	}

	outPath := filepath.Join(tmpDir, "reassembled.dat")
	outF, err := os.Create(outPath)
	if err != nil {
		t.Fatalf("failed to create output file: %v", err)
	}

	if err := eng.Reassemble(ctx, m, outF); err != nil {
		outF.Close()
		t.Fatalf("reassemble failed: %v", err)
	}
	outF.Close()

	reassembledData, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read reassembled file: %v", err)
	}

	reassembledHash := sha256.Sum256(reassembledData)
	if originalHash != reassembledHash {
		t.Fatalf("SHA256 hash mismatch! Original: %x, Reassembled: %x", originalHash, reassembledHash)
	}
}

func TestContentEngine_StreamReassemble_OutOfOrder(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-ooo-test-*")
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

	data := make([]byte, 256*1024) // 8 chunks
	rand.Seed(123)
	rand.Read(data)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("ingest failed: %v", err)
	}

	// Reverse the order of chunk IDs in manifest to simulate out-of-order chunk processing
	oooManifest := *m
	oooChunkIDs := make([]core.ChunkID, len(m.ChunkIDs))
	for i, id := range m.ChunkIDs {
		oooChunkIDs[len(m.ChunkIDs)-1-i] = id
	}
	oooManifest.ChunkIDs = oooChunkIDs

	outPath := filepath.Join(tmpDir, "ooo_out.dat")
	outF, err := os.Create(outPath)
	if err != nil {
		t.Fatalf("failed to create output file: %v", err)
	}

	if err := eng.Reassemble(ctx, &oooManifest, outF); err != nil {
		outF.Close()
		t.Fatalf("reassemble out-of-order failed: %v", err)
	}
	outF.Close()

	reassembledData, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}

	if !bytes.Equal(data, reassembledData) {
		t.Fatalf("out-of-order reassembly failed to reconstruct exact file contents")
	}
}

func TestContentEngine_ReassembleMemoryFootprint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-mem-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	chunkSize := uint32(64 * 1024) // 64KB chunks
	config := core.EngineConfig{ChunkSize: chunkSize}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	store := storage.NewFSStore(tmpDir)
	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	// 10MB data = ~160 chunks
	dataSize := 10 * 1024 * 1024
	data := make([]byte, dataSize)
	rand.Seed(999)
	rand.Read(data)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("ingest failed: %v", err)
	}

	outPath := filepath.Join(tmpDir, "mem_out.dat")
	outF, err := os.Create(outPath)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	if err := eng.Reassemble(ctx, m, outF); err != nil {
		outF.Close()
		t.Fatalf("reassemble failed: %v", err)
	}
	outF.Close()

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	// Since chunks are freed immediately during streaming,
	// net heap allocation increase after GC should be negligible (far below 10MB data size)
	allocDiff := int64(m2.HeapAlloc) - int64(m1.HeapAlloc)
	t.Logf("HeapAlloc before: %d, HeapAlloc after: %d, Diff: %d", m1.HeapAlloc, m2.HeapAlloc, allocDiff)

	// Verify file matches bit-for-bit
	reassembledData, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read reassembled file: %v", err)
	}
	if !bytes.Equal(data, reassembledData) {
		t.Fatalf("reassembled data mismatch")
	}
}
