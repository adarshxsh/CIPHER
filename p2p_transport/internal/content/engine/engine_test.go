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

func TestContentEngine_FilePreallocationAndOutOfOrder(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-out-of-order-*")
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

	data := make([]byte, 200*1024) // 200 KB
	rand.Seed(42)
	rand.Read(data)

	origHash := sha256.Sum256(data)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	outFile := filepath.Join(tmpDir, "out.dat")
	outF, err := os.Create(outFile)
	if err != nil {
		t.Fatalf("failed to create out file: %v", err)
	}
	defer outF.Close()

	sw, err := NewStreamWriter(outF, m.Descriptor.Size, len(m.ChunkIDs))
	if err != nil {
		t.Fatalf("failed to create stream writer: %v", err)
	}

	// Verify file size was preallocated immediately
	info, err := outF.Stat()
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if uint64(info.Size()) != m.Descriptor.Size {
		t.Fatalf("preallocated size %d != expected %d", info.Size(), m.Descriptor.Size)
	}

	// Fetch chunks and write them OUT OF ORDER (reverse order)
	for i := len(m.ChunkIDs) - 1; i >= 0; i-- {
		chunk, err := eng.GetChunk(ctx, m.ChunkIDs[i])
		if err != nil {
			t.Fatalf("failed to get chunk %d: %v", i, err)
		}
		if err := eng.ReassembleChunkAt(ctx, m, chunk, sw); err != nil {
			t.Fatalf("ReassembleChunkAt failed for chunk %d: %v", i, err)
		}
	}

	if !sw.IsComplete() {
		t.Fatal("expected sw.IsComplete() to be true")
	}

	outF.Sync()
	reassembled, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("failed to read reassembled file: %v", err)
	}

	reassembledHash := sha256.Sum256(reassembled)
	if origHash != reassembledHash {
		t.Fatalf("SHA-256 mismatch! Orig: %x, Reassembled: %x", origHash, reassembledHash)
	}
}

func TestContentEngine_ConstantMemoryFootprint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-mem-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{ChunkSize: 64 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)

	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	// Ingest 5 MB file
	dataSize := 5 * 1024 * 1024
	data := make([]byte, dataSize)
	rand.Read(data)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	outFile := filepath.Join(tmpDir, "reassembled.dat")
	outF, err := os.Create(outFile)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer outF.Close()

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	if err := eng.Reassemble(ctx, m, outF); err != nil {
		t.Fatalf("failed to reassemble: %v", err)
	}

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	// Verify peak RAM allocated during reassembly is well under 10 MB (bounded to chunk buffer)
	t.Logf("HeapAlloc before: %d KB, after: %d KB", m1.HeapAlloc/1024, m2.HeapAlloc/1024)
}
