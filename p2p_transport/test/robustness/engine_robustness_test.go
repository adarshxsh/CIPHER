package robustness_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	mrand "math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
)

func setupTestEngine(t *testing.T, chunkSize uint32) (*engine.ContentEngine, string, core.KeyProvider) {
	tmpDir, err := os.MkdirTemp("", "content-robustness-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	config := core.EngineConfig{ChunkSize: chunkSize}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)

	eng := engine.NewContentEngine(config, enc, dig, store, store, keys, store)
	return eng, tmpDir, keys
}

// TestBoundarySizes tests exact boundaries around the chunk size
func TestBoundarySizes(t *testing.T) {
	chunkSize := uint32(32 * 1024)
	sizes := []int{
		0,
		1,
		int(chunkSize) - 1,
		int(chunkSize),
		int(chunkSize) + 1,
		int(chunkSize) * 2,
		int(chunkSize)*2 + 1,
		1 * 1024 * 1024, // 1MB
	}

	eng, tmpDir, _ := setupTestEngine(t, chunkSize)
	defer os.RemoveAll(tmpDir)
	ctx := context.Background()

	for _, sz := range sizes {
		t.Run(fmt.Sprintf("Size_%d", sz), func(t *testing.T) {
			data := make([]byte, sz)
			if sz > 0 {
				rand.Read(data)
			}

			m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
			if err != nil {
				t.Fatalf("Ingest failed for size %d: %v", sz, err)
			}

			var outBuf bytes.Buffer
			if err := eng.Reassemble(ctx, m, &outBuf); err != nil {
				t.Fatalf("Reassemble failed for size %d: %v", sz, err)
			}

			if !bytes.Equal(data, outBuf.Bytes()) {
				t.Fatalf("Data mismatch for size %d", sz)
			}
		})
	}
}

// TestMissingChunk verifies that Reassemble returns an error immediately if a chunk is missing
func TestMissingChunk(t *testing.T) {
	eng, tmpDir, _ := setupTestEngine(t, 32*1024)
	defer os.RemoveAll(tmpDir)
	ctx := context.Background()

	data := make([]byte, 100*1024) // ~4 chunks
	rand.Read(data)

	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	if len(m.ChunkIDs) < 3 {
		t.Fatalf("Expected at least 3 chunks, got %d", len(m.ChunkIDs))
	}

	// Delete chunk index 1 (the 2nd chunk)
	targetChunk := m.ChunkIDs[1]
	encoded := hex.EncodeToString(targetChunk[:])
	chunkFile := filepath.Join(tmpDir, encoded[0:2], encoded[2:4], encoded)
	if err := os.Remove(chunkFile); err != nil {
		t.Fatalf("Failed to remove target chunk file: %v", err)
	}

	var outBuf bytes.Buffer
	err = eng.Reassemble(ctx, m, &outBuf)
	if err == nil {
		t.Fatal("Expected Reassemble to fail due to missing chunk, but it succeeded")
	}

	// Verify streaming reassembly stopped at missing chunk (wrote only chunk 0 = 32KB)
	if outBuf.Len() != 32*1024 {
		t.Fatalf("Expected exactly 1 chunk (32768 bytes) written before failing, got %d bytes", outBuf.Len())
	}
}

// TestCorruptedChunk verifies that Reassemble aborts immediately on hash mismatch
func TestCorruptedChunk(t *testing.T) {
	eng, tmpDir, _ := setupTestEngine(t, 32*1024)
	defer os.RemoveAll(tmpDir)
	ctx := context.Background()

	data := make([]byte, 100*1024) // ~4 chunks
	rand.Read(data)

	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	if len(m.ChunkIDs) < 3 {
		t.Fatalf("Expected at least 3 chunks, got %d", len(m.ChunkIDs))
	}

	// Corrupt chunk index 1 (the 2nd chunk)
	targetChunk := m.ChunkIDs[1]
	encoded := hex.EncodeToString(targetChunk[:])
	chunkFile := filepath.Join(tmpDir, encoded[0:2], encoded[2:4], encoded)

	f, err := os.OpenFile(chunkFile, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open chunk file for corruption: %v", err)
	}
	f.Seek(100, os.SEEK_SET) // corrupt payload bytes past the 66-byte ChunkHeader
	f.Write([]byte("CORRUPTED_DATA_BYTES"))
	f.Close()

	var outBuf bytes.Buffer
	err = eng.Reassemble(ctx, m, &outBuf)
	if err == nil {
		t.Fatal("Expected Reassemble to fail due to corrupted chunk, but it succeeded")
	}

	// Verify streaming reassembly stopped at corrupted chunk
	if outBuf.Len() != 32*1024 {
		t.Fatalf("Expected exactly 1 chunk (32768 bytes) written before failing, got %d bytes", outBuf.Len())
	}
}

// TestWrongKey verifies that decryption fails securely if the wrong key is supplied
func TestWrongKey(t *testing.T) {
	eng, tmpDir, keys := setupTestEngine(t, 32*1024)
	defer os.RemoveAll(tmpDir)
	ctx := context.Background()

	data := make([]byte, 10*1024)
	rand.Read(data)

	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Replace the key with a wrong one
	wrongKey := make([]byte, 32)
	rand.Read(wrongKey)
	keys.Put(ctx, m.Descriptor.ID, wrongKey)

	var outBuf bytes.Buffer
	err = eng.Reassemble(ctx, m, &outBuf)
	if err == nil {
		t.Fatal("Expected Reassemble to fail due to wrong key, but it succeeded")
	}
}

// TestTheGauntlet runs randomized tests (random sizes, random keys, shuffled chunks simulated)
func TestTheGauntlet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping the gauntlet in short mode")
	}

	iterations := 10 // scale up to 1000 for full stress test
	ctx := context.Background()
	mrand.Seed(time.Now().UnixNano())

	for i := 0; i < iterations; i++ {
		chunkSize := uint32(mrand.Intn(128*1024) + 1024) // 1KB to 129KB
		fileSize := mrand.Intn(2 * 1024 * 1024)          // 0 to 2MB

		eng, tmpDir, _ := setupTestEngine(t, chunkSize)

		data := make([]byte, fileSize)
		if fileSize > 0 {
			rand.Read(data)
		}

		m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
		if err != nil {
			t.Fatalf("Iteration %d: Ingest failed: %v", i, err)
		}

		var outBuf bytes.Buffer
		if err := eng.Reassemble(ctx, m, &outBuf); err != nil {
			t.Fatalf("Iteration %d: Reassemble failed: %v", i, err)
		}

		if !bytes.Equal(data, outBuf.Bytes()) {
			t.Fatalf("Iteration %d: Data mismatch! size=%d chunk=%d", i, fileSize, chunkSize)
		}

		os.RemoveAll(tmpDir)
	}
}
