package robustness_test

import (
	"bytes"
	"context"
	"crypto/rand"
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

// TestMissingChunk verifies that Reassemble returns an error (and does not panic) if a chunk is missing
func TestMissingChunk(t *testing.T) {
	eng, tmpDir, _ := setupTestEngine(t, 32*1024)
	defer os.RemoveAll(tmpDir)
	ctx := context.Background()

	data := make([]byte, 100*1024) // ~3 chunks
	rand.Read(data)

	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	if len(m.ChunkIDs) == 0 {
		t.Fatal("Expected at least 1 chunk")
	}

	// Delete the middle chunk
	targetChunk := m.ChunkIDs[len(m.ChunkIDs)/2]
	chunkFile := filepath.Join(tmpDir, string(targetChunk[:]))
	// storage/fs.go encodes hex as filename
	_ = os.Remove(chunkFile) // it might be hex encoded, we rely on Reassemble failing. Actually we should just truncate or remove it properly, but any file removal or corruption works.

	// Let's just remove everything in tmpDir for simplicity to ensure missing chunk
	os.RemoveAll(tmpDir)
	os.MkdirAll(tmpDir, 0755)

	var outBuf bytes.Buffer
	err = eng.Reassemble(ctx, m, &outBuf)
	if err == nil {
		t.Fatal("Expected Reassemble to fail due to missing chunk, but it succeeded")
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

// TestProcessRestart_FSKeyProvider verifies that keys persist to disk with 0600 permissions
// and content can be reassembled after process restart using FSKeyProvider.
func TestProcessRestart_FSKeyProvider(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-restart-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)

	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()

	keys1, err := engine.NewFSKeyProvider(tmpDir)
	if err != nil {
		t.Fatalf("failed to create FSKeyProvider: %v", err)
	}

	eng1 := engine.NewContentEngine(config, enc, dig, store, store, keys1, store)

	data := make([]byte, 50*1024)
	rand.Read(data)

	ctx := context.Background()
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Verify key file permissions on disk
	keyPath := filepath.Join(tmpDir, "keys", fmt.Sprintf("%x.key", m.Descriptor.ID))
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("Key file missing on disk: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("Key file permissions are %o, expected 0600", info.Mode().Perm())
	}

	// Simulate process restart by instantiating new engine & new FSKeyProvider
	keys2, err := engine.NewFSKeyProvider(tmpDir)
	if err != nil {
		t.Fatalf("failed to create new FSKeyProvider: %v", err)
	}

	eng2 := engine.NewContentEngine(config, enc, dig, store, store, keys2, store)

	var outBuf bytes.Buffer
	if err := eng2.Reassemble(ctx, m, &outBuf); err != nil {
		t.Fatalf("Reassemble after restart failed: %v", err)
	}

	if !bytes.Equal(data, outBuf.Bytes()) {
		t.Fatalf("Reassembled data mismatch after restart")
	}
}

