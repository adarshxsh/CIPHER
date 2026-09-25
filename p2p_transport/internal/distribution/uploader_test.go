package distribution

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"testing"
	"time"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/transport"
)

func TestDefaultUploaderConfig(t *testing.T) {
	if DefaultUploaderConfig.MaxConcurrentProviders != 10 {
		t.Errorf("expected DefaultUploaderConfig.MaxConcurrentProviders to be 10, got %d",
			DefaultUploaderConfig.MaxConcurrentProviders)
	}
}

func setupTestEngine(t *testing.T) (*engine.ContentEngine, string, *manifest.Manifest) {
	tmpDir, err := os.MkdirTemp("", "uploader-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	config := core.EngineConfig{ChunkSize: 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)

	eng := engine.NewContentEngine(config, enc, dig, store, store, keys, store)

	// Create test file and store content
	testData := make([]byte, 4096)
	_, _ = rand.Read(testData)

	ctx := context.Background()
	m, err := eng.Ingest(ctx, bytes.NewReader(testData), manifest.TypeFile)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to ingest test content: %v", err)
	}

	return eng, tmpDir, m
}

func TestDistributeConcurrencyLimit(t *testing.T) {
	eng, tmpDir, m := setupTestEngine(t)
	defer os.RemoveAll(tmpDir)

	providers := generateTestPeerIDs(8)
	replication := 2

	plan, err := PlanPlacement(m, providers, replication)
	if err != nil {
		t.Fatalf("PlanPlacement failed: %v", err)
	}

	tracker := NewGlobalReplicaTracker(replication)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	host, kdht, err := transport.NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create test transport host: %v", err)
	}
	defer host.Close()
	if kdht != nil {
		defer kdht.Close()
	}

	tp := transport.NewTransport(host)

	cfg := UploaderConfig{
		MaxRetriesPerChunk:     1,
		FailoverRounds:         1,
		MaxConcurrentProviders: 2,
	}

	// Distribute will complete Phase 1 and Phase 2 (failing network calls to test peers), returning invariant violation error.
	_ = Distribute(ctx, tp, eng, plan, tracker, cfg)

	// Verify tracker state recorded failures cleanly without hanging
	for _, cid := range m.ChunkIDs {
		for _, p := range providers {
			tracker.mu.RLock()
			status := tracker.state[cid][p]
			tracker.mu.RUnlock()
			if status == ReplicaUploading {
				t.Errorf("chunk %x on peer %s stuck in ReplicaUploading status", cid, p)
			}
		}
	}
}

func TestDistributeContextCancellationWithSemaphore(t *testing.T) {
	eng, tmpDir, m := setupTestEngine(t)
	defer os.RemoveAll(tmpDir)

	providers := generateTestPeerIDs(10)
	plan, err := PlanPlacement(m, providers, 2)
	if err != nil {
		t.Fatalf("PlanPlacement failed: %v", err)
	}

	tracker := NewGlobalReplicaTracker(2)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel context immediately to test semaphore select case <-ctx.Done()
	cancel()

	host, _, err := transport.NewNode(context.Background(), 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create host: %v", err)
	}
	defer host.Close()

	tp := transport.NewTransport(host)

	cfg := UploaderConfig{
		MaxRetriesPerChunk:     1,
		FailoverRounds:         1,
		MaxConcurrentProviders: 2,
	}

	err = Distribute(ctx, tp, eng, plan, tracker, cfg)
	if err == nil {
		t.Errorf("expected error on canceled context, got nil")
	}
}
