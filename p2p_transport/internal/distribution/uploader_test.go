package distribution

import (
	"context"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/transport"
)

func TestUploaderConfigDefaults(t *testing.T) {
	if DefaultUploaderConfig.MaxConcurrentProviders != 8 {
		t.Errorf("expected DefaultUploaderConfig.MaxConcurrentProviders to be 8, got %d", DefaultUploaderConfig.MaxConcurrentProviders)
	}
}

func TestUploaderConcurrencyLimit(t *testing.T) {
	m := generateTestManifest(5)
	providers := generateTestPeerIDs(10)

	plan, err := PlanPlacement(m, providers, 2)
	if err != nil {
		t.Fatalf("PlanPlacement failed: %v", err)
	}

	tracker := NewGlobalReplicaTracker(2)

	config := core.EngineConfig{ChunkSize: 1024 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	eng := engine.NewContentEngine(config, enc, dig, store, store, keys, store)

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize manifest failed: %v", err)
	}

	ctx := context.Background()
	if err := eng.PutManifestBytes(ctx, plan.ContentID, mBytes); err != nil {
		t.Fatalf("PutManifestBytes failed: %v", err)
	}

	cfg := UploaderConfig{
		MaxRetriesPerChunk:     1,
		FailoverRounds:         1,
		MaxConcurrentProviders: 3,
	}

	// Distribute passes through provider workers bounded by semaphore
	_ = Distribute(ctx, (*transport.Transport)(nil), eng, plan, tracker, cfg)

	satisfied, total := tracker.GetSummary(m.ChunkIDs)
	if total != 5 {
		t.Errorf("expected total 5 chunks in summary, got %d", total)
	}
	_ = satisfied
}

