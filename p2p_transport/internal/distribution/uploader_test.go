package distribution_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/distribution"
	"cipher/internal/protocol/push"
	"cipher/internal/transport"
)

func createTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func TestUploaderConfigDefaults(t *testing.T) {
	cfg := distribution.DefaultUploaderConfig
	if cfg.MaxConcurrentProviders != 8 {
		t.Errorf("expected default MaxConcurrentProviders to be 8, got %d", cfg.MaxConcurrentProviders)
	}
	if cfg.MaxRetriesPerChunk != 3 {
		t.Errorf("expected default MaxRetriesPerChunk to be 3, got %d", cfg.MaxRetriesPerChunk)
	}
	if cfg.FailoverRounds != 2 {
		t.Errorf("expected default FailoverRounds to be 2, got %d", cfg.FailoverRounds)
	}
}

func TestDistributeWithConcurrencyLimit(t *testing.T) {
	mn := mocknet.New()

	pubHost, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to create publisher host: %v", err)
	}

	const providerCount = 6
	providerHosts := make([]host.Host, providerCount)
	providerIDs := make([]peer.ID, providerCount)

	for i := 0; i < providerCount; i++ {
		h, err := mn.GenPeer()
		if err != nil {
			t.Fatalf("failed to create provider host %d: %v", i, err)
		}
		providerHosts[i] = h
		providerIDs[i] = h.ID()

		provEngine := createTestEngine(t)
		push.NewStreamHandler(h, provEngine, nil, true, push.AuthPolicyOpen, nil)
	}

	if err := mn.LinkAll(); err != nil {
		t.Fatalf("failed to link mocknet: %v", err)
	}
	if err := mn.ConnectAllButSelf(); err != nil {
		t.Fatalf("failed to connect mocknet peers: %v", err)
	}

	pubEngine := createTestEngine(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	payload := make([]byte, 1024*1024) // 1 MB payload -> 4 chunks
	_, _ = rand.Read(payload)

	m, err := pubEngine.Ingest(ctx, bytes.NewReader(payload), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest content: %v", err)
	}

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize manifest: %v", err)
	}
	if err := pubEngine.PutManifestBytes(ctx, m.Descriptor.ID, mBytes); err != nil {
		t.Fatalf("failed to store manifest: %v", err)
	}

	pubTransport := transport.NewTransport(pubHost)

	replication := 3
	plan, err := distribution.PlanPlacement(m, providerIDs, replication)
	if err != nil {
		t.Fatalf("failed to plan placement: %v", err)
	}

	tracker := distribution.NewGlobalReplicaTracker(replication)

	// Distribute with low concurrency limit (MaxConcurrentProviders = 2)
	cfg := distribution.UploaderConfig{
		MaxRetriesPerChunk:    2,
		FailoverRounds:        2,
		MaxConcurrentProviders: 2,
	}

	err = distribution.Distribute(ctx, pubTransport, pubEngine, plan, tracker, cfg)
	if err != nil {
		t.Fatalf("Distribute failed under concurrency limit: %v", err)
	}

	if !tracker.IsComplete(plan.Manifest.ChunkIDs) {
		t.Errorf("tracker reported incomplete replication after Distribute")
	}
}
