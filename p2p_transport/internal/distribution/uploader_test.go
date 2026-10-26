package distribution

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/push"
	"cipher/internal/transport"
)

func TestDefaultUploaderConfig(t *testing.T) {
	if DefaultUploaderConfig.MaxConcurrentProviders != 8 {
		t.Errorf("expected DefaultUploaderConfig.MaxConcurrentProviders to be 8, got %d",
			DefaultUploaderConfig.MaxConcurrentProviders)
	}
}

func setupTestNode(t *testing.T, tmpDir string) (host.Host, *engine.ContentEngine) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create libp2p host: %v", err)
	}

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to create store dir: %v", err)
	}
	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(tmpDir)
	eng := engine.NewContentEngine(config, enc, dig, store, store, keys, store)

	return h, eng
}

func TestDistributeWithConcurrencyLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	baseDir := t.TempDir()

	// 1. Create publisher
	pubHost, pubEng := setupTestNode(t, filepath.Join(baseDir, "pub"))
	defer pubHost.Close()
	pubTransport := transport.NewTransport(pubHost)

	// 2. Create 6 provider nodes
	numProviders := 6
	provHosts := make([]host.Host, numProviders)
	provIDs := make([]peer.ID, numProviders)

	for i := 0; i < numProviders; i++ {
		pHost, pEng := setupTestNode(t, filepath.Join(baseDir, fmt.Sprintf("prov_%d", i)))
		defer pHost.Close()

		// Register push stream handler on provider
		push.NewStreamHandler(pHost, pEng, nil, true, push.AuthPolicyOpen, nil)

		provHosts[i] = pHost
		provIDs[i] = pHost.ID()

		// Connect publisher to provider
		pInfo := peer.AddrInfo{
			ID:    pHost.ID(),
			Addrs: pHost.Addrs(),
		}
		if err := pubHost.Connect(ctx, pInfo); err != nil {
			t.Fatalf("failed to connect publisher to provider %d: %v", i, err)
		}
	}

	// 3. Create dummy file content and ingest
	testData := make([]byte, 128*1024) // 4 chunks (32KB each)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	tmpFile := filepath.Join(baseDir, "test.dat")
	if err := os.WriteFile(tmpFile, testData, 0644); err != nil {
		t.Fatalf("failed to write tmp file: %v", err)
	}

	f, err := os.Open(tmpFile)
	if err != nil {
		t.Fatalf("failed to open test file: %v", err)
	}
	defer f.Close()

	m, err := pubEng.Ingest(ctx, f, manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest content: %v", err)
	}

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize manifest: %v", err)
	}
	if err := pubEng.PutManifestBytes(ctx, m.Descriptor.ID, mBytes); err != nil {
		t.Fatalf("failed to put manifest bytes: %v", err)
	}

	// 4. Create placement plan (R=2 across 6 providers)
	replication := 2
	plan, err := PlanPlacement(m, provIDs, replication)
	if err != nil {
		t.Fatalf("PlanPlacement failed: %v", err)
	}

	tracker := NewGlobalReplicaTracker(replication)
	cfg := UploaderConfig{
		MaxRetriesPerChunk:    2,
		FailoverRounds:        1,
		MaxConcurrentProviders: 2, // Strict limit of 2 concurrent provider uploads
	}

	// 5. Execute distribution
	if err := Distribute(ctx, pubTransport, pubEng, plan, tracker, cfg); err != nil {
		t.Fatalf("Distribute failed: %v", err)
	}

	// 6. Verify replication invariant satisfied
	if !tracker.IsComplete(m.ChunkIDs) {
		t.Errorf("expected tracker.IsComplete to be true")
	}
}
