package retrieval_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/chunk"
	"cipher/internal/reputation"
	"cipher/internal/retrieval"
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

func setupNetwork(t testing.TB) (host.Host, host.Host, host.Host) {
	mocknet := mocknet.New()

	hClient, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	hGood, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	hBad, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}

	return hClient, hGood, hBad
}

func TestResolveManifest_BypassesBannedProvider(t *testing.T) {
	hClient, hGood, hBad := setupNetwork(t)

	engClient := createTestEngine(t)
	engGood := createTestEngine(t)
	engBad := createTestEngine(t)

	chunk.NewStreamHandler(hGood, engGood)
	chunk.NewStreamHandler(hBad, engBad)

	ctx := context.Background()
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := engGood.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	engGood.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	tracker := reputation.NewPeerReputationTracker()
	tracker.RecordIntegrityFault(hBad.ID())

	providers := []peer.ID{hBad.ID(), hGood.ID()}

	resolved, err := retrieval.ResolveManifest(
		ctx,
		m.Descriptor.ID,
		nil,
		transport.NewTransport(hClient),
		engClient,
		providers,
		retrieval.WithResolverTracker(tracker),
	)
	if err != nil {
		t.Fatalf("Expected ResolveManifest to succeed by bypassing bad provider, got: %v", err)
	}

	if resolved.Descriptor.ID != m.Descriptor.ID {
		t.Fatalf("Content ID mismatch")
	}
}
