package retrieval_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/chunk"
	"cipher/internal/retrieval"
	"cipher/internal/transport"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

func createTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupMockNetwork3(t testing.TB) (host.Host, host.Host, host.Host) {
	mn := mocknet.New()

	hClient, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	hProvider1, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	hProvider2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return hClient, hProvider1, hProvider2
}

func TestResolveManifest_FailoverBadProvider(t *testing.T) {
	ctx := context.Background()
	hClient, hP1, hP2 := setupMockNetwork3(t)

	engClient := createTestEngine(t)
	engP1 := createTestEngine(t)
	engP2 := createTestEngine(t)

	chunk.NewStreamHandler(hP1, engP1)
	chunk.NewStreamHandler(hP2, engP2)

	// Create valid manifest
	data := make([]byte, 1024)
	rand.Read(data)

	validManifest, err := engP2.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest data: %v", err)
	}

	validBytes, err := validManifest.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize manifest: %v", err)
	}

	// P2 serves valid manifest
	engP2.PutManifestBytes(ctx, validManifest.Descriptor.ID, validBytes)

	// P1 serves bad manifest with tampered signature
	badManifest, _ := manifest.Deserialize(validBytes)
	badManifest.Signature[0] ^= 0xFF
	badBytes, _ := badManifest.Serialize()
	engP1.PutManifestBytes(ctx, validManifest.Descriptor.ID, badBytes)

	tClient := transport.NewTransport(hClient)
	providers := []peer.ID{hP1.ID(), hP2.ID()}

	mResolved, err := retrieval.ResolveManifest(ctx, validManifest.Descriptor.ID, nil, tClient, engClient, providers)
	if err != nil {
		t.Fatalf("ResolveManifest failed: %v", err)
	}

	if err := mResolved.VerifyPublisher(); err != nil {
		t.Fatalf("Resolved manifest has invalid signature: %v", err)
	}
}

func TestResolveManifest_AllProvidersInvalid(t *testing.T) {
	ctx := context.Background()
	hClient, hP1, hP2 := setupMockNetwork3(t)

	engClient := createTestEngine(t)
	engP1 := createTestEngine(t)
	engP2 := createTestEngine(t)

	chunk.NewStreamHandler(hP1, engP1)
	chunk.NewStreamHandler(hP2, engP2)

	data := make([]byte, 1024)
	rand.Read(data)

	m, err := engP1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest data: %v", err)
	}

	// Strip publisher signature
	m.Signature = nil
	m.PublisherPublicKey = nil
	invalidBytes, _ := m.Serialize()

	engP1.PutManifestBytes(ctx, m.Descriptor.ID, invalidBytes)
	engP2.PutManifestBytes(ctx, m.Descriptor.ID, invalidBytes)

	tClient := transport.NewTransport(hClient)
	providers := []peer.ID{hP1.ID(), hP2.ID()}

	_, err = retrieval.ResolveManifest(ctx, m.Descriptor.ID, nil, tClient, engClient, providers)
	if err == nil {
		t.Fatalf("Expected error when resolving from all invalid providers, got nil")
	}
}
