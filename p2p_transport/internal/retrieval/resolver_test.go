package retrieval_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/discovery"
	"cipher/internal/protocol/chunk"
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

func setupMockNetwork(t testing.TB, numPeers int) []host.Host {
	net := mocknet.New()
	hosts := make([]host.Host, numPeers)
	for i := 0; i < numPeers; i++ {
		h, err := net.GenPeer()
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = h
	}
	if err := net.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return hosts
}

func TestResolveManifest_PublisherProvider(t *testing.T) {
	hosts := setupMockNetwork(t, 2)
	hPublisher, hClient := hosts[0], hosts[1]

	engPublisher := createTestEngine(t)
	pubPriv := hPublisher.Peerstore().PrivKey(hPublisher.ID())
	engPublisher.SetPublisherKey(pubPriv)

	engClient := createTestEngine(t)

	chunk.NewStreamHandler(hPublisher, engPublisher)

	ctx := context.Background()
	data := make([]byte, 1024*1024)
	rand.Read(data)

	m, err := engPublisher.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	mBytes, _ := m.Serialize()
	if err := engPublisher.PutManifestBytes(ctx, m.Descriptor.ID, mBytes); err != nil {
		t.Fatalf("PutManifestBytes failed: %v", err)
	}

	tClient := transport.NewTransport(hClient)
	resolvedManifest, err := retrieval.ResolveManifest(
		ctx,
		m.Descriptor.ID,
		nil,
		tClient,
		engClient,
		[]peer.ID{hPublisher.ID()},
	)
	if err != nil {
		t.Fatalf("ResolveManifest failed: %v", err)
	}

	if err := resolvedManifest.VerifySignature(); err != nil {
		t.Fatalf("Resolved manifest signature verification failed: %v", err)
	}

	pubPeerID, err := resolvedManifest.PublisherPeerID()
	if err != nil {
		t.Fatalf("PublisherPeerID failed: %v", err)
	}

	if pubPeerID != hPublisher.ID() {
		t.Fatalf("expected publisher %s, got %s", hPublisher.ID(), pubPeerID)
	}
}

func TestResolveManifest_SecondaryProviderWithAttestation(t *testing.T) {
	hosts := setupMockNetwork(t, 3)
	hPublisher, hProvider, hClient := hosts[0], hosts[1], hosts[2]

	engPublisher := createTestEngine(t)
	pubPriv := hPublisher.Peerstore().PrivKey(hPublisher.ID())
	engPublisher.SetPublisherKey(pubPriv)

	engProvider := createTestEngine(t)
	engClient := createTestEngine(t)

	chunk.NewStreamHandler(hPublisher, engPublisher)
	chunk.NewStreamHandler(hProvider, engProvider)

	ctx := context.Background()
	data := make([]byte, 1024*1024)
	rand.Read(data)

	m, err := engPublisher.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Publisher creates ownership attestation for hProvider
	att, err := discovery.CreateAttestation(pubPriv, m.Descriptor.ID, hProvider.ID())
	if err != nil {
		t.Fatalf("CreateAttestation failed: %v", err)
	}

	// Attach attestation to manifest served by hProvider
	mWithAttestation := *m
	mWithAttestation.Attestation = att
	mBytes, _ := mWithAttestation.Serialize()

	if err := engProvider.PutManifestBytes(ctx, m.Descriptor.ID, mBytes); err != nil {
		t.Fatalf("PutManifestBytes on provider failed: %v", err)
	}

	tClient := transport.NewTransport(hClient)
	resolvedManifest, err := retrieval.ResolveManifest(
		ctx,
		m.Descriptor.ID,
		nil,
		tClient,
		engClient,
		[]peer.ID{hProvider.ID()},
	)
	if err != nil {
		t.Fatalf("ResolveManifest from secondary provider failed: %v", err)
	}

	if resolvedManifest.Attestation == nil {
		t.Fatalf("expected attestation in resolved manifest")
	}

	pubKey, err := resolvedManifest.PublisherPublicKey()
	if err != nil {
		t.Fatalf("PublisherPublicKey failed: %v", err)
	}

	if err := discovery.VerifyAttestation(resolvedManifest.Attestation, pubKey, m.Descriptor.ID, hProvider.ID()); err != nil {
		t.Fatalf("VerifyAttestation failed: %v", err)
	}
}

func TestResolveManifest_SpoofedProviderFailover(t *testing.T) {
	hosts := setupMockNetwork(t, 4)
	hPublisher, hRogue, hValidProvider, hClient := hosts[0], hosts[1], hosts[2], hosts[3]

	engPublisher := createTestEngine(t)
	pubPriv := hPublisher.Peerstore().PrivKey(hPublisher.ID())
	engPublisher.SetPublisherKey(pubPriv)

	engRogue := createTestEngine(t)
	engValidProvider := createTestEngine(t)
	engClient := createTestEngine(t)

	chunk.NewStreamHandler(hRogue, engRogue)
	chunk.NewStreamHandler(hValidProvider, engValidProvider)

	ctx := context.Background()
	data := make([]byte, 1024*1024)
	rand.Read(data)

	m, err := engPublisher.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// 1. Rogue provider stores publisher's manifest directly without holding a valid ownership attestation
	unauthBytes, _ := m.Serialize()
	engRogue.PutManifestBytes(ctx, m.Descriptor.ID, unauthBytes)

	// 2. Valid provider stores manifest with valid attestation from Publisher
	att, err := discovery.CreateAttestation(pubPriv, m.Descriptor.ID, hValidProvider.ID())
	if err != nil {
		t.Fatalf("CreateAttestation failed: %v", err)
	}
	validManifest := *m
	validManifest.Attestation = att
	validBytes, _ := validManifest.Serialize()
	engValidProvider.PutManifestBytes(ctx, m.Descriptor.ID, validBytes)

	// 3. Client tries resolving with Rogue Provider first, then Valid Provider
	tClient := transport.NewTransport(hClient)
	resolvedManifest, err := retrieval.ResolveManifest(
		ctx,
		m.Descriptor.ID,
		nil,
		tClient,
		engClient,
		[]peer.ID{hRogue.ID(), hValidProvider.ID()},
	)
	if err != nil {
		t.Fatalf("ResolveManifest should have succeeded by failing over to valid provider, got: %v", err)
	}

	pubPeerID, err := resolvedManifest.PublisherPeerID()
	if err != nil {
		t.Fatalf("PublisherPeerID failed: %v", err)
	}

	if pubPeerID != hPublisher.ID() {
		t.Fatalf("expected resolved manifest from publisher %s, got %s", hPublisher.ID(), pubPeerID)
	}
}

func TestResolveManifest_AllProvidersInvalid(t *testing.T) {
	hosts := setupMockNetwork(t, 2)
	hRogue, hClient := hosts[0], hosts[1]

	engRogue := createTestEngine(t)
	engClient := createTestEngine(t)
	chunk.NewStreamHandler(hRogue, engRogue)

	ctx := context.Background()
	var cid core.ContentID
	copy(cid[:], []byte("bad_content_id_123456789012345"))

	// Rogue serves unsigned manifest
	unsignedManifest := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   cid,
			Type: manifest.TypeFile,
			Size: 100,
		},
	}
	uBytes, _ := unsignedManifest.Serialize()
	engRogue.PutManifestBytes(ctx, cid, uBytes)

	tClient := transport.NewTransport(hClient)
	_, err := retrieval.ResolveManifest(
		ctx,
		cid,
		nil,
		tClient,
		engClient,
		[]peer.ID{hRogue.ID()},
	)
	if err == nil {
		t.Fatalf("expected error when all providers are invalid, got nil")
	}
}
