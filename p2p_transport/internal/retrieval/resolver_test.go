package retrieval_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/identity"
	"cipher/internal/protocol/chunk"
	"cipher/internal/retrieval"
	"cipher/internal/transport"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

func createTestNode(t *testing.T) (host.Host, *transport.Transport, *engine.ContentEngine, libp2pcrypto.PrivKey) {
	priv, err := identity.GenerateEphemeral()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	ctx := context.Background()
	h, _, err := transport.NewNode(ctx, 0, 0, priv, "", false)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	tmpDir := t.TempDir()
	storage.NewFSStorage(tmpDir)
	store := storage.NewFSStore(tmpDir)
	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()

	eng := engine.NewContentEngine(config, enc, dig, store, store, keys, store)
	eng.SetPublisherKey(priv)

	chunk.NewStreamHandler(h, eng)
	tr := transport.NewTransport(h)

	return h, tr, eng, priv
}

func TestResolveManifest_ValidAndSpoofedProviders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h1, _, eng1, _ := createTestNode(t)
	h2, _, eng2, _ := createTestNode(t)
	_, trClient, engClient, _ := createTestNode(t)

	// Ingest valid content on Node 1
	data := []byte("authenticity test content payload")
	m1, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes1, _ := m1.Serialize()
	eng1.PutManifestBytes(ctx, m1.Descriptor.ID, mBytes1)

	// Node 2 stores a spoofed manifest with an invalid signature for the same ContentID
	spoofedManifest := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   m1.Descriptor.ID,
			Type: manifest.TypeFile,
			Size: uint64(len(data)),
		},
		PublicKey: []byte("fake_public_key_32_bytes_1234567"),
		Signature: []byte("fake_invalid_signature_64_bytes_12345678901234567890123456789012"),
	}
	spoofedBytes, _ := spoofedManifest.Serialize()
	eng2.PutManifestBytes(ctx, m1.Descriptor.ID, spoofedBytes)

	// Connect Client Node to Node 1 and Node 2
	if err := trClient.ConnectPeer(ctx, h1.Peerstore().PeerInfo(h1.ID())); err != nil {
		t.Fatalf("failed to connect to node 1: %v", err)
	}
	if err := trClient.ConnectPeer(ctx, h2.Peerstore().PeerInfo(h2.ID())); err != nil {
		t.Fatalf("failed to connect to node 2: %v", err)
	}

	// ResolveManifest trying Node 2 (spoofed) first, then Node 1 (valid)
	targetPeers := []peer.ID{h2.ID(), h1.ID()}

	resolved, err := retrieval.ResolveManifest(ctx, m1.Descriptor.ID, nil, trClient, engClient, targetPeers)
	if err != nil {
		t.Fatalf("ResolveManifest failed to failover to valid provider: %v", err)
	}

	if !resolved.Verify() {
		t.Fatalf("Resolved manifest signature verification failed")
	}

	if resolved.Descriptor.ID != m1.Descriptor.ID {
		t.Fatalf("Resolved manifest ID mismatch")
	}

	// Test case where ALL providers are spoofed / invalid
	onlySpoofed := []peer.ID{h2.ID()}
	_, err = retrieval.ResolveManifest(ctx, m1.Descriptor.ID, nil, trClient, engClient, onlySpoofed)
	if err == nil {
		t.Fatalf("expected ResolveManifest to fail when all providers are spoofed")
	}
}
