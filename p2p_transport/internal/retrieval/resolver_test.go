package retrieval

import (
	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/discovery"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
	"context"
	"strings"
	"testing"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestVerifyProviderRecord_AuthenticAndSpoofed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	priv1, _, err := libp2pcrypto.GenerateKeyPair(libp2pcrypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}

	priv2, _, err := libp2pcrypto.GenerateKeyPair(libp2pcrypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}

	h1, kdht1, err := transport.NewNode(ctx, 0, 0, priv1, "", false)
	if err != nil {
		t.Fatalf("failed to create host1: %v", err)
	}
	defer h1.Close()
	defer kdht1.Close()

	h2, kdht2, err := transport.NewNode(ctx, 0, 0, priv2, "", false)
	if err != nil {
		t.Fatalf("failed to create host2: %v", err)
	}
	defer h2.Close()
	defer kdht2.Close()

	// Connect h2 to h1
	h2Info := peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}
	if err := h1.Connect(ctx, h2Info); err != nil {
		t.Fatalf("failed to connect host1 to host2: %v", err)
	}

	var contentID core.ContentID
	for i := range contentID {
		contentID[i] = byte(i + 1)
	}

	// Host 1 announces content with signature
	if err := discovery.Provide(ctx, kdht1, contentID, priv1); err != nil {
		t.Fatalf("discovery.Provide failed: %v", err)
	}

	// Give DHT time to propagate value
	time.Sleep(200 * time.Millisecond)

	// Verify Host 1's record from Host 2
	rec, err := VerifyProviderRecord(ctx, kdht2, contentID, h1.ID())
	if err != nil {
		t.Fatalf("VerifyProviderRecord failed for authentic provider: %v", err)
	}
	if rec == nil || rec.ProviderPeerID != h1.ID() {
		t.Fatalf("expected valid record for provider %s, got: %v", h1.ID(), rec)
	}

	// Create a spoofed peer ID that hasn't signed the provider record
	_, spoofedPub, _ := libp2pcrypto.GenerateKeyPair(libp2pcrypto.Ed25519, -1)
	spoofedPeerID, _ := peer.IDFromPublicKey(spoofedPub)

	// Verify spoofed peer record fails
	_, err = VerifyProviderRecord(ctx, kdht2, contentID, spoofedPeerID)
	if err == nil {
		t.Fatal("expected VerifyProviderRecord to fail for spoofed provider peer, but passed")
	}
}

func TestResolveManifest_FiltersSpoofedProviders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	storage.NewFSStorage(tmpDir)
	store := storage.NewFSStore(tmpDir)

	priv1, _, _ := libp2pcrypto.GenerateKeyPair(libp2pcrypto.Ed25519, -1)
	priv2, _, _ := libp2pcrypto.GenerateKeyPair(libp2pcrypto.Ed25519, -1)

	h1, kdht1, _ := transport.NewNode(ctx, 0, 0, priv1, "", false)
	defer h1.Close()
	defer kdht1.Close()

	h2, kdht2, _ := transport.NewNode(ctx, 0, 0, priv2, "", false)
	defer h2.Close()
	defer kdht2.Close()

	h2Info := peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}
	h1.Connect(ctx, h2Info)

	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	eng1 := engine.NewContentEngine(config, enc, dig, store, store, keys, store)
	eng2 := engine.NewContentEngine(config, enc, dig, store, store, keys, store)

	chunk.NewStreamHandler(h1, eng1)

	// Ingest sample data on provider h1
	m, err := eng1.Ingest(ctx, strings.NewReader("hello provider test payload"), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Announce content with signature from provider h1
	if err := discovery.Provide(ctx, kdht1, m.Descriptor.ID, priv1); err != nil {
		t.Fatalf("failed to Provide: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Generate spoofed provider ID
	_, spoofedPub, _ := libp2pcrypto.GenerateKeyPair(libp2pcrypto.Ed25519, -1)
	spoofedPeerID, _ := peer.IDFromPublicKey(spoofedPub)

	t2 := transport.NewTransport(h2)

	// Attempt resolution with both spoofedPeerID and authentic h1.ID()
	providersList := []peer.ID{spoofedPeerID, h1.ID()}

	resManifest, err := ResolveManifest(ctx, m.Descriptor.ID, kdht2, t2, eng2, providersList)
	if err != nil {
		t.Fatalf("ResolveManifest failed: %v", err)
	}

	if resManifest.Descriptor.ID != m.Descriptor.ID {
		t.Fatalf("ContentID mismatch: expected %x, got %x", m.Descriptor.ID, resManifest.Descriptor.ID)
	}

	// Attempt resolution with ONLY spoofed provider
	_, err = ResolveManifest(ctx, m.Descriptor.ID, kdht2, t2, eng2, []peer.ID{spoofedPeerID})
	if err == nil {
		t.Fatal("expected ResolveManifest to fail when all providers are spoofed, but passed")
	}
}
