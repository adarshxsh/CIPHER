package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/retrieval"
	"cipher/internal/transport"
)

func TestSignedManifest_ValidResolution(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	resolvedData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Expected valid manifest resolution, got error: %v", err)
	}

	if !bytes.Equal(resolvedData, mBytes) {
		t.Errorf("Resolved manifest bytes mismatch")
	}
}

func TestSignedManifest_SpoofedSignatureRejection(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	handler1 := chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	// Override handler1's private key with a newly generated key (spoofed identity)
	fakePriv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate fake key: %v", err)
	}
	handler1.SetPrivateKey(fakePriv)

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	_, err = client.Resolve(ctx, m.Descriptor.ID)
	if err == nil {
		t.Fatalf("Expected client to reject spoofed signature, but got nil error")
	}

	if !strings.Contains(err.Error(), "invalid provider signature") {
		t.Errorf("Expected 'invalid provider signature' error, got: %v", err)
	}
}

func TestSignedManifest_MissingSignatureRejection(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	handler1 := chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	// Set nil private key to simulate an unauthenticated provider missing signature
	handler1.SetPrivateKey(nil)

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	_, err = client.Resolve(ctx, m.Descriptor.ID)
	if err == nil {
		t.Fatalf("Expected client to reject missing signature, but got nil error")
	}

	if !strings.Contains(err.Error(), "missing provider signature") {
		t.Errorf("Expected 'missing provider signature' error, got: %v", err)
	}
}

func setupMockNetwork3(t testing.TB) (host.Host, host.Host, host.Host) {
	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h3, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return h1, h2, h3
}

func TestSignedManifest_MultiProviderFallback(t *testing.T) {
	h1, h2, h3 := setupMockNetwork3(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)
	eng3 := createTestEngine(t)

	// h1 is spoofed provider, h2 is client, h3 is authentic provider
	handler1 := chunk.NewStreamHandler(h1, eng1)
	fakePriv, _, _ := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	handler1.SetPrivateKey(fakePriv)

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	eng3.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)
	chunk.NewStreamHandler(h3, eng3)

	providers := []peer.ID{h1.ID(), h3.ID()}

	t2 := transport.NewTransport(h2)

	// ResolveManifest should skip h1 (spoofed) and resolve from h3 (authentic)
	resolvedManifest, err := retrieval.ResolveManifest(ctx, m.Descriptor.ID, nil, t2, eng2, providers)
	if err != nil {
		t.Fatalf("ResolveManifest failed: %v", err)
	}

	if resolvedManifest.Descriptor.ID != m.Descriptor.ID {
		t.Errorf("Manifest content ID mismatch: expected %x, got %x", m.Descriptor.ID, resolvedManifest.Descriptor.ID)
	}
}
