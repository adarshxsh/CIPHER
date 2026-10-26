package retrieval_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol"
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

func setupMockNetwork(t testing.TB) (host.Host, host.Host, host.Host) {
	mocknet := mocknet.New()

	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	h2, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	h3, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return h1, h2, h3
}

func TestResolveManifest_FailoverOnSpoofedProvider(t *testing.T) {
	clientHost, spoofedHost, legitimateHost := setupMockNetwork(t)

	engClient := createTestEngine(t)
	engSpoofed := createTestEngine(t)
	_ = engSpoofed
	engLegit := createTestEngine(t)

	ctx := context.Background()
	data := []byte("content for failover test")

	// Legitimate host ingests content
	m, err := engLegit.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	engLegit.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Legitimate host runs normal handler
	chunk.NewStreamHandler(legitimateHost, engLegit)

	// Spoofed host returns an invalid/tampered signature response for the same content
	spoofedHost.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		msg, err := chunk.ReadMessage(s)
		if err != nil {
			return
		}
		contentID, err := chunk.ParseRequestManifest(msg.Payload)
		if err != nil {
			return
		}

		// Return manifest bytes but with corrupted signature
		now := time.Now().Unix()
		badSig := []byte("invalid-signature-bytes")
		resp := chunk.BuildManifest(contentID, mBytes, now, spoofedHost.ID(), badSig)
		chunk.WriteMessage(s, resp)
	})

	// Client transport
	tClient := transport.NewTransport(clientHost)

	// Providers list has spoofedHost first, then legitimateHost second
	providers := []peer.ID{spoofedHost.ID(), legitimateHost.ID()}

	resolvedManifest, err := retrieval.ResolveManifest(ctx, m.Descriptor.ID, nil, tClient, engClient, providers)
	if err != nil {
		t.Fatalf("ResolveManifest failed to failover to legitimate provider: %v", err)
	}

	if resolvedManifest.Descriptor.ID != m.Descriptor.ID {
		t.Fatalf("Resolved manifest ID mismatch")
	}
}
