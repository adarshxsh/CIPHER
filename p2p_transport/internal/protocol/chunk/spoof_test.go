package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/retrieval"
	"cipher/internal/transport"
)

func TestClient_Resolve_RejectsSpoofedManifest(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Peer 1 acts as server
	chunk.NewStreamHandler(h1, eng1)
	// Peer 2 acts as server/client
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()

	// Ingest legitimate file on eng1
	data := make([]byte, 1024)
	rand.Read(data)

	legitManifest, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	legitContentID := legitManifest.Descriptor.ID

	// Create a spoofed manifest (e.g. altered size or chunk topology)
	spoofedManifest := *legitManifest
	spoofedManifest.Descriptor.Size = 999999
	spoofedManifest.ChunkIDs = append(spoofedManifest.ChunkIDs, core.ChunkID{0xFF, 0xFF})

	spoofedBytes, err := spoofedManifest.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize spoofed manifest: %v", err)
	}

	// Store spoofed manifest under legitContentID on server (Peer 1)
	if err := eng1.PutManifestBytes(ctx, legitContentID, spoofedBytes); err != nil {
		t.Fatalf("Failed to store spoofed manifest in engine: %v", err)
	}

	// Peer 2 attempts to resolve legitContentID from Peer 1
	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	_, err = client.Resolve(ctx, legitContentID)
	if err == nil {
		t.Fatalf("Expected Resolve to reject spoofed manifest, but it succeeded")
	}

	if !errors.Is(err, chunk.ErrContentIDMismatch) {
		t.Errorf("Expected error to wrap ErrContentIDMismatch, got %v", err)
	}
}

func TestResolver_ResolveManifest_RejectsSpoofedManifest(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()

	data := []byte("legitimate provider content")
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	legitContentID := m.Descriptor.ID

	// Store corrupt/spoofed manifest bytes under legitContentID
	badManifest := *m
	badManifest.Descriptor.Type = "spoofed_type"
	badBytes, _ := badManifest.Serialize()
	eng1.PutManifestBytes(ctx, legitContentID, badBytes)

	_, err = retrieval.ResolveManifest(ctx, legitContentID, nil, transport.NewTransport(h2), eng2, []peer.ID{h1.ID()})
	if err == nil {
		t.Fatalf("Expected ResolveManifest to fail for spoofed manifest")
	}

	if !errors.Is(err, manifest.ErrContentIDMismatch) {
		t.Errorf("Expected errors.Is(err, manifest.ErrContentIDMismatch), got %v", err)
	}
}
