package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	mrand "math/rand"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

// CorruptingStream wraps a network.Stream to inject fault/data corruption into stream writes or reads.
type CorruptingStream struct {
	network.Stream
	corruptProb    float64
	corruptOnWrite bool
}

func NewCorruptingStream(s network.Stream, prob float64, corruptOnWrite bool) *CorruptingStream {
	return &CorruptingStream{
		Stream:         s,
		corruptProb:    prob,
		corruptOnWrite: corruptOnWrite,
	}
}

func (c *CorruptingStream) Write(p []byte) (int, error) {
	if c.corruptOnWrite && c.corruptProb > 0 && len(p) > 0 {
		if mrand.Float64() < c.corruptProb {
			corrupted := make([]byte, len(p))
			copy(corrupted, p)
			// Corrupt the last byte of the frame
			corrupted[len(corrupted)-1] ^= 0xFF
			return c.Stream.Write(corrupted)
		}
	}
	return c.Stream.Write(p)
}

func (c *CorruptingStream) Read(p []byte) (int, error) {
	n, err := c.Stream.Read(p)
	if err == nil && !c.corruptOnWrite && c.corruptProb > 0 && n > 0 {
		if mrand.Float64() < c.corruptProb {
			p[n-1] ^= 0xFF
		}
	}
	return n, err
}

func TestFaultInjection_MockStreamCorruptsData(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	handler := chunk.NewStreamHandler(h1, eng1)

	// Wrap stream handler on h1 with 100% corruption on writes
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		corruptedStream := NewCorruptingStream(s, 1.0, true)
		handler.HandleStream(corruptedStream)
	})

	ctx := context.Background()
	data := make([]byte, 512*1024) // 2 chunks
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest data: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Download should fail because transmitted data is corrupted by mock stream wrapper
	err = client.Download(ctx, m.ChunkIDs)
	if err == nil {
		t.Fatalf("Expected download to fail due to mock stream byte corruption, but it succeeded")
	}
}

func TestFaultInjection_NoCorruptionSuccess(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	handler := chunk.NewStreamHandler(h1, eng1)

	// Wrap stream handler on h1 with 0% corruption (clean mock stream)
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		cleanStream := NewCorruptingStream(s, 0.0, true)
		handler.HandleStream(cleanStream)
	})

	ctx := context.Background()
	data := make([]byte, 512*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest data: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	if err := client.Download(ctx, m.ChunkIDs); err != nil {
		t.Fatalf("Expected download to succeed with clean mock stream, got error: %v", err)
	}

	for _, chunkID := range m.ChunkIDs {
		if _, err := eng2.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("eng2 missing chunk %x", chunkID)
		}
	}
}
