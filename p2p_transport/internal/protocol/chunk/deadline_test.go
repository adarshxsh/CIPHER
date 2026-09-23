package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func setupRealNetwork(t testing.TB) (host.Host, host.Host) {
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("libp2p.New h1 failed: %v", err)
	}
	t.Cleanup(func() { h1.Close() })

	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("libp2p.New h2 failed: %v", err)
	}
	t.Cleanup(func() { h2.Close() })

	if err := h2.Connect(context.Background(), *host.InfoFromHost(h1)); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	return h1, h2
}

func TestChunkHandler_DefaultTimeouts(t *testing.T) {
	if chunk.DefaultReadTimeout != 15*time.Second {
		t.Errorf("Expected DefaultReadTimeout 15s, got %v", chunk.DefaultReadTimeout)
	}
	if chunk.DefaultWriteTimeout != 15*time.Second {
		t.Errorf("Expected DefaultWriteTimeout 15s, got %v", chunk.DefaultWriteTimeout)
	}
}

func TestChunkHandler_PeerCeasesTransmission_IdleTimeout(t *testing.T) {
	h1, h2 := setupRealNetwork(t)
	eng1 := createTestEngine(t)

	sh := chunk.NewStreamHandler(h1, eng1)
	sh.SetReadTimeout(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Peer ceases transmission immediately after opening stream (idle peer).
	// Stream handler should time out after ~200ms and reset/close stream.
	buf := make([]byte, 100)
	start := time.Now()
	_, err = s.Read(buf)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Expected read error on peer due to stream closure by handler, got nil")
	}
	if elapsed > 1*time.Second {
		t.Errorf("Stream took too long to close on idle timeout: %v", elapsed)
	}
}

func TestChunkHandler_PeerCeasesTransmission_MidFrameTimeout(t *testing.T) {
	h1, h2 := setupRealNetwork(t)
	eng1 := createTestEngine(t)

	sh := chunk.NewStreamHandler(h1, eng1)
	sh.SetReadTimeout(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Peer sends size header indicating 1000 bytes message, but only writes 10 bytes then ceases transmission mid-frame
	sizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuf, 1000)
	if _, err := s.Write(sizeBuf); err != nil {
		t.Fatalf("Failed to write frame size: %v", err)
	}
	if _, err := s.Write([]byte("partial123")); err != nil {
		t.Fatalf("Failed to write partial frame payload: %v", err)
	}

	// Peer halts sending.
	// Stream handler should time out on ReadMessage (io.ReadFull) and reset/close stream.
	buf := make([]byte, 100)
	start := time.Now()
	_, err = s.Read(buf)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Expected read error on peer due to stream closure by handler on mid-frame timeout, got nil")
	}
	if elapsed > 1*time.Second {
		t.Errorf("Stream took too long to close on mid-frame timeout: %v", elapsed)
	}
}

func TestChunkHandler_HighThroughputStreaming_NoSpuriousTimeouts(t *testing.T) {
	h1, h2 := setupRealNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	sh1 := chunk.NewStreamHandler(h1, eng1)
	sh2 := chunk.NewStreamHandler(h2, eng2)

	// Set short read timeout (500ms) on handlers.
	// A multi-chunk transfer that takes longer than 500ms total will succeed
	// because deadlines are refreshed on each wire frame.
	sh1.SetReadTimeout(500 * time.Millisecond)
	sh2.SetReadTimeout(500 * time.Millisecond)

	ctx := context.Background()
	dataSize := 1024 * 1024 // 4 chunks (256KB each)
	data := make([]byte, dataSize)
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
		t.Fatalf("Failed to resolve manifest: %v", err)
	}
	m2, err := manifest.Deserialize(resolvedData)
	if err != nil {
		t.Fatalf("Failed to deserialize manifest: %v", err)
	}

	for _, chunkID := range m2.ChunkIDs {
		time.Sleep(100 * time.Millisecond) // Inter-chunk delay; total duration will exceed 500ms
		chunkData, err := client.FetchChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("Failed to fetch chunk %x: %v", chunkID, err)
		}
		if err := eng2.PutChunk(ctx, chunkData); err != nil {
			t.Fatalf("Failed to store chunk %x: %v", chunkID, err)
		}
	}
}
