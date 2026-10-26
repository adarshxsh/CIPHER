package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestManifestAttestation_ValidSignature(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := []byte("hello signed manifest world")
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
		t.Fatalf("Expected successful signed manifest resolution, got: %v", err)
	}

	if !bytes.Equal(resolvedData, mBytes) {
		t.Fatalf("Resolved manifest data mismatch")
	}
}

func TestManifestAttestation_SpoofedPeerID(t *testing.T) {
	h1, h2 := setupMockNetwork(t) // h1 is legitimate, h2 is client
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Create custom handler on h1 that returns spoofed ProviderID (h2's ID instead of h1's ID)
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		msg, err := chunk.ReadMessage(s)
		if err != nil {
			return
		}
		contentID, err := chunk.ParseRequestManifest(msg.Payload)
		if err != nil {
			return
		}
		manifestData, _ := eng1.GetManifestBytes(context.Background(), contentID)
		
		// Spoof: claims provider ID is h2.ID()
		now := time.Now().Unix()
		privKey := h1.Peerstore().PrivKey(h1.ID())
		sig, _ := privKey.Sign(chunk.FormatAttestationData(contentID, h2.ID(), now))
		
		resp := chunk.BuildManifest(contentID, manifestData, now, h2.ID(), sig)
		chunk.WriteMessage(s, resp)
	})

	ctx := context.Background()
	data := []byte("spoofed peer ID content")
	m, _ := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	_, err = client.Resolve(ctx, m.Descriptor.ID)
	if err == nil {
		t.Fatalf("Expected error for spoofed provider peer ID, got nil")
	}
}

func TestManifestAttestation_CorruptedSignature(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Handler on h1 sends corrupt signature
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		msg, err := chunk.ReadMessage(s)
		if err != nil {
			return
		}
		contentID, _ := chunk.ParseRequestManifest(msg.Payload)
		manifestData, _ := eng1.GetManifestBytes(context.Background(), contentID)

		now := time.Now().Unix()
		privKey := h1.Peerstore().PrivKey(h1.ID())
		sig, _ := privKey.Sign(chunk.FormatAttestationData(contentID, h1.ID(), now))
		if len(sig) > 0 {
			sig[0] ^= 0xFF // corrupt signature
		}

		resp := chunk.BuildManifest(contentID, manifestData, now, h1.ID(), sig)
		chunk.WriteMessage(s, resp)
	})

	ctx := context.Background()
	data := []byte("corrupt signature content")
	m, _ := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	_, err = client.Resolve(ctx, m.Descriptor.ID)
	if err == nil {
		t.Fatalf("Expected signature verification error, got nil")
	}
}

func TestManifestAttestation_ExpiredTimestamp(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Handler on h1 sends timestamp 10 minutes in the past
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		msg, err := chunk.ReadMessage(s)
		if err != nil {
			return
		}
		contentID, _ := chunk.ParseRequestManifest(msg.Payload)
		manifestData, _ := eng1.GetManifestBytes(context.Background(), contentID)

		oldTimestamp := time.Now().Add(-10 * time.Minute).Unix()
		privKey := h1.Peerstore().PrivKey(h1.ID())
		sig, _ := privKey.Sign(chunk.FormatAttestationData(contentID, h1.ID(), oldTimestamp))

		resp := chunk.BuildManifest(contentID, manifestData, oldTimestamp, h1.ID(), sig)
		chunk.WriteMessage(s, resp)
	})

	ctx := context.Background()
	data := []byte("expired timestamp content")
	m, _ := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	_, err = client.Resolve(ctx, m.Descriptor.ID)
	if err == nil {
		t.Fatalf("Expected timestamp expiry error, got nil")
	}
}

func TestManifestAttestation_FutureTimestamp(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Handler on h1 sends timestamp 10 minutes in the future
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		msg, err := chunk.ReadMessage(s)
		if err != nil {
			return
		}
		contentID, _ := chunk.ParseRequestManifest(msg.Payload)
		manifestData, _ := eng1.GetManifestBytes(context.Background(), contentID)

		futureTimestamp := time.Now().Add(10 * time.Minute).Unix()
		privKey := h1.Peerstore().PrivKey(h1.ID())
		sig, _ := privKey.Sign(chunk.FormatAttestationData(contentID, h1.ID(), futureTimestamp))

		resp := chunk.BuildManifest(contentID, manifestData, futureTimestamp, h1.ID(), sig)
		chunk.WriteMessage(s, resp)
	})

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)
	m, _ := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	_, err = client.Resolve(ctx, m.Descriptor.ID)
	if err == nil {
		t.Fatalf("Expected future timestamp rejection error, got nil")
	}
}
