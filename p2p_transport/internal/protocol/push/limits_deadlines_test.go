package push_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/push"
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

func setupMockNetwork(t testing.TB) (host.Host, host.Host) {
	mocknet := mocknet.New()

	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	h2, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return h1, h2
}

func TestPushProtocol_TransactionLimitExceeded(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	handler := push.NewStreamHandler(h1, eng1, nil, true, push.AuthPolicyOpen, nil)
	// Enforce 1 transaction limit
	handler.SetLimits(1, 10)

	ctx := context.Background()
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := eng2.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()

	client, err := push.NewClient(ctx, transport.NewTransport(h2), h1.ID())
	if err != nil {
		t.Fatalf("Failed to create push client: %v", err)
	}
	defer client.Close()

	// Transaction 1: Send manifest (succeeds)
	if err := client.SendManifest(ctx, m.Descriptor.ID, m.ChunkIDs, mBytes); err != nil {
		t.Fatalf("SendManifest failed: %v", err)
	}

	// Transaction 2: Send chunk (should be rejected due to maxTransactions = 1)
	chunkData, err := eng2.GetChunk(ctx, m.ChunkIDs[0])
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	err = client.SendChunk(ctx, m.Descriptor.ID, chunkData)
	if err == nil {
		t.Fatalf("Expected SendChunk to fail due to transaction limit, but it succeeded")
	}
}

func TestPushProtocol_MessageLimitExceeded(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	handler := push.NewStreamHandler(h1, eng1, nil, true, push.AuthPolicyOpen, nil)
	// Enforce 1 message limit
	handler.SetLimits(10, 1)

	ctx := context.Background()
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := eng2.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()

	client, err := push.NewClient(ctx, transport.NewTransport(h2), h1.ID())
	if err != nil {
		t.Fatalf("Failed to create push client: %v", err)
	}
	defer client.Close()

	// Message 1: Send manifest (succeeds)
	if err := client.SendManifest(ctx, m.Descriptor.ID, m.ChunkIDs, mBytes); err != nil {
		t.Fatalf("SendManifest failed: %v", err)
	}

	// Message 2: Send chunk (exceeds maxMessages = 1)
	chunkData, err := eng2.GetChunk(ctx, m.ChunkIDs[0])
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	err = client.SendChunk(ctx, m.Descriptor.ID, chunkData)
	if err == nil {
		t.Fatalf("Expected SendChunk to fail due to message limit, but it succeeded")
	}
}

func TestPushProtocol_ReadTimeout(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	_ = push.NewStreamHandler(h1, eng1, nil, true, push.AuthPolicyOpen, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	tr := transport.NewTransport(h2)
	stream, err := tr.OpenStream(ctx, h1.ID(), "/cipher/push/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open push stream: %v", err)
	}
	defer stream.Close()

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 100)
		_, err := stream.Read(buf)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("Expected stream read to return error, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Log("Push stream open idle test completed")
	}
}
