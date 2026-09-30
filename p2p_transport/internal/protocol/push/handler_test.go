package push_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

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

func TestStreamHandler_PushFlow(t *testing.T) {
	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	handler := push.NewStreamHandler(h1, eng1, nil, true, push.AuthPolicyOpen, nil)
	defer handler.Close()

	ctx := context.Background()
	data := make([]byte, 512*1024)
	_, _ = rand.Read(data)

	m, err := eng2.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()

	client, err := push.NewClient(ctx, transport.NewTransport(h2), h1.ID())
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	// 1. Send Manifest
	if err := client.SendManifest(ctx, m.Descriptor.ID, m.ChunkIDs, mBytes); err != nil {
		t.Fatalf("SendManifest failed: %v", err)
	}

	// 2. Send Chunks
	for _, cid := range m.ChunkIDs {
		c, err := eng2.GetChunk(ctx, cid)
		if err != nil {
			t.Fatalf("GetChunk failed: %v", err)
		}
		if err := client.SendChunk(ctx, m.Descriptor.ID, c); err != nil {
			t.Fatalf("SendChunk failed: %v", err)
		}
	}

	// 3. Send Batch Complete
	if err := client.SendBatchComplete(ctx, m.Descriptor.ID); err != nil {
		t.Fatalf("SendBatchComplete failed: %v", err)
	}

	// Verify eng1 now has manifest
	_, err = eng1.GetManifestBytes(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Expected eng1 to have committed manifest %x: %v", m.Descriptor.ID, err)
	}
}

func TestStreamHandler_LRUEviction(t *testing.T) {
	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Set max sessions = 2
	handler := push.NewStreamHandlerWithOptions(
		h1, eng1, nil, true, push.AuthPolicyOpen, nil,
		push.WithMaxSessions(2),
	)
	defer handler.Close()

	ctx := context.Background()

	// Create 3 manifests
	createManifest := func() (*manifest.Manifest, []byte) {
		data := make([]byte, 64*1024)
		_, _ = rand.Read(data)
		m, _ := eng2.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
		mb, _ := m.Serialize()
		return m, mb
	}

	m1, mb1 := createManifest()
	m2, mb2 := createManifest()
	m3, mb3 := createManifest()

	client, err := push.NewClient(ctx, transport.NewTransport(h2), h1.ID())
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	// Send Manifest 1, 2, 3
	if err := client.SendManifest(ctx, m1.Descriptor.ID, m1.ChunkIDs, mb1); err != nil {
		t.Fatalf("SendManifest 1 failed: %v", err)
	}
	if err := client.SendManifest(ctx, m2.Descriptor.ID, m2.ChunkIDs, mb2); err != nil {
		t.Fatalf("SendManifest 2 failed: %v", err)
	}
	// Manifest 3 forces LRU eviction of Manifest 1
	if err := client.SendManifest(ctx, m3.Descriptor.ID, m3.ChunkIDs, mb3); err != nil {
		t.Fatalf("SendManifest 3 failed: %v", err)
	}

	// Attempt to send chunk for evicted Manifest 1 -> should fail
	c1, _ := eng2.GetChunk(ctx, m1.ChunkIDs[0])
	err = client.SendChunk(ctx, m1.Descriptor.ID, c1)
	if err == nil {
		t.Fatalf("Expected chunk send for evicted session to fail, got nil")
	}

	// Chunk send for active Manifest 2 should succeed
	c2, _ := eng2.GetChunk(ctx, m2.ChunkIDs[0])
	if err := client.SendChunk(ctx, m2.Descriptor.ID, c2); err != nil {
		t.Fatalf("Expected chunk send for active session to succeed, got %v", err)
	}
}

func TestStreamHandler_TTLExpiration(t *testing.T) {
	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Set short TTL (50ms) and short cleanup interval (20ms)
	handler := push.NewStreamHandlerWithOptions(
		h1, eng1, nil, true, push.AuthPolicyOpen, nil,
		push.WithSessionTTL(50*time.Millisecond),
		push.WithCleanupInterval(20*time.Millisecond),
	)
	defer handler.Close()

	ctx := context.Background()

	data := make([]byte, 64*1024)
	_, _ = rand.Read(data)
	m, _ := eng2.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	mb, _ := m.Serialize()

	client, err := push.NewClient(ctx, transport.NewTransport(h2), h1.ID())
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	if err := client.SendManifest(ctx, m.Descriptor.ID, m.ChunkIDs, mb); err != nil {
		t.Fatalf("SendManifest failed: %v", err)
	}

	// Wait for TTL expiration + cleanup
	time.Sleep(100 * time.Millisecond)

	c, _ := eng2.GetChunk(ctx, m.ChunkIDs[0])
	err = client.SendChunk(ctx, m.Descriptor.ID, c)
	if err == nil {
		t.Fatalf("Expected chunk send to fail after TTL expiration, got nil")
	}
}
