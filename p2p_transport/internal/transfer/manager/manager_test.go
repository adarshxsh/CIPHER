package manager_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transfer/manager"
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

func setupMockNetwork(t testing.TB, count int) []host.Host {
	net := mocknet.New()
	hosts := make([]host.Host, count)
	for i := 0; i < count; i++ {
		h, err := net.GenPeer()
		if err != nil {
			t.Fatalf("GenPeer failed: %v", err)
		}
		hosts[i] = h
	}
	if err := net.LinkAll(); err != nil {
		t.Fatalf("LinkAll failed: %v", err)
	}
	return hosts
}

func TestTransferManager_Download_MixedSwarm_LogsMetrics(t *testing.T) {
	hosts := setupMockNetwork(t, 3)
	hClient := hosts[0]
	hClean := hosts[1]
	hCorrupt := hosts[2]

	engClient := createTestEngine(t)
	engClean := createTestEngine(t)
	engCorrupt := createTestEngine(t)

	chunk.NewStreamHandler(hClean, engClean)
	chunk.NewStreamHandler(hCorrupt, engCorrupt, chunk.WithCorruptProbability(1.0))

	ctx := context.Background()
	data := make([]byte, 512*1024) // 2 chunks
	rand.Read(data)

	m, err := engClean.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	for _, chunkID := range m.ChunkIDs {
		c, err := engClean.GetChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("GetChunk failed: %v", err)
		}
		if err := engCorrupt.PutChunk(ctx, c); err != nil {
			t.Fatalf("PutChunk failed: %v", err)
		}
	}

	sm, err := manager.NewFileSessionManager(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}
	tm := manager.NewTransferManager(sm, engClient, transport.NewTransport(hClient))

	peers := []peer.ID{hCorrupt.ID(), hClean.ID()}

	err = tm.Download(ctx, m.Descriptor.ID, m.ChunkIDs, peers)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	sess, err := sm.Open(m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Open session failed: %v", err)
	}

	if sess.Status != manager.StatusCompleted {
		t.Errorf("Expected session status completed, got %v", sess.Status)
	}
}
