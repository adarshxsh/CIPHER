package distribution

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/push"
	"cipher/internal/transport"
)

func TestClearBuffer(t *testing.T) {
	buf := []byte{1, 2, 3, 4, 5, 255}
	clearBuffer(buf)
	for i, v := range buf {
		if v != 0 {
			t.Errorf("expected byte at index %d to be 0, got %d", i, v)
		}
	}
}

func TestUploaderConfig_Defaults(t *testing.T) {
	if DefaultUploaderConfig.MaxConcurrentProviders != 4 {
		t.Errorf("expected DefaultMaxConcurrentProviders 4, got %d", DefaultUploaderConfig.MaxConcurrentProviders)
	}
}

func setupTestEngine(t *testing.T, dir string) (*engine.ContentEngine, *manifest.Manifest, []byte) {
	if err := storage.NewFSStorage(dir); err != nil {
		t.Fatalf("failed to create store dir: %v", err)
	}
	config := core.EngineConfig{ChunkSize: 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(dir)
	eng := engine.NewContentEngine(config, enc, dig, store, store, keys, store)

	ctx := context.Background()
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest test data: %v", err)
	}

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize manifest: %v", err)
	}

	if err := eng.PutManifestBytes(ctx, m.Descriptor.ID, mBytes); err != nil {
		t.Fatalf("failed to store manifest bytes: %v", err)
	}

	return eng, m, mBytes
}

func TestDistributeWithSemaphoreAndProviders(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "uploader-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	publisherDir := filepath.Join(tmpDir, "publisher")
	eng, m, _ := setupTestEngine(t, publisherDir)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pubHost, _, err := transport.NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("failed to create publisher node: %v", err)
	}
	defer pubHost.Close()
	tPub := transport.NewTransport(pubHost)

	numProviders := 4
	provIDs := make([]peer.ID, numProviders)

	for i := 0; i < numProviders; i++ {
		pDir := filepath.Join(tmpDir, filepath.Base(tmpDir)+"-prov-"+string(rune('0'+i)))
		pEng, _, _ := setupTestEngine(t, pDir)

		pHost, kdht, err := transport.NewNode(ctx, 0, 0, nil, "", false)
		if err != nil {
			t.Fatalf("failed to create provider node %d: %v", i, err)
		}
		defer pHost.Close()
		defer kdht.Close()

		push.NewStreamHandler(pHost, pEng, kdht, true, push.AuthPolicyOpen, nil)
		provIDs[i] = pHost.ID()

		if err := pubHost.Connect(ctx, peer.AddrInfo{ID: pHost.ID(), Addrs: pHost.Addrs()}); err != nil {
			t.Fatalf("failed to connect publisher to provider %d: %v", i, err)
		}
	}

	plan, err := PlanPlacement(m, provIDs, 2)
	if err != nil {
		t.Fatalf("failed to plan placement: %v", err)
	}

	tracker := NewGlobalReplicaTracker(2)
	cfg := UploaderConfig{
		MaxRetriesPerChunk:     2,
		FailoverRounds:         1,
		MaxConcurrentProviders: 2, // Limit concurrency to 2 providers
	}

	err = Distribute(ctx, tPub, eng, plan, tracker, cfg)
	if err != nil {
		t.Fatalf("Distribute failed: %v", err)
	}

	if !tracker.IsComplete(m.ChunkIDs) {
		t.Errorf("expected global tracker to be complete")
	}
}

func TestDistributeContextCancellation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "uploader-cancel-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	eng, m, _ := setupTestEngine(t, tmpDir)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	pubHost, _, err := transport.NewNode(context.Background(), 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("failed to create publisher node: %v", err)
	}
	defer pubHost.Close()
	tPub := transport.NewTransport(pubHost)

	dummyPeer := peer.ID("dummy-peer-id")
	plan, err := PlanPlacement(m, []peer.ID{dummyPeer}, 1)
	if err != nil {
		t.Fatalf("failed to plan placement: %v", err)
	}

	tracker := NewGlobalReplicaTracker(1)
	cfg := UploaderConfig{
		MaxRetriesPerChunk:     1,
		FailoverRounds:         1,
		MaxConcurrentProviders: 1,
	}

	err = Distribute(ctx, tPub, eng, plan, tracker, cfg)
	if err == nil {
		t.Errorf("expected error on cancelled context, got nil")
	}
}
