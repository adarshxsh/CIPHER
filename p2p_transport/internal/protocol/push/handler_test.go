package push

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol"
)

func setupTestPushHandler(t *testing.T) (*StreamHandler, *Client, func()) {
	ctx, cancel := context.WithCancel(context.Background())

	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		cancel()
		t.Fatalf("failed to create h1: %v", err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		cancel()
		t.Fatalf("failed to create h2: %v", err)
	}

	if err := mn.LinkAll(); err != nil {
		cancel()
		t.Fatalf("failed to link peers: %v", err)
	}

	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	st := storage.NewFSStore(t.TempDir())
	eng := engine.NewContentEngine(config, enc, dig, st, st, keys, st)

	handler := NewStreamHandler(h1, eng, nil, true, AuthPolicyOpen, nil)

	stream, err := h2.NewStream(ctx, h1.ID(), protocol.PushTransportProtocolID)
	if err != nil {
		h1.Close()
		h2.Close()
		cancel()
		t.Fatalf("failed to open push stream: %v", err)
	}

	client := &Client{
		stream: stream,
		peerID: h1.ID(),
	}

	cleanup := func() {
		client.Close()
		h1.Close()
		h2.Close()
		cancel()
	}

	return handler, client, cleanup
}

func TestHandlePushManifestValidation(t *testing.T) {
	t.Run("invalid empty manifest chunk count", func(t *testing.T) {
		_, client, cleanup := setupTestPushHandler(t)
		defer cleanup()

		var contentID core.ContentID
		contentID[0] = 0x11

		// Manifest with empty ChunkIDs
		m := &manifest.Manifest{
			Version: 1,
			Descriptor: manifest.ContentDescriptor{
				ID:   contentID,
				Type: manifest.TypeFile,
				Size: 100,
			},
			ChunkIDs: []core.ChunkID{},
		}
		manifestBytes, err := m.Serialize()
		if err != nil {
			t.Fatalf("Serialize failed: %v", err)
		}

		err = client.SendManifest(context.Background(), contentID, nil, manifestBytes)
		if err == nil {
			t.Fatal("expected error for empty manifest chunk count, got nil")
		}
	})

	t.Run("assigned chunk count exceeds manifest chunk count", func(t *testing.T) {
		_, client, cleanup := setupTestPushHandler(t)
		defer cleanup()

		var contentID core.ContentID
		contentID[0] = 0x22

		var cid1, cid2 core.ChunkID
		cid1[0] = 0xA1
		cid2[0] = 0xA2

		m := &manifest.Manifest{
			Version: 1,
			Descriptor: manifest.ContentDescriptor{
				ID:   contentID,
				Type: manifest.TypeFile,
				Size: 100,
			},
			ChunkIDs: []core.ChunkID{cid1},
		}
		manifestBytes, err := m.Serialize()
		if err != nil {
			t.Fatalf("Serialize failed: %v", err)
		}

		// Assigned count (2) > Manifest count (1)
		err = client.SendManifest(context.Background(), contentID, []core.ChunkID{cid1, cid2}, manifestBytes)
		if err == nil {
			t.Fatal("expected error when assigned chunk count exceeds manifest chunk count, got nil")
		}
	})

	t.Run("assigned chunk not present in manifest", func(t *testing.T) {
		_, client, cleanup := setupTestPushHandler(t)
		defer cleanup()

		var contentID core.ContentID
		contentID[0] = 0x33

		var cid1, cid2 core.ChunkID
		cid1[0] = 0xB1
		cid2[0] = 0xB2

		m := &manifest.Manifest{
			Version: 1,
			Descriptor: manifest.ContentDescriptor{
				ID:   contentID,
				Type: manifest.TypeFile,
				Size: 100,
			},
			ChunkIDs: []core.ChunkID{cid1},
		}
		manifestBytes, err := m.Serialize()
		if err != nil {
			t.Fatalf("Serialize failed: %v", err)
		}

		// Assigned cid2 is not in m.ChunkIDs
		err = client.SendManifest(context.Background(), contentID, []core.ChunkID{cid2}, manifestBytes)
		if err == nil {
			t.Fatal("expected error when assigned chunk is not present in manifest, got nil")
		}
	})

	t.Run("valid push manifest setup", func(t *testing.T) {
		_, client, cleanup := setupTestPushHandler(t)
		defer cleanup()

		var contentID core.ContentID
		contentID[0] = 0x44

		var cid1 core.ChunkID
		cid1[0] = 0xC1

		m := &manifest.Manifest{
			Version: 1,
			Descriptor: manifest.ContentDescriptor{
				ID:   contentID,
				Type: manifest.TypeFile,
				Size: 100,
			},
			ChunkIDs: []core.ChunkID{cid1},
		}
		manifestBytes, err := m.Serialize()
		if err != nil {
			t.Fatalf("Serialize failed: %v", err)
		}

		err = client.SendManifest(context.Background(), contentID, []core.ChunkID{cid1}, manifestBytes)
		if err != nil {
			t.Fatalf("expected SendManifest to succeed, got: %v", err)
		}
	})
}
