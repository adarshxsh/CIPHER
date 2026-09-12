package retrieval_test

import (
	"context"
	"testing"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/chunk"
	"cipher/internal/retrieval"
	"cipher/internal/transport"
)

func TestResolveManifest_SecurityFailover(t *testing.T) {
	mn := mocknet.New()

	// Peer 1: Rogue provider (tampered signature)
	p1, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	// Peer 2: Legitimate provider (valid signed attestation)
	p2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	// Client node
	clientPeer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}

	dir1 := t.TempDir()
	dir2 := t.TempDir()
	dirClient := t.TempDir()

	store1 := storage.NewFSStore(dir1)
	store2 := storage.NewFSStore(dir2)
	storeClient := storage.NewFSStore(dirClient)

	config := core.EngineConfig{ChunkSize: 32 * 1024}
	eng1 := engine.NewContentEngine(config, crypto.NewChaCha20Encryptor(), verifier.NewSHA256Digest(), store1, store1, engine.NewLocalKeyProvider(), store1)
	eng2 := engine.NewContentEngine(config, crypto.NewChaCha20Encryptor(), verifier.NewSHA256Digest(), store2, store2, engine.NewLocalKeyProvider(), store2)
	engClient := engine.NewContentEngine(config, crypto.NewChaCha20Encryptor(), verifier.NewSHA256Digest(), storeClient, storeClient, engine.NewLocalKeyProvider(), storeClient)

	ctx := context.Background()

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{0x77},
			Type: manifest.TypeFile,
			Size: 200,
		},
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)
	eng2.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// p1 returns tampered signature
	priv1 := p1.Peerstore().PrivKey(p1.ID())
	p1.SetStreamHandler("/cipher/chunk/1.0.0", func(s network.Stream) {
		defer s.Close()
		msg, _ := chunk.ReadMessage(s)
		if msg.Type == chunk.MsgRequestManifest {
			pubKeyBytes, _ := libp2pcrypto.MarshalPublicKey(priv1.GetPublic())
			att := &chunk.ProviderAttestation{
				ContentID:      m.Descriptor.ID,
				ProviderID:     p1.ID().String(),
				ProviderPubKey: pubKeyBytes,
				Timestamp:      100, // Invalid old timestamp
			}
			att.Sign(priv1)
			resp, _ := chunk.BuildManifest(m.Descriptor.ID, att, mBytes)
			chunk.WriteMessage(s, resp)
		}
	})

	// p2 is valid provider using StreamHandler
	chunk.NewStreamHandler(p2, eng2)

	clientTrans := transport.NewTransport(clientPeer)

	providers := []peer.ID{p1.ID(), p2.ID()}

	resolvedManifest, err := retrieval.ResolveManifest(ctx, m.Descriptor.ID, nil, clientTrans, engClient, providers)
	if err != nil {
		t.Fatalf("expected ResolveManifest to failover to legitimate provider p2, got error: %v", err)
	}

	if resolvedManifest.Descriptor.ID != m.Descriptor.ID {
		t.Errorf("expected ContentID %x, got %x", m.Descriptor.ID, resolvedManifest.Descriptor.ID)
	}
}
