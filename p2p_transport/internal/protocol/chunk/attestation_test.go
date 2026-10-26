package chunk_test

import (
	"context"
	"testing"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestProviderAttestation_SigningAndVerification(t *testing.T) {
	priv, pub, err := libp2pcrypto.GenerateKeyPair(libp2pcrypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}

	peerID, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("failed to derive peer ID: %v", err)
	}

	pubKeyBytes, err := libp2pcrypto.MarshalPublicKey(pub)
	if err != nil {
		t.Fatalf("failed to marshal public key: %v", err)
	}

	var contentID core.ContentID
	contentID[0] = 0x12

	att := &chunk.ProviderAttestation{
		ContentID:      contentID,
		ProviderID:     peerID.String(),
		ProviderPubKey: pubKeyBytes,
		Timestamp:      time.Now().Unix(),
	}

	if err := att.Sign(priv); err != nil {
		t.Fatalf("Sign failed: %v", err)
	}

	if err := att.Verify(pub); err != nil {
		t.Fatalf("Verify failed: %v", err)
	}

	// Test tampered signature
	att.Signature[0] ^= 0xFF
	if err := att.Verify(pub); err == nil {
		t.Fatalf("expected error on tampered signature, got nil")
	}
}

func setupMockPair(t testing.TB) (host.Host, host.Host) {
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
	return h1, h2
}

func TestClientResolve_ValidAttestation(t *testing.T) {
	h1, h2 := setupMockPair(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{0x01},
			Type: manifest.TypeFile,
			Size: 100,
		},
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	resolvedData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	resolvedManifest, err := manifest.Deserialize(resolvedData)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}
	if resolvedManifest.Descriptor.ID != m.Descriptor.ID {
		t.Errorf("ContentID mismatch: got %x, expected %x", resolvedManifest.Descriptor.ID, m.Descriptor.ID)
	}
}

func TestClientResolve_ResolutionLatencyGuardrail(t *testing.T) {
	h1, h2 := setupMockPair(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   core.ContentID{0x05},
			Type: manifest.TypeFile,
			Size: 500,
		},
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	start := time.Now()
	_, err = client.Resolve(ctx, m.Descriptor.ID)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	t.Logf("Manifest resolution latency: %v", elapsed)
}

func TestClientResolve_RejectsSpoofedProviderID(t *testing.T) {
	h1, h2 := setupMockPair(t)
	eng2 := createTestEngine(t)

	ctx := context.Background()
	var contentID core.ContentID
	contentID[0] = 0x09

	priv1 := h1.Peerstore().PrivKey(h1.ID())

	h1.SetStreamHandler("/cipher/chunk/1.0.0", func(s network.Stream) {
		defer s.Close()
		msg, _ := chunk.ReadMessage(s)
		if msg.Type == chunk.MsgRequestManifest {
			pubKeyBytes, _ := libp2pcrypto.MarshalPublicKey(priv1.GetPublic())
			fakePeerID := "QmYwAPJzv5CZsnA625s3X2nemtYgPpHdWEz79ojWnPbdG1"
			att := &chunk.ProviderAttestation{
				ContentID:      contentID,
				ProviderID:     fakePeerID,
				ProviderPubKey: pubKeyBytes,
				Timestamp:      time.Now().Unix(),
			}
			att.Sign(priv1)
			resp, _ := chunk.BuildManifest(contentID, att, []byte("fake manifest"))
			chunk.WriteMessage(s, resp)
		}
	})

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	_, err = client.Resolve(ctx, contentID)
	if err == nil {
		t.Fatalf("expected Resolve to fail on spoofed ProviderID, got nil")
	}
}

func TestClientResolve_RejectsExpiredTimestamp(t *testing.T) {
	h1, h2 := setupMockPair(t)
	eng2 := createTestEngine(t)

	ctx := context.Background()
	var contentID core.ContentID
	contentID[0] = 0x0A

	priv1 := h1.Peerstore().PrivKey(h1.ID())

	h1.SetStreamHandler("/cipher/chunk/1.0.0", func(s network.Stream) {
		defer s.Close()
		msg, _ := chunk.ReadMessage(s)
		if msg.Type == chunk.MsgRequestManifest {
			pubKeyBytes, _ := libp2pcrypto.MarshalPublicKey(priv1.GetPublic())
			att := &chunk.ProviderAttestation{
				ContentID:      contentID,
				ProviderID:     h1.ID().String(),
				ProviderPubKey: pubKeyBytes,
				Timestamp:      time.Now().Unix() - 400,
			}
			att.Sign(priv1)
			resp, _ := chunk.BuildManifest(contentID, att, []byte("fake manifest"))
			chunk.WriteMessage(s, resp)
		}
	})

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	_, err = client.Resolve(ctx, contentID)
	if err == nil {
		t.Fatalf("expected Resolve to fail on timestamp skew > 300s, got nil")
	}
}

func TestClientResolve_RejectsTamperedSignature(t *testing.T) {
	h1, h2 := setupMockPair(t)
	eng2 := createTestEngine(t)

	ctx := context.Background()
	var contentID core.ContentID
	contentID[0] = 0x0B

	priv1 := h1.Peerstore().PrivKey(h1.ID())

	h1.SetStreamHandler("/cipher/chunk/1.0.0", func(s network.Stream) {
		defer s.Close()
		msg, _ := chunk.ReadMessage(s)
		if msg.Type == chunk.MsgRequestManifest {
			pubKeyBytes, _ := libp2pcrypto.MarshalPublicKey(priv1.GetPublic())
			att := &chunk.ProviderAttestation{
				ContentID:      contentID,
				ProviderID:     h1.ID().String(),
				ProviderPubKey: pubKeyBytes,
				Timestamp:      time.Now().Unix(),
			}
			att.Sign(priv1)
			att.Signature[0] ^= 0xFF
			resp, _ := chunk.BuildManifest(contentID, att, []byte("fake manifest"))
			chunk.WriteMessage(s, resp)
		}
	})

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	_, err = client.Resolve(ctx, contentID)
	if err == nil {
		t.Fatalf("expected Resolve to fail on tampered attestation signature, got nil")
	}
}

func TestClientResolve_RejectsMissingAttestation(t *testing.T) {
	h1, h2 := setupMockPair(t)
	eng2 := createTestEngine(t)

	ctx := context.Background()
	var contentID core.ContentID
	contentID[0] = 0x0C

	h1.SetStreamHandler("/cipher/chunk/1.0.0", func(s network.Stream) {
		defer s.Close()
		msg, _ := chunk.ReadMessage(s)
		if msg.Type == chunk.MsgRequestManifest {
			resp, _ := chunk.BuildManifest(contentID, nil, []byte("unauthenticated manifest"))
			chunk.WriteMessage(s, resp)
		}
	})

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	_, err = client.Resolve(ctx, contentID)
	if err == nil {
		t.Fatalf("expected Resolve to fail on missing attestation, got nil")
	}
}
