package discovery

import (
	"cipher/internal/content/core"
	"cipher/internal/identity"
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestSignAndVerifyProviderRecord(t *testing.T) {
	priv, err := identity.GenerateEphemeral()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	peerID, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("failed to get peer ID: %v", err)
	}

	var contentID core.ContentID
	copy(contentID[:], []byte("01234567890123456789012345678901"))

	rec, err := SignProviderRecord(priv, contentID, peerID, []string{"/ip4/127.0.0.1/tcp/4001"}, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to sign record: %v", err)
	}

	if err := VerifyProviderRecord(rec); err != nil {
		t.Fatalf("expected record to verify, got: %v", err)
	}
}

func TestVerifyProviderRecord_TamperedContentID(t *testing.T) {
	priv, err := identity.GenerateEphemeral()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	peerID, _ := peer.IDFromPrivateKey(priv)

	var contentID core.ContentID
	copy(contentID[:], []byte("01234567890123456789012345678901"))

	rec, err := SignProviderRecord(priv, contentID, peerID, nil, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to sign record: %v", err)
	}

	// Tamper content ID
	rec.ContentID[0] ^= 0xFF

	if err := VerifyProviderRecord(rec); err == nil {
		t.Fatal("expected verification failure for tampered ContentID, got success")
	}
}

func TestVerifyProviderRecord_TamperedPeerID(t *testing.T) {
	priv1, _ := identity.GenerateEphemeral()
	peerID1, _ := peer.IDFromPrivateKey(priv1)

	priv2, _ := identity.GenerateEphemeral()
	peerID2, _ := peer.IDFromPrivateKey(priv2)

	var contentID core.ContentID
	copy(contentID[:], []byte("01234567890123456789012345678901"))

	// Sign with priv1 for peerID1
	rec, err := SignProviderRecord(priv1, contentID, peerID1, nil, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to sign record: %v", err)
	}

	// Tamper peer ID to peerID2
	rec.PeerID = peerID2

	if err := VerifyProviderRecord(rec); err == nil {
		t.Fatal("expected verification failure for spoofed PeerID, got success")
	}
}

func TestVerifyProviderRecord_TamperedSignature(t *testing.T) {
	priv, _ := identity.GenerateEphemeral()
	peerID, _ := peer.IDFromPrivateKey(priv)

	var contentID core.ContentID
	copy(contentID[:], []byte("01234567890123456789012345678901"))

	rec, err := SignProviderRecord(priv, contentID, peerID, nil, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to sign record: %v", err)
	}

	rec.Signature[0] ^= 0xFF

	if err := VerifyProviderRecord(rec); err == nil {
		t.Fatal("expected verification failure for corrupted signature, got success")
	}
}

func TestVerifyProviderRecord_Expired(t *testing.T) {
	priv, _ := identity.GenerateEphemeral()
	peerID, _ := peer.IDFromPrivateKey(priv)

	var contentID core.ContentID
	copy(contentID[:], []byte("01234567890123456789012345678901"))

	rec, err := SignProviderRecord(priv, contentID, peerID, nil, -1*time.Hour)
	if err != nil {
		t.Fatalf("failed to sign record: %v", err)
	}

	if err := VerifyProviderRecord(rec); err == nil {
		t.Fatal("expected verification failure for expired record, got success")
	}
}

func TestStandaloneProvideAndGetValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	priv1, _ := identity.GenerateEphemeral()
	h1, _ := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.Identity(priv1))
	dht1, err := NewDHT(h1, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht1: %v", err)
	}
	defer h1.Close()
	defer dht1.Close()

	priv2, _ := identity.GenerateEphemeral()
	h2, _ := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.Identity(priv2))
	dht2, err := NewDHT(h2, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht2: %v", err)
	}
	defer h2.Close()
	defer dht2.Close()

	// Connect h2 to h1 and bootstrap
	if err := Bootstrap(ctx, dht1, h1, []peer.AddrInfo{{ID: h2.ID(), Addrs: h2.Addrs()}}); err != nil {
		t.Fatalf("Bootstrap dht1 failed: %v", err)
	}
	if err := Bootstrap(ctx, dht2, h2, []peer.AddrInfo{{ID: h1.ID(), Addrs: h1.Addrs()}}); err != nil {
		t.Fatalf("Bootstrap dht2 failed: %v", err)
	}

	var contentID core.ContentID
	copy(contentID[:], []byte("standalonecontentid1234567890123"))

	// Provide on node 1
	if err := Provide(ctx, dht1, priv1, contentID); err != nil {
		t.Fatalf("Provide failed: %v", err)
	}

	// Now node 2 verifies provider record for node 1
	if err := VerifyProviderForContent(ctx, dht2, contentID, h1.ID()); err != nil {
		t.Fatalf("VerifyProviderForContent failed: %v", err)
	}
}
