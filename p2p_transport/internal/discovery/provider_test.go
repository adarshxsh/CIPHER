package discovery

import (
	"cipher/internal/content/core"
	"cipher/internal/identity"
	"context"
	"strings"
	"testing"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/multiformats/go-multiaddr"
	"github.com/libp2p/go-libp2p"
)

func TestSignedProvideSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	priv1, err := identity.GenerateEphemeral()
	if err != nil {
		t.Fatalf("Failed to generate privKey 1: %v", err)
	}
	priv2, err := identity.GenerateEphemeral()
	if err != nil {
		t.Fatalf("Failed to generate privKey 2: %v", err)
	}

	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.Identity(priv1))
	if err != nil {
		t.Fatalf("Failed to create host 1: %v", err)
	}
	defer h1.Close()

	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.Identity(priv2))
	if err != nil {
		t.Fatalf("Failed to create host 2: %v", err)
	}
	defer h2.Close()

	dht1, err := NewDHT(h1, dht.ModeServer)
	if err != nil {
		t.Fatalf("Failed to create DHT 1: %v", err)
	}
	defer dht1.Close()

	dht2, err := NewDHT(h2, dht.ModeServer)
	if err != nil {
		t.Fatalf("Failed to create DHT 2: %v", err)
	}
	defer dht2.Close()

	// Connect h1 and h2
	h2AddrInfo := peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}
	if err := h1.Connect(ctx, h2AddrInfo); err != nil {
		t.Fatalf("Failed to connect h1 to h2: %v", err)
	}
	h1.Peerstore().AddAddrs(h2.ID(), h2.Addrs(), peerstore.PermanentAddrTTL)
	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), peerstore.PermanentAddrTTL)

	var id core.ContentID
	for i := range id {
		id[i] = byte(i + 1)
	}

	// Announce signed provide from h1
	if err := Provide(ctx, dht1, h1, priv1, id); err != nil {
		t.Fatalf("Signed Provide failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Verify h2 can find provider h1
	providers, err := FindProviders(ctx, dht2, id, 5)
	if err != nil {
		t.Fatalf("FindProviders failed: %v", err)
	}

	found := false
	for _, p := range providers {
		if p.ID == h1.ID() {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("Expected provider %s to be found by h2, got providers: %v", h1.ID(), providers)
	}
}

func TestUnsignedProvideRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create host: %v", err)
	}
	defer h.Close()

	dhtNode, err := NewDHT(h, dht.ModeServer)
	if err != nil {
		t.Fatalf("Failed to create DHT: %v", err)
	}
	defer dhtNode.Close()

	var id core.ContentID
	copy(id[:], []byte("unsigned-content-id-1234567890"))

	// Provide without private key (priv = nil)
	err = Provide(ctx, dhtNode, h, nil, id)
	if err == nil {
		t.Fatalf("Expected unsigned Provide to fail, but it succeeded")
	}

	if !strings.Contains(err.Error(), "authorization error") {
		t.Fatalf("Expected authorization error, got: %v", err)
	}

	// Verify no provider record was stored
	cid, _ := contentIDToCID(id)
	provs, err := dhtNode.ProviderStore().GetProviders(ctx, cid.Hash())
	if err != nil {
		t.Fatalf("GetProviders error: %v", err)
	}
	if len(provs) > 0 {
		t.Fatalf("Expected zero provider records stored for unsigned call, got %d", len(provs))
	}
}

func TestInvalidSignatureProvideRejected(t *testing.T) {
	priv1, err := identity.GenerateEphemeral()
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.Identity(priv1))
	if err != nil {
		t.Fatalf("Failed to create host: %v", err)
	}
	defer h.Close()

	var id core.ContentID
	copy(id[:], []byte("forged-content-id-123456789012"))

	rec, err := NewSignedProviderRecord(id, h.ID(), h.Addrs(), priv1)
	if err != nil {
		t.Fatalf("Failed to create record: %v", err)
	}

	// Tamper signature
	rec.Signature[0] ^= 0xFF
	rec.Signature[1] ^= 0xFF

	err = VerifySignedProviderRecord(rec)
	if err == nil {
		t.Fatalf("Expected tampered signature verification to fail, but it passed")
	}

	if !strings.Contains(err.Error(), "authorization error") {
		t.Fatalf("Expected authorization error for tampered signature, got: %v", err)
	}

	// Tamper peer ID
	rec2, err := NewSignedProviderRecord(id, h.ID(), h.Addrs(), priv1)
	if err != nil {
		t.Fatalf("Failed to create record: %v", err)
	}
	rec2.PeerID = peer.ID("Q123456789OtherPeer")

	err = VerifySignedProviderRecord(rec2)
	if err == nil {
		t.Fatalf("Expected peer ID mismatch verification to fail, but it passed")
	}
	if !strings.Contains(err.Error(), "authorization error") {
		t.Fatalf("Expected authorization error for peer ID mismatch, got: %v", err)
	}
}

func TestValidationPerformance(t *testing.T) {
	priv, err := identity.GenerateEphemeral()
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	maddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
	var id core.ContentID
	copy(id[:], []byte("perf-test-content-id-123456789"))

	peerID, _ := peer.IDFromPrivateKey(priv)
	rec, err := NewSignedProviderRecord(id, peerID, []multiaddr.Multiaddr{maddr}, priv)
	if err != nil {
		t.Fatalf("Failed to create record: %v", err)
	}

	start := time.Now()
	iterations := 100
	for i := 0; i < iterations; i++ {
		if err := VerifySignedProviderRecord(rec); err != nil {
			t.Fatalf("Verify failed at iteration %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	avgDuration := elapsed / time.Duration(iterations)

	t.Logf("Average validation duration over %d runs: %v", iterations, avgDuration)

	if avgDuration > 10*time.Millisecond {
		t.Fatalf("Validation duration %v exceeded 10ms threshold", avgDuration)
	}
}
