package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func TestNewDHTAddressFilter(t *testing.T) {
	t.Setenv("CIPHER_ALLOW_PRIVATE_DHT", "")
	t.Setenv("CIPHER_ALLOW_LOCAL_IP", "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	kdht, err := NewDHT(h, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create DHT: %v", err)
	}
	defer kdht.Close()

	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}
	dummyID, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("failed to get peer ID from private key: %v", err)
	}

	publicAddr, _ := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/1234")
	privateAddr, _ := ma.NewMultiaddr("/ip4/10.0.0.1/tcp/1234")

	// Add multiaddrs to peerstore
	h.Peerstore().AddAddrs(dummyID, []ma.Multiaddr{publicAddr, privateAddr}, time.Hour)

	// Addresses in peerstore are stored, but AddressFilter on DHT filters addresses before adding to routing table or peerstore operations
	addrs := h.Peerstore().Addrs(dummyID)
	if len(addrs) == 0 {
		t.Errorf("Expected peerstore to contain addresses")
	}

	// Verify PublicAddressFilter directly rejects private address
	filtered := PublicAddressFilter([]ma.Multiaddr{publicAddr, privateAddr})
	if len(filtered) != 1 || !filtered[0].Equal(publicAddr) {
		t.Errorf("PublicAddressFilter should keep publicAddr and filter privateAddr, got %v", filtered)
	}

	_ = ctx
}

func TestNewDHTWithCustomOptions(t *testing.T) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	kdht, err := NewDHT(h, dht.ModeServer, dht.Concurrency(5))
	if err != nil {
		t.Fatalf("failed to create DHT with custom options: %v", err)
	}
	defer kdht.Close()
}
