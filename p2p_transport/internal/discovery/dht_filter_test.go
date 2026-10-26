package discovery

import (
	"context"
	"testing"
	"time"

	"cipher/internal/content/core"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	bhost "github.com/libp2p/go-libp2p/p2p/host/blank"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"
	ma "github.com/multiformats/go-multiaddr"
)

func makeTestHost(t *testing.T) host.Host {
	s := swarmt.GenSwarm(t)
	return bhost.NewBlankHost(s)
}

func TestDHTAddressFilteringAndProviderPoisoningProtection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h1 := makeTestHost(t)
	defer h1.Close()

	h2 := makeTestHost(t)
	defer h2.Close()

	dht1, err := NewDHT(h1, dht.ModeServer)
	if err != nil {
		t.Fatalf("Failed to create DHT1: %v", err)
	}
	defer dht1.Close()

	dht2, err := NewDHT(h2, dht.ModeServer)
	if err != nil {
		t.Fatalf("Failed to create DHT2: %v", err)
	}
	defer dht2.Close()

	// Connect h2 to h1
	if err := h2.Connect(ctx, peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}); err != nil {
		t.Fatalf("Failed to connect h2 to h1: %v", err)
	}

	// Ensure peers are in each other's routing tables
	if _, err := dht1.RoutingTable().TryAddPeer(h2.ID(), true, false); err != nil {
		t.Fatalf("Failed to add h2 to dht1 routing table: %v", err)
	}
	if _, err := dht2.RoutingTable().TryAddPeer(h1.ID(), true, false); err != nil {
		t.Fatalf("Failed to add h1 to dht2 routing table: %v", err)
	}

	// Bootstrap DHTs
	if err := dht1.Bootstrap(ctx); err != nil {
		t.Fatalf("DHT1 bootstrap failed: %v", err)
	}
	if err := dht2.Bootstrap(ctx); err != nil {
		t.Fatalf("DHT2 bootstrap failed: %v", err)
	}

	// Inject poisoned/invalid addresses into peerstore for h2
	poisonedAddr1, _ := ma.NewMultiaddr("/ip4/0.0.0.0/tcp/9999")
	poisonedAddr2, _ := ma.NewMultiaddr("/ip4/224.0.0.1/tcp/8888")
	h2.Peerstore().AddAddrs(h2.ID(), []ma.Multiaddr{poisonedAddr1, poisonedAddr2}, time.Hour)

	// Announce content from h2
	var cidBytes [32]byte
	copy(cidBytes[:], []byte("test-content-id-12345678901234567890"))
	contentID := core.ContentID(cidBytes)

	if err := Provide(ctx, dht2, contentID); err != nil {
		t.Fatalf("Provide failed: %v", err)
	}

	// Find providers from h1
	providers, err := FindProviders(ctx, dht1, contentID, 10)
	if err != nil {
		t.Fatalf("FindProviders failed: %v", err)
	}

	for _, p := range providers {
		for _, addr := range p.Addrs {
			if err := ValidateMultiaddr(addr, DefaultFilterOptions); err != nil {
				t.Errorf("FindProviders returned invalid/poisoned address %s: %v", addr.String(), err)
			}
		}
	}
}
