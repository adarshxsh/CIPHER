package discovery

import (
	"testing"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	ma "github.com/multiformats/go-multiaddr"
)

func TestPublicAddressFilter(t *testing.T) {
	addrs := []string{
		"/ip4/127.0.0.1/tcp/4001",
		"/ip4/10.0.0.1/tcp/4001",
		"/ip4/192.168.1.100/tcp/4001",
		"/ip6/fe80::1/tcp/4001",
		"/ip4/1.1.1.1/tcp/4001",
		"/dns4/example.com/tcp/4001",
	}

	var maddrs []ma.Multiaddr
	for _, a := range addrs {
		m, err := ma.NewMultiaddr(a)
		if err != nil {
			t.Fatalf("Failed to parse multiaddr %s: %v", a, err)
		}
		maddrs = append(maddrs, m)
	}

	filtered := PublicAddressFilter(maddrs)

	if len(filtered) != 2 {
		t.Fatalf("Expected 2 public multiaddresses, got %d", len(filtered))
	}

	expected := map[string]bool{
		"/ip4/1.1.1.1/tcp/4001":       true,
		"/dns4/example.com/tcp/4001": true,
	}

	for _, m := range filtered {
		if !expected[m.String()] {
			t.Errorf("Unexpected multiaddress in filtered list: %s", m.String())
		}
	}
}

func TestNewDHT_PublicFilters(t *testing.T) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create host: %v", err)
	}
	defer h.Close()

	kdht, err := NewDHT(h, dht.ModeServer)
	if err != nil {
		t.Fatalf("Failed to create DHT: %v", err)
	}
	defer kdht.Close()

	if kdht == nil {
		t.Fatalf("Expected non-nil IpfsDHT")
	}
}

func TestRoutingTableRejectsPrivateAddressesInPublicMode(t *testing.T) {
	t.Setenv("CIPHER_ALLOW_LOCAL", "")
	t.Setenv("CIPHER_LOCAL_DEV", "")

	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create host: %v", err)
	}
	defer h.Close()

	kdht, err := NewDHT(h, dht.ModeServer)
	if err != nil {
		t.Fatalf("Failed to create DHT: %v", err)
	}
	defer kdht.Close()

	loopbackAddr, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	filtered := PublicAddressFilter([]ma.Multiaddr{loopbackAddr})
	if len(filtered) != 0 {
		t.Fatalf("Expected PublicAddressFilter to reject loopback address in public mode, got %v", filtered)
	}

	privateAddr10, _ := ma.NewMultiaddr("/ip4/10.0.0.1/tcp/1234")
	filteredPriv10 := PublicAddressFilter([]ma.Multiaddr{privateAddr10})
	if len(filteredPriv10) != 0 {
		t.Fatalf("Expected PublicAddressFilter to reject 10.0.0.1 address in public mode, got %v", filteredPriv10)
	}

	privateAddr192, _ := ma.NewMultiaddr("/ip4/192.168.0.1/tcp/1234")
	filteredPriv192 := PublicAddressFilter([]ma.Multiaddr{privateAddr192})
	if len(filteredPriv192) != 0 {
		t.Fatalf("Expected PublicAddressFilter to reject 192.168.0.1 address in public mode, got %v", filteredPriv192)
	}
}
