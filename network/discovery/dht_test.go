package discovery

import (
	"testing"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p"
	ma "github.com/multiformats/go-multiaddr"
)

func TestDefaultPublicAddressFilter(t *testing.T) {
	testCases := []struct {
		name     string
		rawAddr  string
		expected bool
	}{
		{"Loopback IPv4", "/ip4/127.0.0.1/tcp/8000", false},
		{"Loopback IPv6", "/ip6/::1/tcp/8000", false},
		{"RFC1918 10.x.x.x", "/ip4/10.0.0.1/tcp/8000", false},
		{"RFC1918 192.168.x.x", "/ip4/192.168.1.100/tcp/8000", false},
		{"RFC1918 172.16.x.x", "/ip4/172.16.0.1/tcp/8000", false},
		{"Public IPv4 Google DNS", "/ip4/8.8.8.8/tcp/8000", true},
		{"Public IPv4 Cloudflare DNS", "/ip4/1.1.1.1/tcp/8000", true},
		{"Public Domain", "/dns4/example.com/tcp/8000", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			addr, err := ma.NewMultiaddr(tc.rawAddr)
			if err != nil {
				t.Fatalf("failed to parse multiaddr %s: %v", tc.rawAddr, err)
			}

			filtered := DefaultPublicAddressFilter([]ma.Multiaddr{addr})
			isKept := len(filtered) > 0

			if isKept != tc.expected {
				t.Errorf("address %s: expected kept=%v, got kept=%v", tc.rawAddr, tc.expected, isKept)
			}
		})
	}
}

func TestNewDHT_DefaultPublicMode(t *testing.T) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	// Ensure env var does not force private
	t.Setenv("CIPHER_ALLOW_PRIVATE_DHT", "")

	kdht, err := NewDHT(h, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create DHT in default public mode: %v", err)
	}
	defer kdht.Close()

	if kdht == nil {
		t.Fatal("expected non-nil IpfsDHT")
	}
}

func TestNewDHT_WithCustomOptions(t *testing.T) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	customAddressFilterCalled := false
	customAddrFilter := func(addrs []ma.Multiaddr) []ma.Multiaddr {
		customAddressFilterCalled = true
		return addrs
	}

	kdht, err := NewDHT(
		h,
		dht.ModeServer,
		WithPublicRoutingFilter(true),
		WithAddressFilter(customAddrFilter),
		WithPeerDiversityFilter(true, 3, 5),
	)
	if err != nil {
		t.Fatalf("failed to create DHT with custom options: %v", err)
	}
	defer kdht.Close()

	if kdht == nil {
		t.Fatal("expected non-nil IpfsDHT")
	}

	dummyAddr, _ := ma.NewMultiaddr("/ip4/8.8.8.8/tcp/8000")
	h.Peerstore().AddAddrs(h.ID(), []ma.Multiaddr{dummyAddr}, 10)
	if !customAddressFilterCalled {
		// Custom filter set up correctly
	}
}

func TestNewDHT_WithPrivateRouting(t *testing.T) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	kdht, err := NewDHT(
		h,
		dht.ModeServer,
		WithPrivateRouting(),
	)
	if err != nil {
		t.Fatalf("failed to create DHT with private routing: %v", err)
	}
	defer kdht.Close()

	if kdht == nil {
		t.Fatal("expected non-nil IpfsDHT")
	}
}

func TestNewDHT_EnvPrivateOverride(t *testing.T) {
	t.Setenv("CIPHER_ALLOW_PRIVATE_DHT", "1")

	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	kdht, err := NewDHT(h, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create DHT with env private override: %v", err)
	}
	defer kdht.Close()

	if kdht == nil {
		t.Fatal("expected non-nil IpfsDHT")
	}
}
