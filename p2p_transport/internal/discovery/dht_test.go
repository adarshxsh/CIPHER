package discovery

import (
	"os"
	"testing"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func TestDHTAddressFiltering_WANMode(t *testing.T) {
	os.Unsetenv("CIPHER_ALLOW_LOCAL_IP")
	os.Unsetenv("CIPHER_ALLOW_PRIVATE_DHT")

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

	// Create a dummy peer ID and loopback multiaddr
	dummyPID, err := peer.Decode("12D3KooWSoLju1RrmL3388pM26R3M3cT4L1L1111111111111111")
	if err != nil {
		t.Fatalf("failed to decode peer ID: %v", err)
	}

	loopbackAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
	if err != nil {
		t.Fatalf("failed to parse loopback multiaddr: %v", err)
	}

	publicAddr, err := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/4001")
	if err != nil {
		t.Fatalf("failed to parse public multiaddr: %v", err)
	}

	info := peer.AddrInfo{
		ID:    dummyPID,
		Addrs: []ma.Multiaddr{loopbackAddr, publicAddr},
	}
	sanitized := SanitizeAddrInfo(info, false)
	if len(sanitized.Addrs) != 1 {
		t.Fatalf("expected 1 address after WAN filtering, got %d", len(sanitized.Addrs))
	}

	// Filter addresses using PublicAddressFilter
	filtered := PublicAddressFilter([]ma.Multiaddr{loopbackAddr, publicAddr})

	if len(filtered) != 1 {
		t.Fatalf("expected 1 address after WAN filtering, got %d", len(filtered))
	}
	if filtered[0].String() != publicAddr.String() {
		t.Errorf("expected public address %s, got %s", publicAddr, filtered[0])
	}
}

func TestDHTAddressFiltering_LANMode(t *testing.T) {
	os.Setenv("CIPHER_ALLOW_PRIVATE_DHT", "1")
	defer os.Unsetenv("CIPHER_ALLOW_PRIVATE_DHT")

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

	loopbackAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
	if err != nil {
		t.Fatalf("failed to parse loopback multiaddr: %v", err)
	}

	privateAddr, err := ma.NewMultiaddr("/ip4/192.168.1.1/tcp/4001")
	if err != nil {
		t.Fatalf("failed to parse private multiaddr: %v", err)
	}

	unspecifiedAddr, err := ma.NewMultiaddr("/ip4/0.0.0.0/tcp/4001")
	if err != nil {
		t.Fatalf("failed to parse unspecified multiaddr: %v", err)
	}

	// Filter addresses in LAN mode
	filtered := PublicAddressFilter([]ma.Multiaddr{loopbackAddr, privateAddr, unspecifiedAddr})

	if len(filtered) != 2 {
		t.Fatalf("expected 2 addresses after LAN filtering (excluding 0.0.0.0), got %d", len(filtered))
	}
}
