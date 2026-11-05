package discovery

import (
	"context"
	"os"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func parseAddr(t *testing.T, s string) ma.Multiaddr {
	t.Helper()
	addr, err := ma.NewMultiaddr(s)
	if err != nil {
		t.Fatalf("failed to parse multiaddr %s: %v", s, err)
	}
	return addr
}

func TestIsRoutableAddress_WANMode(t *testing.T) {
	tests := []struct {
		addr     string
		routable bool
	}{
		{"/ip4/127.0.0.1/tcp/4001", false},
		{"/ip6/::1/tcp/4001", false},
		{"/ip4/192.168.1.100/tcp/4001", false},
		{"/ip4/10.0.0.1/tcp/4001", false},
		{"/ip4/172.16.0.1/tcp/4001", false},
		{"/ip4/0.0.0.0/tcp/4001", false},
		{"/ip6/::/tcp/4001", false},
		{"/ip4/1.2.3.4/tcp/4001", true},
		{"/ip4/8.8.8.8/tcp/4001", true},
		{"/dns4/example.com/tcp/4001", true},
		{"/dns4/localhost/tcp/4001", false},
	}

	for _, tt := range tests {
		maddr := parseAddr(t, tt.addr)
		got := IsRoutableAddress(maddr, false)
		if got != tt.routable {
			t.Errorf("IsRoutableAddress(%s, allowPrivate=false) = %v; want %v", tt.addr, got, tt.routable)
		}
	}
}

func TestIsRoutableAddress_LANMode(t *testing.T) {
	tests := []struct {
		addr     string
		routable bool
	}{
		{"/ip4/127.0.0.1/tcp/4001", true},
		{"/ip6/::1/tcp/4001", true},
		{"/ip4/192.168.1.100/tcp/4001", true},
		{"/ip4/10.0.0.1/tcp/4001", true},
		{"/ip4/172.16.0.1/tcp/4001", true},
		{"/ip4/0.0.0.0/tcp/4001", false}, // unspecified rejected in LAN mode as well
		{"/ip6/::/tcp/4001", false},     // unspecified rejected in LAN mode as well
		{"/ip4/1.2.3.4/tcp/4001", true},
	}

	for _, tt := range tests {
		maddr := parseAddr(t, tt.addr)
		got := IsRoutableAddress(maddr, true)
		if got != tt.routable {
			t.Errorf("IsRoutableAddress(%s, allowPrivate=true) = %v; want %v", tt.addr, got, tt.routable)
		}
	}
}

func TestIsAllowPrivateDHT(t *testing.T) {
	os.Unsetenv("CIPHER_ALLOW_LOCAL_IP")
	os.Unsetenv("CIPHER_ALLOW_PRIVATE_DHT")
	if IsAllowPrivateDHT() {
		t.Errorf("expected IsAllowPrivateDHT() false when env vars unset")
	}

	os.Setenv("CIPHER_ALLOW_LOCAL_IP", "1")
	if !IsAllowPrivateDHT() {
		t.Errorf("expected IsAllowPrivateDHT() true when CIPHER_ALLOW_LOCAL_IP=1")
	}
	os.Unsetenv("CIPHER_ALLOW_LOCAL_IP")

	os.Setenv("CIPHER_ALLOW_PRIVATE_DHT", "1")
	if !IsAllowPrivateDHT() {
		t.Errorf("expected IsAllowPrivateDHT() true when CIPHER_ALLOW_PRIVATE_DHT=1")
	}
	os.Unsetenv("CIPHER_ALLOW_PRIVATE_DHT")
}

func TestFilterAddresses(t *testing.T) {
	addrs := []ma.Multiaddr{
		parseAddr(t, "/ip4/127.0.0.1/tcp/4001"),
		parseAddr(t, "/ip4/192.168.1.1/tcp/4001"),
		parseAddr(t, "/ip4/1.2.3.4/tcp/4001"),
	}

	// In WAN mode (allowPrivate=false), only 1.2.3.4 should remain
	filteredWAN := FilterAddresses(addrs, false)
	if len(filteredWAN) != 1 {
		t.Fatalf("expected 1 WAN address, got %d", len(filteredWAN))
	}
	if filteredWAN[0].String() != "/ip4/1.2.3.4/tcp/4001" {
		t.Errorf("unexpected WAN filtered address: %s", filteredWAN[0])
	}

	// In LAN mode (allowPrivate=true), all 3 should remain
	filteredLAN := FilterAddresses(addrs, true)
	if len(filteredLAN) != 3 {
		t.Fatalf("expected 3 LAN addresses, got %d", len(filteredLAN))
	}
}

func TestSanitizeAddrInfo(t *testing.T) {
	pID, err := peer.Decode("12D3KooWSoLju1RrmL3388pM26R3M3cT4L1L1111111111111111")
	if err != nil {
		t.Skipf("skipping peer decode if test string invalid: %v", err)
	}

	info := peer.AddrInfo{
		ID: pID,
		Addrs: []ma.Multiaddr{
			parseAddr(t, "/ip4/127.0.0.1/tcp/4001"),
			parseAddr(t, "/ip4/93.184.216.34/tcp/4001"),
		},
	}

	sanitized := SanitizeAddrInfo(info, false)
	if len(sanitized.Addrs) != 1 {
		t.Fatalf("expected 1 sanitized address, got %d", len(sanitized.Addrs))
	}
	if sanitized.Addrs[0].String() != "/ip4/93.184.216.34/tcp/4001" {
		t.Errorf("unexpected address: %s", sanitized.Addrs[0])
	}
}

func TestSanitizeProviderChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan peer.AddrInfo, 2)

	p1 := peer.AddrInfo{
		ID: "peer1",
		Addrs: []ma.Multiaddr{
			parseAddr(t, "/ip4/127.0.0.1/tcp/4001"), // loopback only
		},
	}
	p2 := peer.AddrInfo{
		ID: "peer2",
		Addrs: []ma.Multiaddr{
			parseAddr(t, "/ip4/1.2.3.4/tcp/4001"), // public
		},
	}

	in <- p1
	in <- p2
	close(in)

	outWAN := SanitizeProviderChannel(ctx, in, false)
	var results []peer.AddrInfo
	for p := range outWAN {
		results = append(results, p)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 provider in WAN mode, got %d", len(results))
	}
	if results[0].ID != "peer2" {
		t.Errorf("expected provider peer2, got %s", results[0].ID)
	}
}
