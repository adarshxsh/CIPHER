package discovery

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func TestIsRoutableAddressDefault(t *testing.T) {
	// Ensure CIPHER_ALLOW_PRIVATE_DHT and CIPHER_ALLOW_LOCAL_IP are unset
	t.Setenv("CIPHER_ALLOW_PRIVATE_DHT", "")
	t.Setenv("CIPHER_ALLOW_LOCAL_IP", "")

	publicAddr, err := ma.NewMultiaddr("/ip4/8.8.8.8/tcp/1234")
	if err != nil {
		t.Fatalf("failed to parse public multiaddr: %v", err)
	}

	dnsAddr, err := ma.NewMultiaddr("/dns4/example.com/tcp/443")
	if err != nil {
		t.Fatalf("failed to parse dns multiaddr: %v", err)
	}

	privateAddr, err := ma.NewMultiaddr("/ip4/192.168.1.1/tcp/1234")
	if err != nil {
		t.Fatalf("failed to parse private multiaddr: %v", err)
	}

	loopbackAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	if err != nil {
		t.Fatalf("failed to parse loopback multiaddr: %v", err)
	}

	unspecifiedAddr, err := ma.NewMultiaddr("/ip4/0.0.0.0/tcp/1234")
	if err != nil {
		t.Fatalf("failed to parse unspecified multiaddr: %v", err)
	}

	if !IsRoutableAddress(publicAddr) {
		t.Errorf("Expected public address %s to be routable", publicAddr)
	}

	if !IsRoutableAddress(dnsAddr) {
		t.Errorf("Expected DNS address %s to be routable", dnsAddr)
	}

	if IsRoutableAddress(privateAddr) {
		t.Errorf("Expected private address %s to be unroutable in default mode", privateAddr)
	}

	if IsRoutableAddress(loopbackAddr) {
		t.Errorf("Expected loopback address %s to be unroutable in default mode", loopbackAddr)
	}

	if IsRoutableAddress(unspecifiedAddr) {
		t.Errorf("Expected unspecified address %s to be unroutable", unspecifiedAddr)
	}

	if IsRoutableAddress(nil) {
		t.Errorf("Expected nil address to be unroutable")
	}
}

func TestIsRoutableAddressPrivateDHTAllowed(t *testing.T) {
	t.Setenv("CIPHER_ALLOW_PRIVATE_DHT", "1")

	privateAddr, err := ma.NewMultiaddr("/ip4/192.168.1.1/tcp/1234")
	if err != nil {
		t.Fatalf("failed to parse private multiaddr: %v", err)
	}

	loopbackAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	if err != nil {
		t.Fatalf("failed to parse loopback multiaddr: %v", err)
	}

	unspecifiedAddr, err := ma.NewMultiaddr("/ip4/0.0.0.0/tcp/1234")
	if err != nil {
		t.Fatalf("failed to parse unspecified multiaddr: %v", err)
	}

	if !IsRoutableAddress(privateAddr) {
		t.Errorf("Expected private address %s to be routable when CIPHER_ALLOW_PRIVATE_DHT=1", privateAddr)
	}

	if !IsRoutableAddress(loopbackAddr) {
		t.Errorf("Expected loopback address %s to be routable when CIPHER_ALLOW_PRIVATE_DHT=1", loopbackAddr)
	}

	if IsRoutableAddress(unspecifiedAddr) {
		t.Errorf("Expected unspecified address %s to remain unroutable even with CIPHER_ALLOW_PRIVATE_DHT=1", unspecifiedAddr)
	}
}

func TestPublicAddressFilter(t *testing.T) {
	t.Setenv("CIPHER_ALLOW_PRIVATE_DHT", "")
	t.Setenv("CIPHER_ALLOW_LOCAL_IP", "")

	publicAddr, _ := ma.NewMultiaddr("/ip4/1.1.1.1/tcp/80")
	privateAddr, _ := ma.NewMultiaddr("/ip4/10.0.0.1/tcp/80")
	loopbackAddr, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/80")

	input := []ma.Multiaddr{publicAddr, privateAddr, loopbackAddr}
	filtered := PublicAddressFilter(input)

	if len(filtered) != 1 {
		t.Fatalf("Expected 1 filtered address, got %d", len(filtered))
	}

	if !filtered[0].Equal(publicAddr) {
		t.Errorf("Expected filtered address to be %s, got %s", publicAddr, filtered[0])
	}
}

func TestSanitizeAddrInfo(t *testing.T) {
	t.Setenv("CIPHER_ALLOW_PRIVATE_DHT", "")
	t.Setenv("CIPHER_ALLOW_LOCAL_IP", "")

	pid, err := peer.Decode("12D3KooWSD55Sip9e5Mdeqve9B219yqTfF4i8Qc14EwJ45c1K123")
	if err != nil {
		// Use a simple peer ID if constant string is invalid
		pid = peer.ID("testpeer")
	}

	publicAddr, _ := ma.NewMultiaddr("/ip4/8.8.8.8/tcp/4001")
	privateAddr, _ := ma.NewMultiaddr("/ip4/172.16.0.1/tcp/4001")

	info := peer.AddrInfo{
		ID:    pid,
		Addrs: []ma.Multiaddr{publicAddr, privateAddr},
	}

	sanitized := SanitizeAddrInfo(info)

	if len(sanitized.Addrs) != 1 {
		t.Fatalf("Expected 1 address after sanitization, got %d", len(sanitized.Addrs))
	}

	if !sanitized.Addrs[0].Equal(publicAddr) {
		t.Errorf("Expected %s, got %s", publicAddr, sanitized.Addrs[0])
	}
}

func TestSanitizeProviderChannel(t *testing.T) {
	t.Setenv("CIPHER_ALLOW_PRIVATE_DHT", "")
	t.Setenv("CIPHER_ALLOW_LOCAL_IP", "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	publicAddr, _ := ma.NewMultiaddr("/ip4/8.8.8.8/tcp/4001")
	privateAddr, _ := ma.NewMultiaddr("/ip4/10.0.0.1/tcp/4001")

	in := make(chan peer.AddrInfo, 2)
	in <- peer.AddrInfo{ID: peer.ID("peer1"), Addrs: []ma.Multiaddr{publicAddr}}
	in <- peer.AddrInfo{ID: peer.ID("peer2"), Addrs: []ma.Multiaddr{privateAddr}}
	close(in)

	out := SanitizeProviderChannel(ctx, in)

	var results []peer.AddrInfo
	for info := range out {
		results = append(results, info)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result from channel, got %d", len(results))
	}

	if results[0].ID != peer.ID("peer1") {
		t.Errorf("Expected peer1, got %s", results[0].ID)
	}
}
