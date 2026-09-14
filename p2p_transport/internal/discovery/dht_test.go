package discovery

import (
	"context"
	"os"
	"testing"
	"time"

	"cipher/internal/content/core"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kbucket/peerdiversity"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/mock"
	ma "github.com/multiformats/go-multiaddr"
)

func TestAddressSanitization(t *testing.T) {
	// Ensure CIPHER_ALLOW_LOCAL_IP is unset during this test
	os.Unsetenv("CIPHER_ALLOW_LOCAL_IP")

	loopback, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/8000")
	private, _ := ma.NewMultiaddr("/ip4/192.168.1.1/tcp/8000")
	unspecified, _ := ma.NewMultiaddr("/ip4/0.0.0.0/tcp/8000")
	public, _ := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/8000")

	if IsRoutableAddress(loopback) {
		t.Errorf("expected loopback address to be unroutable")
	}
	if IsRoutableAddress(private) {
		t.Errorf("expected private address to be unroutable")
	}
	if IsRoutableAddress(unspecified) {
		t.Errorf("expected unspecified address to be unroutable")
	}
	if !IsRoutableAddress(public) {
		t.Errorf("expected public address to be routable")
	}

	addrs := []ma.Multiaddr{loopback, private, unspecified, public}
	sanitized := SanitizeAddresses(addrs)
	if len(sanitized) != 1 {
		t.Fatalf("expected 1 sanitized address, got %d", len(sanitized))
	}
	if !sanitized[0].Equal(public) {
		t.Errorf("expected public address %s, got %s", public, sanitized[0])
	}

	// Test CIPHER_ALLOW_LOCAL_IP override
	os.Setenv("CIPHER_ALLOW_LOCAL_IP", "1")
	defer os.Unsetenv("CIPHER_ALLOW_LOCAL_IP")

	if !IsRoutableAddress(loopback) {
		t.Errorf("expected loopback to be allowed when CIPHER_ALLOW_LOCAL_IP=1")
	}
	if !IsRoutableAddress(private) {
		t.Errorf("expected private to be allowed when CIPHER_ALLOW_LOCAL_IP=1")
	}
	if IsRoutableAddress(unspecified) {
		t.Errorf("unspecified address should still be unroutable")
	}
}

func TestNewDHT_Configuration(t *testing.T) {
	mn := mocknet.New()
	h, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate mock peer: %v", err)
	}
	defer h.Close()

	// NewDHT initializes with default security options
	kdht, err := NewDHT(h, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create DHT: %v", err)
	}
	defer kdht.Close()

	if kdht == nil {
		t.Fatal("expected non-nil DHT")
	}

	// Verify NewDHT accepts additional dht.Option options
	kdht2, err := NewDHT(h, dht.ModeServer, dht.Concurrency(5))
	if err != nil {
		t.Fatalf("failed to create DHT with custom option: %v", err)
	}
	defer kdht2.Close()
}

func TestPeerDiversityFilter_MaxLimit(t *testing.T) {
	mn := mocknet.New()
	h, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate mock peer: %v", err)
	}
	defer h.Close()

	maxPerCpl := 2
	maxForTable := 2
	filter := dht.NewRTPeerDiversityFilter(h, maxPerCpl, maxForTable)

	ipGroupKey := peerdiversity.PeerIPGroupKey("192.168.1.0/24")

	g1 := peerdiversity.PeerGroupInfo{
		Id:         peer.ID("peer1"),
		Cpl:        0,
		IPGroupKey: ipGroupKey,
	}
	g2 := peerdiversity.PeerGroupInfo{
		Id:         peer.ID("peer2"),
		Cpl:        0,
		IPGroupKey: ipGroupKey,
	}
	g3 := peerdiversity.PeerGroupInfo{
		Id:         peer.ID("peer3"),
		Cpl:        0,
		IPGroupKey: ipGroupKey,
	}

	if !filter.Allow(g1) {
		t.Errorf("expected peer 1 to be allowed")
	}
	filter.Increment(g1)

	if !filter.Allow(g2) {
		t.Errorf("expected peer 2 to be allowed")
	}
	filter.Increment(g2)

	// Peer 3 is above the max limit of 2 for the IP subnet group
	if filter.Allow(g3) {
		t.Errorf("expected peer 3 from same IP subnet above diversity limit to be rejected")
	}
}

func TestFindProviders_UnroutableAddressFiltering(t *testing.T) {
	os.Unsetenv("CIPHER_ALLOW_LOCAL_IP")

	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate mock peer: %v", err)
	}
	defer h1.Close()

	kdht, err := NewDHT(h1, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create DHT: %v", err)
	}
	defer kdht.Close()

	var testID core.ContentID
	copy(testID[:], []byte("01234567890123456789012345678901"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	providers, err := FindProviders(ctx, kdht, testID, 5)
	if err != nil {
		t.Fatalf("FindProviders error: %v", err)
	}

	for _, p := range providers {
		for _, addr := range p.Addrs {
			if !IsRoutableAddress(addr) {
				t.Errorf("FindProviders returned unroutable multiaddress: %s", addr)
			}
		}
	}
}

func TestBootstrap_UnroutableAddressFiltering(t *testing.T) {
	os.Unsetenv("CIPHER_ALLOW_LOCAL_IP")

	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate mock peer: %v", err)
	}
	defer h1.Close()

	kdht, err := NewDHT(h1, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create DHT: %v", err)
	}
	defer kdht.Close()

	p2, _ := peer.Decode("12D3KooWSD15a") // dummy peer ID
	unroutableAddr, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/9000")

	seeds := []peer.AddrInfo{
		{
			ID:    p2,
			Addrs: []ma.Multiaddr{unroutableAddr},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err = Bootstrap(ctx, kdht, h1, seeds)
	if err == nil {
		t.Errorf("expected Bootstrap to fail when all seeds have unroutable multiaddresses")
	}
}
