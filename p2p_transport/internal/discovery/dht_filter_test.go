package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

func TestIsPublicAddr(t *testing.T) {
	tests := []struct {
		addr     string
		isPublic bool
	}{
		{"/ip4/127.0.0.1/tcp/4001", false},
		{"/ip6/::1/tcp/4001", false},
		{"/ip4/10.0.0.1/tcp/4001", false},
		{"/ip4/192.168.1.100/tcp/4001", false},
		{"/ip4/172.16.0.1/tcp/4001", false},
		{"/ip4/169.254.1.1/tcp/4001", false},
		{"/ip6/fe80::1/tcp/4001", false},
		{"/ip4/0.0.0.0/tcp/4001", false},
		{"/ip6/::/tcp/4001", false},
		{"/ip4/1.1.1.1/tcp/4001", true},
		{"/ip4/8.8.8.8/tcp/4001", true},
		{"/dns4/example.com/tcp/4001", true},
	}

	for _, tt := range tests {
		m, err := multiaddr.NewMultiaddr(tt.addr)
		if err != nil {
			t.Fatalf("failed to parse multiaddr %s: %v", tt.addr, err)
		}
		got := IsPublicAddr(m)
		if got != tt.isPublic {
			t.Errorf("IsPublicAddr(%s) = %v; want %v", tt.addr, got, tt.isPublic)
		}
	}
}

func TestPublicAddressFilter(t *testing.T) {
	rawAddrs := []string{
		"/ip4/127.0.0.1/tcp/4001",
		"/ip4/10.0.1.5/tcp/4001",
		"/ip4/1.1.1.1/tcp/4001",
		"/ip4/192.168.0.10/tcp/4001",
		"/ip4/8.8.8.8/tcp/4001",
	}

	var addrs []multiaddr.Multiaddr
	for _, a := range rawAddrs {
		m, err := multiaddr.NewMultiaddr(a)
		if err != nil {
			t.Fatalf("failed to parse %s: %v", a, err)
		}
		addrs = append(addrs, m)
	}

	filtered := PublicAddressFilter(addrs)

	if len(filtered) != 2 {
		t.Fatalf("expected 2 public addresses, got %d", len(filtered))
	}

	for _, a := range filtered {
		if !IsPublicAddr(a) {
			t.Errorf("expected only public address, got %s", a)
		}
	}
}

func TestSanitizeAddrInfo(t *testing.T) {
	pID := peer.ID("test-peer-id-123")
	m1, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
	m2, _ := multiaddr.NewMultiaddr("/ip4/1.1.1.1/tcp/4001")

	info := peer.AddrInfo{
		ID:    pID,
		Addrs: []multiaddr.Multiaddr{m1, m2},
	}

	sanitized := SanitizeAddrInfo(info, false)
	if len(sanitized.Addrs) != 1 {
		t.Fatalf("expected 1 address after sanitization, got %d", len(sanitized.Addrs))
	}
	if sanitized.Addrs[0].String() != "/ip4/1.1.1.1/tcp/4001" {
		t.Errorf("expected /ip4/1.1.1.1/tcp/4001, got %s", sanitized.Addrs[0].String())
	}
}

func TestSanitizeProviderChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pID1 := peer.ID("peer-1")
	pID2 := peer.ID("peer-2")

	mPrivate, _ := multiaddr.NewMultiaddr("/ip4/10.0.0.1/tcp/4001")
	mPublic, _ := multiaddr.NewMultiaddr("/ip4/8.8.8.8/tcp/4001")

	in := make(chan peer.AddrInfo, 2)
	// Peer 1 has only private address -> should be dropped
	in <- peer.AddrInfo{ID: pID1, Addrs: []multiaddr.Multiaddr{mPrivate}}
	// Peer 2 has private & public address -> private stripped, peer retained
	in <- peer.AddrInfo{ID: pID2, Addrs: []multiaddr.Multiaddr{mPrivate, mPublic}}
	close(in)

	out := SanitizeProviderChannel(ctx, in, false)

	var results []peer.AddrInfo
	for p := range out {
		results = append(results, p)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 peer from channel, got %d", len(results))
	}
	if results[0].ID != pID2 {
		t.Errorf("expected peer %s, got %s", pID2, results[0].ID)
	}
	if len(results[0].Addrs) != 1 || results[0].Addrs[0].String() != "/ip4/8.8.8.8/tcp/4001" {
		t.Errorf("expected public address /ip4/8.8.8.8/tcp/4001, got %v", results[0].Addrs)
	}
}

func TestNewDHTInitializationAndFiltering(t *testing.T) {
	h, err := libp2p.New()
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	customAddrFilter := dht.AddressFilter(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
		return FilterAddresses(addrs, false)
	})

	kdht, err := NewDHT(h, dht.ModeServer, customAddrFilter)
	if err != nil {
		t.Fatalf("failed to create DHT with custom options: %v", err)
	}
	defer kdht.Close()

	if kdht.RoutingTable() == nil {
		t.Fatal("expected routing table to be initialized")
	}
}

func TestRoutingTableFiltersUnroutablePeers(t *testing.T) {
	h1, err := libp2p.New()
	if err != nil {
		t.Fatalf("failed to create h1: %v", err)
	}
	defer h1.Close()

	dht1, err := NewDHT(h1, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht1: %v", err)
	}
	defer dht1.Close()

	// In public mode, peers with loopback or private addresses should be rejected by RoutingTableFilter
	dummyID := peer.ID("12D3KooWDpjB924PCh2qMZaUe4f2B2KH27524625225252525252")
	loopbackMaddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")

	// Host 1 peerstore only has loopback address for dummyID
	h1.Peerstore().AddAddrs(dummyID, []multiaddr.Multiaddr{loopbackMaddr}, time.Minute)

	// RoutingTableFilter check should return false for loopback/unroutable peer
	filterPassed := CustomPublicRoutingTableFilter(dht1, dummyID)
	if filterPassed {
		t.Errorf("expected RoutingTableFilter to reject peer with only loopback address")
	}

	// Verify routing table does not contain unroutable peer
	if len(dht1.RoutingTable().ListPeers()) != 0 {
		t.Errorf("expected empty routing table, got %d peers", len(dht1.RoutingTable().ListPeers()))
	}
}
