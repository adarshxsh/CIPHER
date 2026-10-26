package discovery

import (
	"context"
	"os"
	"testing"
	"time"

	"cipher/internal/content/core"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/mock"
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

func TestIsRoutableAddress(t *testing.T) {
	pubAddr := parseAddr(t, "/ip4/8.8.8.8/tcp/4001")
	dnsAddr := parseAddr(t, "/dns4/example.com/tcp/4001")
	loopbackAddr := parseAddr(t, "/ip4/127.0.0.1/tcp/4001")
	privAddr10 := parseAddr(t, "/ip4/10.0.0.1/tcp/4001")
	privAddr192 := parseAddr(t, "/ip4/192.168.1.1/tcp/4001")
	unspecAddr := parseAddr(t, "/ip4/0.0.0.0/tcp/4001")

	// Public mode tests
	os.Unsetenv("CIPHER_ALLOW_PRIVATE_DHT")
	os.Unsetenv("CIPHER_ALLOW_LOCAL_IP")

	if !IsRoutableAddress(pubAddr) {
		t.Errorf("expected public address %s to be routable in public mode", pubAddr)
	}
	if !IsRoutableAddress(dnsAddr) {
		t.Errorf("expected dns address %s to be routable in public mode", dnsAddr)
	}
	if IsRoutableAddress(loopbackAddr) {
		t.Errorf("expected loopback address %s to be rejected in public mode", loopbackAddr)
	}
	if IsRoutableAddress(privAddr10) {
		t.Errorf("expected private address 10.x %s to be rejected in public mode", privAddr10)
	}
	if IsRoutableAddress(privAddr192) {
		t.Errorf("expected private address 192.x %s to be rejected in public mode", privAddr192)
	}
	if IsRoutableAddress(unspecAddr) {
		t.Errorf("expected unspecified address %s to be rejected in public mode", unspecAddr)
	}
	if IsRoutableAddress(nil) {
		t.Errorf("expected nil multiaddr to be rejected")
	}

	// Private network / test mode
	t.Setenv("CIPHER_ALLOW_PRIVATE_DHT", "1")

	if !IsRoutableAddress(pubAddr) {
		t.Errorf("expected public address %s to be routable in private mode", pubAddr)
	}
	if !IsRoutableAddress(loopbackAddr) {
		t.Errorf("expected loopback address %s to be routable in private mode", loopbackAddr)
	}
	if !IsRoutableAddress(privAddr10) {
		t.Errorf("expected private address 10.x %s to be routable in private mode", privAddr10)
	}
	if IsRoutableAddress(unspecAddr) {
		t.Errorf("expected unspecified address %s to be rejected in private mode", unspecAddr)
	}
}

func TestPublicAddressFilterAndSanitizeAddrInfo(t *testing.T) {
	pubAddr := parseAddr(t, "/ip4/8.8.8.8/tcp/4001")
	privAddr := parseAddr(t, "/ip4/192.168.1.1/tcp/4001")
	loopbackAddr := parseAddr(t, "/ip4/127.0.0.1/tcp/4001")
	unspecAddr := parseAddr(t, "/ip4/0.0.0.0/tcp/4001")

	os.Unsetenv("CIPHER_ALLOW_PRIVATE_DHT")
	os.Unsetenv("CIPHER_ALLOW_LOCAL_IP")

	addrs := []ma.Multiaddr{pubAddr, privAddr, loopbackAddr, unspecAddr}
	filtered := PublicAddressFilter(addrs)

	if len(filtered) != 1 || !filtered[0].Equal(pubAddr) {
		t.Fatalf("PublicAddressFilter returned unexpected addrs: %v", filtered)
	}

	info := peer.AddrInfo{
		ID:    peer.ID("testpeer"),
		Addrs: addrs,
	}

	sanitized := SanitizeAddrInfo(info)
	if len(sanitized.Addrs) != 1 || !sanitized.Addrs[0].Equal(pubAddr) {
		t.Fatalf("SanitizeAddrInfo returned unexpected addrs: %v", sanitized.Addrs)
	}
}

func TestSanitizeProviderChannel(t *testing.T) {
	pubAddr := parseAddr(t, "/ip4/8.8.8.8/tcp/4001")
	privAddr := parseAddr(t, "/ip4/10.0.0.1/tcp/4001")

	os.Unsetenv("CIPHER_ALLOW_PRIVATE_DHT")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	in := make(chan peer.AddrInfo, 2)
	in <- peer.AddrInfo{ID: peer.ID("peer1"), Addrs: []ma.Multiaddr{privAddr}}
	in <- peer.AddrInfo{ID: peer.ID("peer2"), Addrs: []ma.Multiaddr{pubAddr, privAddr}}
	close(in)

	out := SanitizeProviderChannel(ctx, in)

	var results []peer.AddrInfo
	for p := range out {
		results = append(results, p)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 provider result after channel sanitization, got %d", len(results))
	}
	if results[0].ID != peer.ID("peer2") {
		t.Errorf("expected peer2, got %s", results[0].ID)
	}
	if len(results[0].Addrs) != 1 || !results[0].Addrs[0].Equal(pubAddr) {
		t.Errorf("expected only public address in peer2 addrs, got %v", results[0].Addrs)
	}
}

func TestNewDHT_DefaultSecurityOptions(t *testing.T) {
	mn := mocknet.New()
	defer mn.Close()

	h, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate mock peer: %v", err)
	}

	os.Unsetenv("CIPHER_ALLOW_PRIVATE_DHT")

	kdht, err := NewDHT(h, dht.ModeServer)
	if err != nil {
		t.Fatalf("NewDHT failed in default public mode: %v", err)
	}
	defer kdht.Close()

	if kdht == nil {
		t.Fatal("expected non-nil IpfsDHT")
	}
}

func TestFindProviders_UnroutableAddressFiltering(t *testing.T) {
	t.Setenv("CIPHER_ALLOW_PRIVATE_DHT", "1")

	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create real host 1: %v", err)
	}
	defer h1.Close()

	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create real host 2: %v", err)
	}
	defer h2.Close()

	kdht1, err := NewDHT(h1, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create DHT 1: %v", err)
	}
	defer kdht1.Close()

	kdht2, err := NewDHT(h2, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create DHT 2: %v", err)
	}
	defer kdht2.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	h1Info := peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}
	h2Info := peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}

	if err := h1.Connect(ctx, h2Info); err != nil {
		t.Fatalf("failed to connect h1 to h2: %v", err)
	}
	if err := h2.Connect(ctx, h1Info); err != nil {
		t.Fatalf("failed to connect h2 to h1: %v", err)
	}

	kdht1.RoutingTable().TryAddPeer(h2.ID(), true, false)
	kdht2.RoutingTable().TryAddPeer(h1.ID(), true, false)
	time.Sleep(200 * time.Millisecond)

	var dummyContentID core.ContentID
	copy(dummyContentID[:], []byte("01234567890123456789012345678901"))

	if err := Provide(ctx, kdht2, dummyContentID); err != nil {
		t.Fatalf("Provide failed: %v", err)
	}

	// Verify FindProviders returns provider record in private mode
	providers, err := FindProviders(ctx, kdht1, dummyContentID, 5)
	if err != nil {
		t.Fatalf("FindProviders failed: %v", err)
	}
	if len(providers) == 0 {
		t.Fatalf("expected at least 1 provider in private mode")
	}

	// Verify that in public mode, non-public multiaddresses are filtered out
	os.Unsetenv("CIPHER_ALLOW_PRIVATE_DHT")
	for _, p := range providers {
		sanitized := SanitizeAddrInfo(p)
		for _, addr := range sanitized.Addrs {
			if !IsRoutableAddress(addr) {
				t.Errorf("SanitizeAddrInfo allowed unroutable multiaddr %s in public mode", addr)
			}
		}
	}
}

func TestRoutingTablePeerDiversityFilter_RejectsSyntheticFlood(t *testing.T) {
	mn := mocknet.New()
	defer mn.Close()

	hHost, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to create host peer: %v", err)
	}

	os.Unsetenv("CIPHER_ALLOW_PRIVATE_DHT")

	kdht, err := NewDHT(hHost, dht.ModeServer)
	if err != nil {
		t.Fatalf("NewDHT failed: %v", err)
	}
	defer kdht.Close()

	rt := kdht.RoutingTable()
	if rt == nil {
		t.Fatal("RoutingTable is nil")
	}

	// Synthetic flood attempt: try adding multiple peers from the same IP group/subnet
	var added int
	for i := 0; i < 10; i++ {
		fakePeer, err := mn.GenPeer()
		if err != nil {
			t.Fatalf("failed to gen fake peer: %v", err)
		}
		// Attempt to insert peer into routing table
		if ok, _ := rt.TryAddPeer(fakePeer.ID(), true, false); ok {
			added++
		}
	}

	// Diversity filter enforces max peers per CPL / IP group (default 2/3)
	if added > 3 {
		t.Errorf("Routing table diversity filter failed: added %d peers from same IP group (limit should be <= 3)", added)
	}
}
