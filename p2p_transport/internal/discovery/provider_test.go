package discovery

import (
	"context"
	"testing"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func TestSanitizeAddrInfo(t *testing.T) {
	pid := peer.ID("test-peer-id")

	m1, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
	m2, _ := ma.NewMultiaddr("/ip4/10.0.0.1/tcp/4001")
	m3, _ := ma.NewMultiaddr("/ip4/8.8.8.8/tcp/4001")

	info := peer.AddrInfo{
		ID:    pid,
		Addrs: []ma.Multiaddr{m1, m2, m3},
	}

	sanitized := SanitizeAddrInfo(info)

	if len(sanitized.Addrs) != 1 {
		t.Fatalf("Expected 1 public multiaddr, got %d", len(sanitized.Addrs))
	}

	if sanitized.Addrs[0].String() != "/ip4/8.8.8.8/tcp/4001" {
		t.Errorf("Expected /ip4/8.8.8.8/tcp/4001, got %s", sanitized.Addrs[0].String())
	}
}

func TestSanitizeAddrInfo_AllUnroutable(t *testing.T) {
	pid := peer.ID("test-peer-id")

	m1, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
	m2, _ := ma.NewMultiaddr("/ip4/192.168.1.1/tcp/4001")

	info := peer.AddrInfo{
		ID:    pid,
		Addrs: []ma.Multiaddr{m1, m2},
	}

	sanitized := SanitizeAddrInfo(info)

	if len(sanitized.Addrs) != 0 {
		t.Fatalf("Expected 0 public multiaddrs, got %d", len(sanitized.Addrs))
	}
}

func TestFindProviders_InvalidLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	var cid core.ContentID
	_, err = FindProviders(ctx, kdht, cid, 0)
	if err == nil {
		t.Fatalf("Expected error for limit <= 0, got nil")
	}
}
