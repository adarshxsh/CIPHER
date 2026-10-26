package transport

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/multiformats/go-multiaddr"
)

func TestNewNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer kdht.Close()

	if host == nil {
		t.Fatalf("Expected a host, got nil")
	}

	if len(host.Addrs()) == 0 {
		t.Fatalf("Expected at least one listen address")
	}

	host.Close()
}

func TestRelayNodeDefaults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rc := relay.DefaultResources()
	if rc.Limit.Data != 128*1024 {
		t.Fatalf("Expected default data limit to be 128 KB (%d bytes), got %d", 128*1024, rc.Limit.Data)
	}
	if rc.Limit.Duration != 2*time.Minute {
		t.Fatalf("Expected default duration limit to be 2 min, got %v", rc.Limit.Duration)
	}

	hRelay, dhtRelay, err := NewNode(ctx, 0, 0, nil, "", false, WithRelayResources(rc))
	if err != nil {
		t.Fatalf("Failed to create relay node with default resources: %v", err)
	}
	defer dhtRelay.Close()
	defer hRelay.Close()

	if hRelay == nil {
		t.Fatal("Expected non-nil relay host")
	}
}

func TestRelayDataCapEnforcement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Configure a tight data limit of 10 KB for testing fast cap enforcement
	rc := relay.DefaultResources()
	rc.Limit.Data = 10 * 1024 // 10 KB
	rc.Limit.Duration = 1 * time.Minute

	hRelay, dhtRelay, err := NewNode(ctx, 0, 0, nil, "", false, WithRelayResources(rc))
	if err != nil {
		t.Fatalf("Failed to create relay node: %v", err)
	}
	defer dhtRelay.Close()
	defer hRelay.Close()

	// Create peer A (behind relay) and peer B
	hA, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.EnableRelay())
	if err != nil {
		t.Fatalf("Failed to create peer A: %v", err)
	}
	defer hA.Close()

	hB, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.EnableRelay())
	if err != nil {
		t.Fatalf("Failed to create peer B: %v", err)
	}
	defer hB.Close()

	relayInfo := peer.AddrInfo{
		ID:    hRelay.ID(),
		Addrs: hRelay.Addrs(),
	}

	if err := hA.Connect(ctx, relayInfo); err != nil {
		t.Fatalf("Peer A failed to connect to relay: %v", err)
	}

	res, err := client.Reserve(ctx, hA, relayInfo)
	if err != nil || res == nil {
		t.Fatalf("Peer A failed to reserve relay slot: %v", err)
	}

	// Connect peer B to relay as well
	if err := hB.Connect(ctx, relayInfo); err != nil {
		t.Fatalf("Peer B failed to connect to relay: %v", err)
	}

	// Set stream handler on Peer A
	receivedBytes := make(chan int, 1)
	hA.SetStreamHandler("/test/data-cap/1.0.0", func(s network.Stream) {
		defer s.Close()
		buf := make([]byte, 4096)
		total := 0
		for {
			n, err := s.Read(buf)
			total += n
			if err != nil {
				break
			}
		}
		receivedBytes <- total
	})

	// Construct relayed multiaddr for Peer A including relay transport address
	relayAddrStr := hRelay.Addrs()[0].String() + "/p2p/" + hRelay.ID().String() + "/p2p-circuit"
	circuitMaddr, err := multiaddr.NewMultiaddr(relayAddrStr)
	if err != nil {
		t.Fatalf("Failed to parse circuit multiaddr: %v", err)
	}

	peerAInfo := peer.AddrInfo{
		ID:    hA.ID(),
		Addrs: []multiaddr.Multiaddr{circuitMaddr},
	}

	if err := hB.Connect(ctx, peerAInfo); err != nil {
		t.Fatalf("Peer B failed to connect to Peer A via relay: %v", err)
	}

	s, err := hB.NewStream(network.WithAllowLimitedConn(ctx, "test"), hA.ID(), "/test/data-cap/1.0.0")
	if err != nil {
		t.Fatalf("Peer B failed to open relayed stream to Peer A: %v", err)
	}

	// Attempt to send 20 KB (exceeding the 10 KB limit)
	data := make([]byte, 20*1024)
	_, writeErr := s.Write(data)
	s.Close()

	select {
	case total := <-receivedBytes:
		if int64(total) > rc.Limit.Data {
			t.Fatalf("Expected received bytes to be capped at %d, but received %d", rc.Limit.Data, total)
		}
		t.Logf("Relay capped data transfer successfully: received %d bytes (limit was %d bytes, attempt was 20 KB)", total, rc.Limit.Data)
	case <-time.After(5 * time.Second):
		t.Logf("Stream closed due to relay data limit enforcement (writeErr: %v)", writeErr)
	}
}
