package transport

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"cipher/internal/protocol"

	"github.com/libp2p/go-libp2p/core/network"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
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

	// Verify ResourceManager is attached to host network
	rm := host.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected ResourceManager to be attached, got nil")
	}

	host.Close()
}

func TestNewNode_OptionsAndAllowlist(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ma, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	if err != nil {
		t.Fatalf("Failed to parse multiaddr: %v", err)
	}

	limits := DefaultScalingLimits()
	host, kdht, err := NewNode(
		ctx,
		0,
		0,
		nil,
		"",
		false,
		WithAllowlist([]multiaddr.Multiaddr{ma}),
		WithScalingLimits(limits),
		WithMaxMemory(512<<20),
	)
	if err != nil {
		t.Fatalf("Expected host creation with options to succeed, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	if host.Network().ResourceManager() == nil {
		t.Fatalf("Expected attached ResourceManager with options")
	}
}

func TestResourceManager_PerProtocolStreamLimits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configure strict limit of 1 stream for ChunkTransportProtocolID per peer
	strictLimits := DefaultScalingLimits()
	strictLimits.AddProtocolPeerLimit(
		protocol.ChunkTransportProtocolID,
		rcmgr.BaseLimit{
			StreamsInbound:  1,
			StreamsOutbound: 1,
			Streams:         1,
			Memory:          16 << 20,
		},
		rcmgr.BaseLimitIncrease{},
	)

	h1, dht1, err := NewNode(ctx, 0, 0, nil, "", false, WithScalingLimits(strictLimits))
	if err != nil {
		t.Fatalf("Failed to create h1: %v", err)
	}
	defer dht1.Close()
	defer h1.Close()

	h2, dht2, err := NewNode(ctx, 0, 0, nil, "", false, WithScalingLimits(strictLimits))
	if err != nil {
		t.Fatalf("Failed to create h2: %v", err)
	}
	defer dht2.Close()
	defer h2.Close()

	// Set dummy handler on h1 for ChunkTransportProtocolID
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		time.Sleep(1 * time.Second)
		s.Close()
	})

	// Connect h2 to h1
	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), 10*time.Minute)
	if err := h2.Connect(ctx, h1.Peerstore().PeerInfo(h1.ID())); err != nil {
		t.Fatalf("Failed to connect h2 to h1: %v", err)
	}

	// First stream should succeed
	s1, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Expected 1st stream to succeed, got %v", err)
	}
	defer s1.Reset()

	// Second concurrent stream should exceed protocol limit
	_, err = h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err == nil {
		t.Fatalf("Expected 2nd stream to fail due to resource limits, but succeeded")
	}

	var limitErr *rcmgr.ErrStreamOrConnLimitExceeded
	if !errors.As(err, &limitErr) && !strings.Contains(err.Error(), "limit exceeded") {
		t.Fatalf("Expected resource limit error, got: %v", err)
	}
}

func TestResourceManager_PerPeerConnectionLimits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	strictLimits := DefaultScalingLimits()
	strictLimits.PeerBaseLimit = rcmgr.BaseLimit{
		ConnsInbound:  1,
		ConnsOutbound: 1,
		Conns:         1,
		FD:            1,
		Memory:        32 << 20,
	}
	strictLimits.PeerLimitIncrease = rcmgr.BaseLimitIncrease{}

	h1, dht1, err := NewNode(ctx, 0, 0, nil, "", false, WithScalingLimits(strictLimits))
	if err != nil {
		t.Fatalf("Failed to create h1: %v", err)
	}
	defer dht1.Close()
	defer h1.Close()

	h2, dht2, err := NewNode(ctx, 0, 0, nil, "", false, WithScalingLimits(strictLimits))
	if err != nil {
		t.Fatalf("Failed to create h2: %v", err)
	}
	defer dht2.Close()
	defer h2.Close()

	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), 10*time.Minute)

	// First connection should succeed
	if err := h2.Connect(ctx, h1.Peerstore().PeerInfo(h1.ID())); err != nil {
		t.Fatalf("Failed to connect h2 to h1: %v", err)
	}

	// Verify connected
	if len(h2.Network().ConnsToPeer(h1.ID())) == 0 {
		t.Fatalf("Expected active connection to h1")
	}
}

