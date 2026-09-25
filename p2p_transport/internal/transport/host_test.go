package transport

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func TestNewNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	if host == nil {
		t.Fatalf("Expected a host, got nil")
	}

	if len(host.Addrs()) == 0 {
		t.Fatalf("Expected at least one listen address")
	}

	// Verify ConnManager and ResourceManager are non-nil and active
	if host.ConnManager() == nil {
		t.Fatalf("Expected non-nil ConnManager")
	}

	if host.Network().ResourceManager() == nil {
		t.Fatalf("Expected non-nil ResourceManager")
	}
}

func TestNewNodeRoleProfiles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	roles := []NodeRole{RoleClient, RoleProvider, RoleRelay, RoleBootstrap}

	for _, role := range roles {
		t.Run(string(role), func(t *testing.T) {
			h, kdht, err := NewNode(ctx, 0, 0, nil, "", false, WithRoleProfile(role))
			if err != nil {
				t.Fatalf("Failed to create node with role profile %s: %v", role, err)
			}
			defer kdht.Close()
			defer h.Close()

			if h.ConnManager() == nil {
				t.Fatalf("ConnManager is nil for role %s", role)
			}
			if h.Network().ResourceManager() == nil {
				t.Fatalf("ResourceManager is nil for role %s", role)
			}
		})
	}
}

func TestNodeCustomQuotaOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := QuotaConfig{
		ConnLow:            15,
		ConnHigh:           35,
		GracePeriod:        10 * time.Second,
		MaxMemory:          64 * 1024 * 1024,
		MaxStreams:         128,
		MaxStreamsInbound:  32,
		MaxStreamsOutbound: 96,
		MaxConns:           30,
		MaxConnsInbound:    10,
		MaxConnsOutbound:   20,
	}

	h, kdht, err := NewNode(ctx, 0, 0, nil, "", false, WithQuotaConfig(cfg), WithConnWatermarks(20, 40, 5*time.Second))
	if err != nil {
		t.Fatalf("Failed to create node with custom quota options: %v", err)
	}
	defer kdht.Close()
	defer h.Close()

	if h.ConnManager() == nil {
		t.Fatalf("Expected non-nil ConnManager")
	}
}

func TestConnManagerProtectionTagging(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer kdht.Close()
	defer h.Close()

	cm := h.ConnManager()
	dummyPeer := peer.ID("12D3KooWDpjB7VfR3JJL3qXvAn4C2C9D1X5y3W8")
	tag := "keepalive"

	if cm.IsProtected(dummyPeer, tag) {
		t.Fatalf("Peer should not be protected initially")
	}

	cm.Protect(dummyPeer, tag)
	if !cm.IsProtected(dummyPeer, tag) {
		t.Fatalf("Peer should be protected after Protect()")
	}

	cm.Unprotect(dummyPeer, tag)
	if cm.IsProtected(dummyPeer, tag) {
		t.Fatalf("Peer should not be protected after Unprotect()")
	}
}

func TestSwarmLimitsRejection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Host A configured with strict quota (max 1 inbound stream)
	strictCfg := QuotaConfig{
		ConnLow:            1,
		ConnHigh:           2,
		GracePeriod:        1 * time.Second,
		MaxMemory:          16 * 1024 * 1024,
		MaxStreams:         1,
		MaxStreamsInbound:  1,
		MaxStreamsOutbound: 1,
		MaxConns:           5,
		MaxConnsInbound:    5,
		MaxConnsOutbound:   5,
	}

	hostA, dhtA, err := NewNode(ctx, 0, 0, nil, "", false, WithQuotaConfig(strictCfg))
	if err != nil {
		t.Fatalf("Failed to create host A: %v", err)
	}
	defer dhtA.Close()
	defer hostA.Close()

	const testProto protocol.ID = "/quota-test/1.0.0"
	hostA.SetStreamHandler(testProto, func(s network.Stream) {
		// keep stream open briefly
		time.Sleep(200 * time.Millisecond)
		s.Close()
	})

	// Host B (client)
	hostB, dhtB, err := NewNode(ctx, 0, 0, nil, "", false, WithRoleProfile(RoleClient))
	if err != nil {
		t.Fatalf("Failed to create host B: %v", err)
	}
	defer dhtB.Close()
	defer hostB.Close()

	// Connect host B to host A
	targetInfo := peer.AddrInfo{
		ID:    hostA.ID(),
		Addrs: hostA.Addrs(),
	}
	if err := hostB.Connect(ctx, targetInfo); err != nil {
		t.Fatalf("Failed to connect host B to host A: %v", err)
	}

	// Open first stream from host B -> host A
	s1, err := hostB.NewStream(ctx, hostA.ID(), testProto)
	if err != nil {
		t.Fatalf("Expected first stream to succeed, got %v", err)
	}
	defer s1.Close()

	// Attempt second concurrent stream (exceeding host A's MaxStreamsInbound: 1)
	s2, err := hostB.NewStream(ctx, hostA.ID(), testProto)
	if err == nil {
		// If NewStream didn't immediately fail, try writing or reading to detect stream reset/rejection
		_, writeErr := s2.Write([]byte("test"))
		if writeErr == nil {
			buf := make([]byte, 10)
			s2.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			_, readErr := s2.Read(buf)
			if readErr == nil {
				s2.Close()
				t.Fatalf("Expected second stream to be rejected by resource manager, but it succeeded")
			}
		}
		s2.Close()
	}
}
