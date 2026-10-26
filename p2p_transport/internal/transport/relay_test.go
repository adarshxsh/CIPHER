package transport

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
)

func TestDefaultRelayResources(t *testing.T) {
	rc := DefaultRelayResources()

	if rc.Limit == nil {
		t.Fatalf("Expected non-nil RelayLimit in DefaultRelayResources")
	}

	if rc.Limit.Duration != 2*time.Minute {
		t.Errorf("Expected Limit.Duration = 2m, got %v", rc.Limit.Duration)
	}

	if rc.Limit.Data != 128*1024 {
		t.Errorf("Expected Limit.Data = 128KB (131072), got %d", rc.Limit.Data)
	}

	if rc.MaxReservations <= 0 {
		t.Errorf("Expected positive MaxReservations, got %d", rc.MaxReservations)
	}

	if rc.MaxReservationsPerPeer <= 0 {
		t.Errorf("Expected positive MaxReservationsPerPeer, got %d", rc.MaxReservationsPerPeer)
	}

	if rc.MaxReservationsPerIP <= 0 {
		t.Errorf("Expected positive MaxReservationsPerIP, got %d", rc.MaxReservationsPerIP)
	}
}

func TestNewRelayService(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create host: %v", err)
	}
	defer h.Close()

	r, err := NewRelayService(h)
	if err != nil {
		t.Fatalf("Failed to create relay service with default resources: %v", err)
	}
	if r == nil {
		t.Fatalf("Expected non-nil relay instance")
	}

	// Test custom options override
	customRC := DefaultRelayResources()
	customRC.MaxReservations = 50
	r2, err := NewRelayService(h, relay.WithResources(customRC))
	if err != nil {
		t.Fatalf("Failed to create relay service with custom resources: %v", err)
	}
	if r2 == nil {
		t.Fatalf("Expected non-nil relay instance with custom resources")
	}

	_ = ctx
}
