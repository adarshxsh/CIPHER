package transport

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
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

	cm := host.ConnManager()
	if cm == nil {
		t.Fatalf("Expected non-nil ConnectionManager")
	}

	rm := host.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected non-nil ResourceManager")
	}

	if _, ok := rm.(*network.NullResourceManager); ok {
		t.Fatalf("Expected active ResourceManager, got NullResourceManager")
	}
}

func TestNewNode_WithOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host, kdht, err := NewNode(
		ctx, 0, 0, nil, "", false,
		WithConnLimits(20, 40),
		WithConnWatermarks(15, 30),
		WithConnGracePeriod(30*time.Second),
		WithMemoryLimits(128*1024*1024, 8*1024*1024),
		WithSystemMemoryLimit(100*1024*1024),
		WithPeerMemoryLimit(4*1024*1024),
		WithPeerStreamLimit(32),
	)
	if err != nil {
		t.Fatalf("Expected no error with options, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	if host.ConnManager() == nil {
		t.Fatalf("Expected non-nil ConnectionManager with custom options")
	}

	if host.Network().ResourceManager() == nil {
		t.Fatalf("Expected non-nil ResourceManager with custom options")
	}
}

