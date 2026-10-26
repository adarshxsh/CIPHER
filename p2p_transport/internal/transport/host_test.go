package transport

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
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

	if host.ConnManager() == nil {
		t.Fatalf("Expected active ConnectionManager, got nil")
	}

	rm := host.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected active ResourceManager, got nil")
	}
	if _, isNull := rm.(*network.NullResourceManager); isNull {
		t.Fatalf("Expected real ResourceManager, got NullResourceManager")
	}
}

func TestNewNode_WithConnLimits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lowWater := 50
	highWater := 150
	gracePeriod := 30 * time.Second

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false, WithConnLimits(lowWater, highWater, gracePeriod))
	if err != nil {
		t.Fatalf("Expected no error with custom conn limits, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	if host.ConnManager() == nil {
		t.Fatalf("Expected active ConnectionManager, got nil")
	}
}

func TestNewNode_WithResourceLimits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	customLimits := rcmgr.DefaultLimits
	customLimits.SystemBaseLimit.ConnsInbound = 200

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false, WithResourceLimits(customLimits))
	if err != nil {
		t.Fatalf("Expected no error with custom resource limits, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	rm := host.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected active ResourceManager, got nil")
	}
}

