package transport

import (
	"context"
	"testing"
	"time"

	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
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

	if host.ConnManager() == nil {
		t.Errorf("Expected non-nil ConnManager on host")
	}

	if host.Network().ResourceManager() == nil {
		t.Errorf("Expected non-nil ResourceManager on host")
	}

	host.Close()
}

func TestNewNode_CustomOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false,
		WithMinConns(50),
		WithMaxConns(150),
		WithGracePeriod(30*time.Second),
		WithMemoryLimitMB(512),
	)
	if err != nil {
		t.Fatalf("Expected no error with custom options, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	if host.ConnManager() == nil {
		t.Errorf("Expected non-nil ConnManager")
	}
	if host.Network().ResourceManager() == nil {
		t.Errorf("Expected non-nil ResourceManager")
	}
}

func TestNewNode_ConnWatermarks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false,
		WithConnLimits(10, 20, 5*time.Second),
	)
	if err != nil {
		t.Fatalf("Expected no error with WithConnLimits option, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	if host.ConnManager() == nil {
		t.Fatalf("Expected non-nil ConnManager")
	}
}

func TestNewNode_CustomManagers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm, err := connmgr.NewConnManager(20, 80, connmgr.WithGracePeriod(10*time.Second))
	if err != nil {
		t.Fatalf("Failed to create conn manager: %v", err)
	}

	limiter := rcmgr.NewFixedLimiter(rcmgr.DefaultLimits.AutoScale())
	rm, err := rcmgr.NewResourceManager(limiter)
	if err != nil {
		t.Fatalf("Failed to create resource manager: %v", err)
	}

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false,
		WithConnManager(cm),
		WithResourceManager(rm),
	)
	if err != nil {
		t.Fatalf("Expected no error with custom managers, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	if host.ConnManager() != cm {
		t.Errorf("Expected custom ConnManager to be set on host")
	}
	if host.Network().ResourceManager() != rm {
		t.Errorf("Expected custom ResourceManager to be set on host")
	}
}

func TestNewNode_WatermarkSanitization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Invalid watermarks: low <= 0, high < low
	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false,
		WithMinConns(-10),
		WithMaxConns(5),
	)
	if err != nil {
		t.Fatalf("Expected no error with sanitized watermarks, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	if host.ConnManager() == nil {
		t.Errorf("Expected non-nil ConnManager")
	}
}

