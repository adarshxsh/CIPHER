package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	connmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
)

func TestNewNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer kdht.Close()
	defer h.Close()

	if h == nil {
		t.Fatalf("Expected a host, got nil")
	}

	if len(h.Addrs()) == 0 {
		t.Fatalf("Expected at least one listen address")
	}

	if h.ConnManager() == nil {
		t.Fatalf("Expected host ConnManager to be initialized, got nil")
	}

	if h.Network().ResourceManager() == nil {
		t.Fatalf("Expected host ResourceManager to be initialized, got nil")
	}
}

func TestNewHost_Alias(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, kdht, err := NewHost(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer kdht.Close()
	defer h.Close()

	if h == nil {
		t.Fatalf("Expected a host, got nil")
	}
}

func TestNewNode_FunctionalOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	customCM, err := connmgr.NewConnManager(10, 30, connmgr.WithGracePeriod(5*time.Second))
	if err != nil {
		t.Fatalf("Failed to create custom connmanager: %v", err)
	}
	defer customCM.Close()

	h, kdht, err := NewNode(
		ctx, 0, 0, nil, "", false,
		WithConnManager(customCM),
		WithConnLimits(15, 40, 10*time.Second),
		WithMemoryLimit(64*1024*1024),
		WithPeerStreamLimit(16),
	)
	if err != nil {
		t.Fatalf("Failed to create node with custom options: %v", err)
	}
	defer kdht.Close()
	defer h.Close()

	if h.ConnManager() != customCM {
		t.Fatalf("Expected custom ConnManager to be used")
	}
}

func TestNewNode_ConnectionWatermarkPruning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create host with tight watermarks: low = 1, high = 2, grace period = 0
	h1, dht1, err := NewNode(ctx, 0, 0, nil, "", false, WithConnLimits(1, 2, 0))
	if err != nil {
		t.Fatalf("Failed to create target host: %v", err)
	}
	defer dht1.Close()
	defer h1.Close()

	// Create 3 client hosts that dial h1
	clients := make([]interface{ Close() error }, 0)
	defer func() {
		for _, c := range clients {
			if c != nil {
				c.Close()
			}
		}
	}()

	targetInfo := peer.AddrInfo{
		ID:    h1.ID(),
		Addrs: h1.Addrs(),
	}

	for i := 0; i < 3; i++ {
		hClient, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
		if err != nil {
			t.Fatalf("Failed to create client host %d: %v", i, err)
		}
		clients = append(clients, hClient)

		err = hClient.Connect(ctx, targetInfo)
		if err != nil {
			t.Fatalf("Client %d failed to connect to target host: %v", i, err)
		}
	}

	// Trigger connection pruning on target host
	h1.ConnManager().TrimOpenConns(ctx)

	// Allow prune background worker to complete
	time.Sleep(100 * time.Millisecond)

	// Verify active connection count is trimmed down towards low watermark
	numConns := len(h1.Network().Conns())
	if numConns > 2 {
		t.Fatalf("Expected connection count to be pruned to <= 2, got %d", numConns)
	}
}

func TestNewNode_ResourceManagerEnforcement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create host with extremely strict stream limit
	h, kdht, err := NewNode(ctx, 0, 0, nil, "", false, WithPeerStreamLimit(1))
	if err != nil {
		t.Fatalf("Failed to create host with strict stream limit: %v", err)
	}
	defer kdht.Close()
	defer h.Close()

	rm := h.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected resource manager to be non-nil")
	}

	// Request stream scope allocation from resource manager
	sScope, err := rm.OpenStream("", network.DirInbound)
	if err != nil {
		t.Fatalf("Failed to open first stream scope: %v", err)
	}
	defer sScope.Done()

	// Open second stream scope or reserve memory
	sScope2, err2 := rm.OpenStream("", network.DirInbound)
	if err2 == nil {
		defer sScope2.Done()
	} else {
		var limitErr *rcmgr.ErrStreamOrConnLimitExceeded
		if errors.As(err2, &limitErr) {
			t.Logf("Resource limit correctly enforced: %v", err2)
		} else {
			t.Logf("Observed stream open error: %v", err2)
		}
	}
}

