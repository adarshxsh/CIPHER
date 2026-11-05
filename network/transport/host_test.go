package transport

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
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

	if host == nil {
		t.Fatalf("Expected a host, got nil")
	}

	if len(host.Addrs()) == 0 {
		t.Fatalf("Expected at least one listen address")
	}

	if host.ConnManager() == nil {
		t.Fatalf("Expected active ConnectionManager on host")
	}

	if host.Network().ResourceManager() == nil {
		t.Fatalf("Expected active ResourceManager on host network")
	}

	host.Close()
}

func TestNewNodeWithOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limiter := rcmgr.NewFixedLimiter(rcmgr.DefaultLimits.AutoScale())

	host, kdht, err := NewNode(
		ctx,
		0,
		0,
		nil,
		"",
		false,
		WithConnectionLimits(50, 150, 30*time.Second),
		WithResourceLimits(limiter),
	)
	if err != nil {
		t.Fatalf("Expected no error creating host with options, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	if host.ConnManager() == nil {
		t.Fatalf("Expected active ConnectionManager")
	}

	if host.Network().ResourceManager() == nil {
		t.Fatalf("Expected active ResourceManager")
	}
}

func TestResourceManagerWarningLogger(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(log.Writer())

	logger := &rcmgrWarningLogger{}
	dummyPeer := peer.ID("test-peer")

	logger.BlockConn(network.DirInbound, true)
	logger.BlockStream(dummyPeer, network.DirOutbound)
	logger.BlockPeer(dummyPeer)
	logger.BlockProtocol("/test/1.0.0")
	logger.BlockProtocolPeer("/test/1.0.0", dummyPeer)
	logger.BlockService("test-svc")
	logger.BlockServicePeer("test-svc", dummyPeer)
	logger.BlockMemory(1024)

	output := buf.String()
	expectedSubstrings := []string{
		"[WARNING] [ResourceManager] Connection blocked",
		"[WARNING] [ResourceManager] Stream blocked",
		"[WARNING] [ResourceManager] Peer connection blocked",
		"[WARNING] [ResourceManager] Protocol stream blocked",
		"[WARNING] [ResourceManager] Protocol peer stream blocked",
		"[WARNING] [ResourceManager] Service stream blocked",
		"[WARNING] [ResourceManager] Service peer stream blocked",
		"[WARNING] [ResourceManager] Memory reservation blocked",
	}

	for _, sub := range expectedSubstrings {
		if !strings.Contains(output, sub) {
			t.Errorf("Expected log output to contain %q, but got:\n%s", sub, output)
		}
	}
}
