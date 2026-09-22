package transport

import (
	"context"
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

	host.Close()
}

func TestResourceManagerActive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer kdht.Close()
	defer h.Close()

	rm := h.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected active ResourceManager on host, got nil")
	}
	if _, ok := rm.(*network.NullResourceManager); ok {
		t.Fatalf("Expected active ResourceManager on host, got NullResourceManager")
	}
}

func TestStreamFloodAttackLocal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Both hosts use default config (16 max streams per peer)
	hostB, kdhtB, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create hostB: %v", err)
	}
	defer kdhtB.Close()
	defer hostB.Close()

	doneChan := make(chan struct{})
	defer close(doneChan)

	hostB.SetStreamHandler("/cipher/test/1.0.0", func(s network.Stream) {
		<-doneChan
		s.Close()
	})

	hostA, kdhtA, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create hostA: %v", err)
	}
	defer kdhtA.Close()
	defer hostA.Close()

	err = hostA.Connect(ctx, peer.AddrInfo{
		ID:    hostB.ID(),
		Addrs: hostB.Addrs(),
	})
	if err != nil {
		t.Fatalf("Failed to connect hostA to hostB: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	var openStreams []network.Stream
	defer func() {
		for _, s := range openStreams {
			s.Reset()
		}
	}()

	successCount := 0
	rejectedCount := 0

	for i := 0; i < 25; i++ {
		s, err := hostA.NewStream(ctx, hostB.ID(), "/cipher/test/1.0.0")
		if err == nil {
			openStreams = append(openStreams, s)
			successCount++
		} else {
			rejectedCount++
		}
	}

	t.Logf("Streams opened: %d, rejected: %d", successCount, rejectedCount)

	if successCount > 16 {
		t.Errorf("Expected at most 16 successful streams per peer, got %d", successCount)
	}
	if rejectedCount == 0 {
		t.Errorf("Expected excess stream attempts to be rejected by ResourceManager, but 0 were rejected")
	}
}

func TestStreamFloodAttackRemote(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Host B enforces per-peer limit of 16
	hostB, kdhtB, err := NewNode(ctx, 0, 0, nil, "", false, WithPeerStreamLimit(16))
	if err != nil {
		t.Fatalf("Failed to create hostB: %v", err)
	}
	defer kdhtB.Close()
	defer hostB.Close()

	doneChan := make(chan struct{})
	defer close(doneChan)

	hostB.SetStreamHandler("/cipher/test/1.0.0", func(s network.Stream) {
		<-doneChan
		s.Close()
	})

	// Host A has higher stream limit (100) to act as flood sender
	hostA, kdhtA, err := NewNode(ctx, 0, 0, nil, "", false, WithPeerStreamLimit(100))
	if err != nil {
		t.Fatalf("Failed to create hostA: %v", err)
	}
	defer kdhtA.Close()
	defer hostA.Close()

	err = hostA.Connect(ctx, peer.AddrInfo{
		ID:    hostB.ID(),
		Addrs: hostB.Addrs(),
	})
	if err != nil {
		t.Fatalf("Failed to connect hostA to hostB: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	var openStreams []network.Stream
	defer func() {
		for _, s := range openStreams {
			s.Reset()
		}
	}()

	for i := 0; i < 25; i++ {
		s, err := hostA.NewStream(ctx, hostB.ID(), "/cipher/test/1.0.0")
		if err == nil {
			openStreams = append(openStreams, s)
		}
	}

	time.Sleep(100 * time.Millisecond)

	rmB := hostB.Network().ResourceManager()
	if stat, ok := rmB.(rcmgr.ResourceManagerState); ok {
		rmStat := stat.Stat()
		pStat := rmStat.Peers[hostA.ID()]
		t.Logf("Host B Stat for Peer A: NumStreamsInbound=%d, NumStreamsOutbound=%d", pStat.NumStreamsInbound, pStat.NumStreamsOutbound)
		if pStat.NumStreamsInbound > 16 {
			t.Errorf("Expected Host B inbound streams from Host A to be <= 16, got %d", pStat.NumStreamsInbound)
		}
	}
}

func TestCustomTransportConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	customConnLow := 10
	customConnHigh := 50
	customGrace := 5 * time.Second

	host, kdht, err := NewNode(
		ctx, 0, 0, nil, "", false,
		WithConnManager(customConnLow, customConnHigh, customGrace),
		WithConnLimits(80),
		WithPeerStreamLimit(12),
		WithMemoryLimit(256<<20),
	)
	if err != nil {
		t.Fatalf("Expected no error creating host with custom config, got %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	if host.Network().ResourceManager() == nil {
		t.Fatalf("Expected active ResourceManager with custom config")
	}
}
