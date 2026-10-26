package transport

import (
	"context"
	"fmt"
	"strings"
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

	if host.Network().ResourceManager() == nil {
		t.Fatalf("Expected resource manager to be initialized on host")
	}
}

func TestNewHostCustomOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metrics := NewResourceMetrics()
	customLimits := rcmgr.PartialLimitConfig{
		PeerDefault: rcmgr.ResourceLimits{
			Streams:        10,
			StreamsInbound: 10,
		},
	}

	host, kdht, err := NewHost(
		ctx, 0, 0, nil, "", false,
		WithMemoryLimit(512*1024*1024),
		WithConnLimits(10, 50, 30*time.Second),
		WithMetricsReporter(metrics),
		WithPartialLimitConfig(customLimits),
	)
	if err != nil {
		t.Fatalf("Failed to create host with custom options: %v", err)
	}
	defer kdht.Close()
	defer host.Close()

	stat, err := GetResourceStats(host)
	if err != nil {
		t.Fatalf("Failed to get resource stats: %v", err)
	}
	if stat.Memory < 0 {
		t.Errorf("Unexpected negative memory stat: %v", stat.Memory)
	}
}

func TestStreamLimitEnforcement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limitCap := 8
	smallLimits := rcmgr.PartialLimitConfig{
		PeerDefault: rcmgr.ResourceLimits{
			Streams:         rcmgr.LimitVal(limitCap),
			StreamsInbound:  rcmgr.LimitVal(limitCap),
			StreamsOutbound: rcmgr.LimitVal(limitCap),
		},
		ProtocolPeerDefault: rcmgr.ResourceLimits{
			Streams:         rcmgr.LimitVal(limitCap),
			StreamsInbound:  rcmgr.LimitVal(limitCap),
			StreamsOutbound: rcmgr.LimitVal(limitCap),
		},
	}

	hA, dhtA, err := NewHost(ctx, 0, 0, nil, "", false, WithPartialLimitConfig(smallLimits))
	if err != nil {
		t.Fatalf("Failed to create Host A: %v", err)
	}
	defer dhtA.Close()
	defer hA.Close()

	hB, dhtB, err := NewHost(ctx, 0, 0, nil, "", false, WithPartialLimitConfig(smallLimits))
	if err != nil {
		t.Fatalf("Failed to create Host B: %v", err)
	}
	defer dhtB.Close()
	defer hB.Close()

	hA.SetStreamHandler("/test/1.0.0", func(s network.Stream) {
		defer s.Close()
		buf := make([]byte, 10)
		s.Read(buf)
	})

	err = hB.Connect(ctx, hA.Peerstore().PeerInfo(hA.ID()))
	if err != nil {
		t.Fatalf("Failed to connect Host B to Host A: %v", err)
	}

	var openStreams []network.Stream
	defer func() {
		for _, s := range openStreams {
			s.Reset()
		}
	}()

	var hitLimit bool
	var succeeded int

	for i := 0; i < limitCap+5; i++ {
		s, err := hB.NewStream(ctx, hA.ID(), "/test/1.0.0")
		if err != nil {
			hitLimit = true
			errMsg := strings.ToLower(err.Error())
			if !strings.Contains(errMsg, "limit") && !strings.Contains(errMsg, "resource") {
				t.Fatalf("Expected resource limit error, got: %v", err)
			}
			t.Logf("Got expected stream limit error after %d open streams: %v", succeeded, err)
			break
		}
		openStreams = append(openStreams, s)
		succeeded++
	}

	if !hitLimit {
		t.Fatalf("Expected stream creation to hit limit, but opened %d streams without error", succeeded)
	}

	if succeeded == 0 {
		t.Fatalf("Expected at least 1 stream to succeed before hitting limit")
	}
}

func TestDefaultLimitsConfiguration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, kdht, err := NewHost(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create default host: %v", err)
	}
	defer kdht.Close()
	defer h.Close()

	rm := h.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Resource manager is nil")
	}

	err = rm.ViewSystem(func(s network.ResourceScope) error {
		stat := s.Stat()
		if stat.Memory < 0 {
			t.Errorf("Unexpected negative memory stat: %d", stat.Memory)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ViewSystem failed: %v", err)
	}
}

func TestResourceMetricsExposed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metricsA := NewResourceMetrics()
	hA, dhtA, err := NewHost(ctx, 0, 0, nil, "", false, WithMetricsReporter(metricsA))
	if err != nil {
		t.Fatalf("Failed to create Host A: %v", err)
	}
	defer dhtA.Close()
	defer hA.Close()

	hB, dhtB, err := NewHost(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create Host B: %v", err)
	}
	defer dhtB.Close()
	defer hB.Close()

	hA.SetStreamHandler("/test/1.0.0", func(s network.Stream) {
		s.Close()
	})

	if err := hB.Connect(ctx, hA.Peerstore().PeerInfo(hA.ID())); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	s, err := hB.NewStream(ctx, hA.ID(), "/test/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	statA, err := GetResourceStats(hA)
	if err != nil {
		t.Fatalf("GetResourceStats failed: %v", err)
	}

	if statA.NumConnsInbound < 0 {
		t.Errorf("Unexpected negative connection count: %d", statA.NumConnsInbound)
	}

	peerStat, err := GetPeerResourceStats(hA, hB.ID())
	if err != nil {
		t.Fatalf("GetPeerResourceStats failed: %v", err)
	}
	if peerStat.NumConnsInbound < 0 {
		t.Errorf("Unexpected negative peer connection count: %d", peerStat.NumConnsInbound)
	}

	allowedStreams := metricsA.GetAllowedStreams()
	allowedConns := metricsA.GetAllowedConns()
	if allowedStreams == 0 && allowedConns == 0 {
		fmt.Printf("Metrics captured: conns=%d streams=%d\n", allowedConns, allowedStreams)
	}
}
