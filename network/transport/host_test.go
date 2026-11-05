package transport

import (
	"context"
	"os"
	"path/filepath"
	"testing"

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

	if host == nil {
		t.Fatalf("Expected a host, got nil")
	}

	if len(host.Addrs()) == 0 {
		t.Fatalf("Expected at least one listen address")
	}

	host.Close()
}

func TestBuildProfileLimits(t *testing.T) {
	bootLimits := BuildProfileLimits(ProfileBootstrap)
	relayLimits := BuildProfileLimits(ProfileRelay)
	clientLimits := BuildProfileLimits(ProfileClient)
	defaultLimits := BuildProfileLimits(ProfileDefault)

	bootPartial := bootLimits.ToPartialLimitConfig()
	relayPartial := relayLimits.ToPartialLimitConfig()
	clientPartial := clientLimits.ToPartialLimitConfig()
	defaultPartial := defaultLimits.ToPartialLimitConfig()

	// Verify Bootstrap system limits > Client system limits
	if bootPartial.System.Conns <= clientPartial.System.Conns {
		t.Errorf("Expected Bootstrap conns limit (%d) > Client conns limit (%d)",
			bootPartial.System.Conns, clientPartial.System.Conns)
	}

	if bootPartial.System.Streams <= clientPartial.System.Streams {
		t.Errorf("Expected Bootstrap streams limit (%d) > Client streams limit (%d)",
			bootPartial.System.Streams, clientPartial.System.Streams)
	}

	if bootPartial.System.Memory <= clientPartial.System.Memory {
		t.Errorf("Expected Bootstrap memory limit (%d) > Client memory limit (%d)",
			bootPartial.System.Memory, clientPartial.System.Memory)
	}

	// Verify Relay system limits > Client system limits
	if relayPartial.System.Conns <= clientPartial.System.Conns {
		t.Errorf("Expected Relay conns limit (%d) > Client conns limit (%d)",
			relayPartial.System.Conns, clientPartial.System.Conns)
	}

	// Verify Default limits
	if defaultPartial.System.Conns <= clientPartial.System.Conns {
		t.Errorf("Expected Default conns limit (%d) > Client conns limit (%d)",
			defaultPartial.System.Conns, clientPartial.System.Conns)
	}
}

func TestNewNodeWithProfiles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	profiles := []NodeProfile{ProfileBootstrap, ProfileRelay, ProfileClient, ProfileDefault}

	for _, profile := range profiles {
		t.Run(string(profile), func(t *testing.T) {
			h, kdht, err := NewNode(ctx, 0, 0, nil, "", false, WithNodeProfile(profile))
			if err != nil {
				t.Fatalf("Failed to create host with profile %s: %v", profile, err)
			}
			defer kdht.Close()
			defer h.Close()

			if h == nil {
				t.Fatalf("Expected non-nil host for profile %s", profile)
			}
		})
	}
}

func TestNewNodeWithConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := ResourceLimitsConfig{
		Profile: ProfileBootstrap,
	}

	h, kdht, err := NewNodeWithConfig(ctx, 0, 0, nil, "", false, cfg)
	if err != nil {
		t.Fatalf("Failed to create host with NewNodeWithConfig: %v", err)
	}
	defer kdht.Close()
	defer h.Close()

	if h == nil {
		t.Fatalf("Expected non-nil host")
	}
}

func TestNewNodeWithJSONConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmpDir := t.TempDir()
	jsonPath := filepath.Join(tmpDir, "limits.json")

	// Write valid rcmgr JSON limit config
	jsonContent := `{
		"System": {
			"Conns": 128,
			"Streams": 512,
			"Memory": 67108864
		}
	}`
	if err := os.WriteFile(jsonPath, []byte(jsonContent), 0644); err != nil {
		t.Fatalf("Failed to write JSON config: %v", err)
	}

	h, kdht, err := NewNode(ctx, 0, 0, nil, "", false, WithLimitConfigFile(jsonPath))
	if err != nil {
		t.Fatalf("Failed to create host with JSON config file: %v", err)
	}
	defer kdht.Close()
	defer h.Close()

	if h == nil {
		t.Fatalf("Expected non-nil host")
	}
}

func TestResourceLimitsEnforcement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Create a host with strict limits
	var partialStrict rcmgr.PartialLimitConfig
	partialStrict.System = rcmgr.ResourceLimits{
		Conns:           10,
		ConnsInbound:    10,
		ConnsOutbound:   10,
		Streams:         3,
		StreamsInbound:  3,
		StreamsOutbound: 3,
		FD:              100,
		Memory:          32 << 20, // 32 MB
	}
	strictLimits := partialStrict.Build(rcmgr.DefaultLimits.AutoScale())

	strictHost, strictDHT, err := NewNode(ctx, 0, 0, nil, "", false, WithCustomLimits(strictLimits))
	if err != nil {
		t.Fatalf("Failed to create strict host: %v", err)
	}
	defer strictDHT.Close()
	defer strictHost.Close()

	rm := strictHost.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected non-nil resource manager on host")
	}

	// Verify stream/memory limit rejection
	dummyPeer := strictHost.ID()
	var streams []network.StreamManagementScope
	var limitErr error

	for i := 0; i < 10; i++ {
		s, err := rm.OpenStream(dummyPeer, network.DirInbound)
		if err != nil {
			limitErr = err
			break
		}
		if err := s.ReserveMemory(16<<20, 255); err != nil {
			limitErr = err
			s.Done()
			break
		}
		streams = append(streams, s)
	}

	defer func() {
		for _, s := range streams {
			s.Done()
		}
	}()

	if limitErr == nil {
		t.Fatalf("Expected resource manager to enforce limit, but all 10 succeeded")
	}

	t.Logf("Stream/Memory allocation correctly rejected by rcmgr: %v", limitErr)
}
