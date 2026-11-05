package transport

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
)

func TestParseMemoryString(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		hasErr   bool
	}{
		{"", 0, false},
		{"512MB", 512 * 1024 * 1024, false},
		{"1GB", 1 * 1024 * 1024 * 1024, false},
		{"256MiB", 256 * 1024 * 1024, false},
		{"1024", 1024, false},
		{"1GiB", 1 * 1024 * 1024 * 1024, false},
		{"64KB", 64 * 1024, false},
		{"invalid", 0, true},
		{"-500MB", 0, true},
		{"0MB", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseMemoryString(tt.input)
			if tt.hasErr {
				if err == nil {
					t.Errorf("Expected error for %q, got none (value: %d)", tt.input, got)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for %q: %v", tt.input, err)
				}
				if got != tt.expected {
					t.Errorf("For %q expected %d, got %d", tt.input, tt.expected, got)
				}
			}
		})
	}
}

func TestParseNodeRole(t *testing.T) {
	tests := []struct {
		input    string
		expected NodeRole
		hasErr   bool
	}{
		{"", RoleDefault, false},
		{"default", RoleDefault, false},
		{"peer", RoleDefault, false},
		{"bootstrap", RoleBootstrap, false},
		{"relay", RoleRelay, false},
		{"client", RoleClient, false},
		{"publisher", RolePublisher, false},
		{"provider", RoleProvider, false},
		{"CLIENT", RoleClient, false},
		{" Bootstrap ", RoleBootstrap, false},
		{"invalid_role", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseNodeRole(tt.input)
			if tt.hasErr {
				if err == nil {
					t.Errorf("Expected error for role %q, got none", tt.input)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for role %q: %v", tt.input, err)
				}
				if got != tt.expected {
					t.Errorf("For role %q expected %s, got %s", tt.input, tt.expected, got)
				}
			}
		})
	}
}

func TestLoadResourceConfigFromEnv(t *testing.T) {
	origRole := os.Getenv("CIPHER_ROLE")
	origMem := os.Getenv("CIPHER_MAX_MEMORY")
	origConns := os.Getenv("CIPHER_MAX_CONNS")
	origFD := os.Getenv("CIPHER_MAX_FD")
	origJSON := os.Getenv("CIPHER_RESOURCE_CONFIG")

	defer func() {
		os.Setenv("CIPHER_ROLE", origRole)
		os.Setenv("CIPHER_MAX_MEMORY", origMem)
		os.Setenv("CIPHER_MAX_CONNS", origConns)
		os.Setenv("CIPHER_MAX_FD", origFD)
		os.Setenv("CIPHER_RESOURCE_CONFIG", origJSON)
	}()

	t.Run("Valid env overrides", func(t *testing.T) {
		os.Setenv("CIPHER_ROLE", "bootstrap")
		os.Setenv("CIPHER_MAX_MEMORY", "1GB")
		os.Setenv("CIPHER_MAX_CONNS", "500")
		os.Setenv("CIPHER_MAX_FD", "2048")
		os.Unsetenv("CIPHER_RESOURCE_CONFIG")

		cfg, err := LoadResourceConfigFromEnv()
		if err != nil {
			t.Fatalf("LoadResourceConfigFromEnv failed: %v", err)
		}

		if cfg.Role != RoleBootstrap {
			t.Errorf("Expected role Bootstrap, got %s", cfg.Role)
		}
		if cfg.MaxMemory != 1*1024*1024*1024 {
			t.Errorf("Expected 1GB memory, got %d", cfg.MaxMemory)
		}
		if cfg.MaxConns != 500 {
			t.Errorf("Expected 500 max conns, got %d", cfg.MaxConns)
		}
		if cfg.MaxFD != 2048 {
			t.Errorf("Expected 2048 max FDs, got %d", cfg.MaxFD)
		}
	})

	t.Run("Invalid memory env returns error", func(t *testing.T) {
		os.Setenv("CIPHER_ROLE", "default")
		os.Setenv("CIPHER_MAX_MEMORY", "invalid_mem")
		os.Unsetenv("CIPHER_MAX_CONNS")

		_, err := LoadResourceConfigFromEnv()
		if err == nil {
			t.Fatal("Expected error for invalid CIPHER_MAX_MEMORY, got nil")
		}
	})

	t.Run("Invalid conns env returns error", func(t *testing.T) {
		os.Setenv("CIPHER_ROLE", "default")
		os.Setenv("CIPHER_MAX_MEMORY", "256MB")
		os.Setenv("CIPHER_MAX_CONNS", "-100")

		_, err := LoadResourceConfigFromEnv()
		if err == nil {
			t.Fatal("Expected error for negative CIPHER_MAX_CONNS, got nil")
		}
	})

	t.Run("Invalid role env returns error", func(t *testing.T) {
		os.Setenv("CIPHER_ROLE", "unknown_role_xyz")
		os.Unsetenv("CIPHER_MAX_MEMORY")
		os.Unsetenv("CIPHER_MAX_CONNS")

		_, err := LoadResourceConfigFromEnv()
		if err == nil {
			t.Fatal("Expected error for invalid CIPHER_ROLE, got nil")
		}
	})
}

func TestProfileDefaults(t *testing.T) {
	profiles := []NodeRole{RoleBootstrap, RoleRelay, RoleClient, RoleDefault}

	for _, role := range profiles {
		t.Run(string(role), func(t *testing.T) {
			cfg := DefaultProfileConfig(role)
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Profile %s validation failed: %v", role, err)
			}
			if cfg.MaxMemory <= 0 {
				t.Errorf("Profile %s has non-positive MaxMemory: %d", role, cfg.MaxMemory)
			}
			if cfg.MaxConns <= 0 {
				t.Errorf("Profile %s has non-positive MaxConns: %d", role, cfg.MaxConns)
			}

			rm, err := cfg.BuildResourceManager()
			if err != nil {
				t.Fatalf("Failed to build resource manager for profile %s: %v", role, err)
			}
			if rm == nil {
				t.Fatalf("Resource manager is nil for profile %s", role)
			}
		})
	}

	clientCfg := DefaultProfileConfig(RoleClient)
	bootstrapCfg := DefaultProfileConfig(RoleBootstrap)

	if clientCfg.MaxMemory >= bootstrapCfg.MaxMemory {
		t.Errorf("Client memory (%d) should be less than Bootstrap memory (%d)", clientCfg.MaxMemory, bootstrapCfg.MaxMemory)
	}
	if clientCfg.MaxConns >= bootstrapCfg.MaxConns {
		t.Errorf("Client conns (%d) should be less than Bootstrap conns (%d)", clientCfg.MaxConns, bootstrapCfg.MaxConns)
	}
}

func TestJSONConfigLoading(t *testing.T) {
	tmpDir := t.TempDir()
	jsonFile := filepath.Join(tmpDir, "limits.json")

	jsonContent := `{
		"System": {
			"Memory": 268435456,
			"Conns": 50,
			"ConnsInbound": 40,
			"ConnsOutbound": 10,
			"Streams": 100
		}
	}`

	if err := os.WriteFile(jsonFile, []byte(jsonContent), 0600); err != nil {
		t.Fatalf("Failed to write test JSON file: %v", err)
	}

	resCfg := ResourceConfig{
		Role:           RoleClient,
		JSONConfigPath: jsonFile,
	}

	rm, err := resCfg.BuildResourceManager()
	if err != nil {
		t.Fatalf("Failed to build resource manager from JSON config: %v", err)
	}
	if rm == nil {
		t.Fatal("Resource manager from JSON config is nil")
	}
}

func TestProtocolLimitsEnforcement(t *testing.T) {
	resCfg := DefaultProfileConfig(RoleDefault)
	rm, err := resCfg.BuildResourceManager()
	if err != nil {
		t.Fatalf("Failed to build resource manager: %v", err)
	}

	chunkProto := protocol.ID("/cipher/chunk/1.0.0")
	testPeerID, err := peer.Decode("12D3KooWTestPeerID11111111111111111111111111111111111")
	if err != nil {
		// If test peer decoding fails, generate or use peer.ID("testpeer")
		testPeerID = peer.ID("testpeer")
	}

	sScope, err := rm.OpenStream(testPeerID, network.DirInbound)
	if err != nil {
		t.Fatalf("Failed to open stream scope: %v", err)
	}
	defer sScope.Done()

	err = sScope.SetProtocol(chunkProto)
	if err != nil {
		t.Fatalf("Failed to set protocol /cipher/chunk/1.0.0 on stream scope: %v", err)
	}

	err = sScope.ReserveMemory(1024*1024, network.ReservationPriorityAlways)
	if err != nil {
		t.Fatalf("Failed to reserve memory for /cipher/chunk/1.0.0: %v", err)
	}
	sScope.ReleaseMemory(1024 * 1024)
}

func TestConnectionFloodProtection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tightCfg := ResourceConfig{
		Role:          RoleClient,
		MaxMemory:     64 * 1024 * 1024,
		MaxConns:      2,
		InboundConns:  1,
		OutboundConns: 1,
		PeerConnLimit: 1,
	}

	hA, _, err := NewNode(ctx, 0, 0, nil, "", false, WithResourceConfig(tightCfg))
	if err != nil {
		t.Fatalf("Failed to create host A: %v", err)
	}
	defer hA.Close()

	sScope, err := hA.Network().ResourceManager().OpenConnection(network.DirInbound, false, multiaddr.StringCast("/ip4/127.0.0.1/tcp/1234"))
	if err != nil {
		t.Fatalf("Failed to open connection scope: %v", err)
	}

	err = sScope.ReserveMemory(100*1024*1024, network.ReservationPriorityHigh)
	if err == nil {
		t.Fatal("Expected memory limit error when reserving 100MB on 64MB host, got nil")
	}
	sScope.Done()
}

func TestNewNodeWithNodeOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, kdht, err := NewNode(
		ctx, 0, 0, nil, "", false,
		WithRole(RoleBootstrap),
		WithMemoryLimit(1024*1024*1024),
		WithConnLimits(100, 500),
		WithPeerStreamLimit(256),
	)
	if err != nil {
		t.Fatalf("NewNode with options failed: %v", err)
	}
	defer h.Close()
	defer kdht.Close()

	if h.Network().ResourceManager() == nil {
		t.Fatal("Expected host resource manager to be non-nil")
	}
}
