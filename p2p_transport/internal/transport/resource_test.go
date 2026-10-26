package transport_test

import (
	"context"
	"testing"

	"cipher/internal/transport"

	"github.com/libp2p/go-libp2p/core/network"
)

func TestNewResourceManager_Defaults(t *testing.T) {
	rm, err := transport.NewResourceManager(nil)
	if err != nil {
		t.Fatalf("NewResourceManager failed: %v", err)
	}
	if rm == nil {
		t.Fatal("Expected non-nil ResourceManager")
	}
	defer rm.Close()
}

func TestNewNode_ResourceManagerIntegration(t *testing.T) {
	ctx := context.Background()
	h, kdht, err := transport.NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	defer h.Close()
	defer kdht.Close()

	// Verify host Network has a valid ResourceManager
	if h.Network().ResourceManager() == nil {
		t.Fatal("Expected host network to have a non-nil ResourceManager")
	}
}

func TestResourceManager_MemoryLimitEnforcement(t *testing.T) {
	limits := transport.DefaultScalingLimits()
	// Set low system memory limit: 1 KB
	limits.SystemBaseLimit.Memory = 1024
	limits.SystemLimitIncrease.Memory = 0

	rm, err := transport.NewResourceManager(&limits)
	if err != nil {
		t.Fatalf("Failed to create ResourceManager with custom limits: %v", err)
	}
	defer rm.Close()

	// Open a scope in system via connection scope
	s, err := rm.OpenConnection(network.DirInbound, false, nil)
	if err != nil {
		t.Fatalf("Failed to open connection scope: %v", err)
	}
	defer s.Done()

	// 512 bytes should succeed (within 1024 B limit)
	if err := s.ReserveMemory(512, 255); err != nil {
		t.Errorf("Expected 512B reservation to succeed, got: %v", err)
	}

	// Reserving another 1000 bytes (total 1512 B > 1024 B) should fail
	if err := s.ReserveMemory(1000, 255); err == nil {
		t.Error("Expected memory limit exceeded error when exceeding memory limit, got nil")
	}

	// Release 512 bytes
	s.ReleaseMemory(512)

	// 512 bytes reservation should succeed again
	if err := s.ReserveMemory(512, 255); err != nil {
		t.Errorf("Expected reservation to succeed after memory release, got: %v", err)
	}
}
