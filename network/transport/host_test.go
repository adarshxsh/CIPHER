package transport

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
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

func TestSetupNetworkMonitor_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	h, err := libp2p.New()
	if err != nil {
		t.Fatalf("Failed to create libp2p host: %v", err)
	}
	defer h.Close()

	// Wait for libp2p internal background goroutines to settle
	time.Sleep(200 * time.Millisecond)
	initialGoroutines := runtime.NumGoroutine()

	setupNetworkMonitor(ctx, h)

	// Wait for setupNetworkMonitor goroutine to start
	time.Sleep(50 * time.Millisecond)
	goroutinesWithMonitor := runtime.NumGoroutine()

	if goroutinesWithMonitor <= initialGoroutines {
		t.Fatalf("Expected goroutine count to increase after setupNetworkMonitor, initial: %d, current: %d", initialGoroutines, goroutinesWithMonitor)
	}

	cancel()

	deadline := time.Now().Add(2 * time.Second)
	cleanedUp := false
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() < goroutinesWithMonitor {
			cleanedUp = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !cleanedUp {
		t.Fatalf("Expected network monitor goroutine to terminate after context cancellation, before cancel: %d, after cancel: %d", goroutinesWithMonitor, runtime.NumGoroutine())
	}
}
