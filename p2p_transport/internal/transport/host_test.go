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

func TestSetupNetworkMonitor_TeardownOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	h, err := libp2p.New()
	if err != nil {
		t.Fatalf("Failed to create libp2p host: %v", err)
	}
	defer h.Close()

	goroutinesBefore := runtime.NumGoroutine()
	setupNetworkMonitor(ctx, h)

	time.Sleep(20 * time.Millisecond)
	goroutinesWithMonitor := runtime.NumGoroutine()

	// Cancel context to stop setupNetworkMonitor worker routine
	cancel()

	// Poll until goroutine exits
	success := false
	for i := 0; i < 50; i++ {
		time.Sleep(10 * time.Millisecond)
		if runtime.NumGoroutine() < goroutinesWithMonitor {
			success = true
			break
		}
	}

	if !success {
		t.Errorf("Expected network monitor goroutine to exit after context cancellation. Before: %d, With monitor: %d, After cancel: %d",
			goroutinesBefore, goroutinesWithMonitor, runtime.NumGoroutine())
	}
}
