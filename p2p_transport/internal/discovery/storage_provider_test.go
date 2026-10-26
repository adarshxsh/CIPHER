package discovery

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestStorageProviderDHTRegistration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 1. Create Bootstrapper DHT Node
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create h1: %v", err)
	}
	defer h1.Close()

	dht1, err := NewDHT(h1, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht1: %v", err)
	}
	defer dht1.Close()

	// 2. Create Storage Provider DHT Node
	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create h2: %v", err)
	}
	defer h2.Close()

	dht2, err := NewDHT(h2, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht2: %v", err)
	}
	defer dht2.Close()

	h1Info := peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}

	// Bootstrap h2 with h1
	if err := Bootstrap(ctx, dht2, h2, []peer.AddrInfo{h1Info}); err != nil {
		t.Fatalf("dht2 bootstrap failed: %v", err)
	}

	// 3. Register h2 as Storage Provider
	if err := RegisterStorageProvider(ctx, dht2); err != nil {
		t.Fatalf("RegisterStorageProvider failed: %v", err)
	}

	// 4. Create Publisher DHT Node
	h3, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create h3: %v", err)
	}
	defer h3.Close()

	dht3, err := NewDHT(h3, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht3: %v", err)
	}
	defer dht3.Close()

	// Bootstrap h3 with h1
	if err := Bootstrap(ctx, dht3, h3, []peer.AddrInfo{h1Info}); err != nil {
		t.Fatalf("dht3 bootstrap failed: %v", err)
	}

	// Give DHT record a moment to settle
	time.Sleep(300 * time.Millisecond)

	// 5. Query DHT for Storage Providers from h3
	discovered, err := FindStorageProviders(ctx, dht3, 5)
	if err != nil {
		t.Fatalf("FindStorageProviders failed: %v", err)
	}

	found := false
	for _, p := range discovered {
		if p.ID == h2.ID() {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("expected to discover storage provider %s, got %v", h2.ID(), discovered)
	}
}

func TestFindProviders_EarlyLimitTermination(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 1. Create Bootstrapper DHT Node
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create h1: %v", err)
	}
	defer h1.Close()

	dht1, err := NewDHT(h1, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht1: %v", err)
	}
	defer dht1.Close()

	h1Info := peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}

	// 2. Create Storage Provider DHT Node 1
	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create h2: %v", err)
	}
	defer h2.Close()

	dht2, err := NewDHT(h2, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht2: %v", err)
	}
	defer dht2.Close()

	if err := Bootstrap(ctx, dht2, h2, []peer.AddrInfo{h1Info}); err != nil {
		t.Fatalf("dht2 bootstrap failed: %v", err)
	}
	if err := RegisterStorageProvider(ctx, dht2); err != nil {
		t.Fatalf("RegisterStorageProvider h2 failed: %v", err)
	}

	// 3. Create Storage Provider DHT Node 2
	h3, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create h3: %v", err)
	}
	defer h3.Close()

	dht3, err := NewDHT(h3, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht3: %v", err)
	}
	defer dht3.Close()

	if err := Bootstrap(ctx, dht3, h3, []peer.AddrInfo{h1Info}); err != nil {
		t.Fatalf("dht3 bootstrap failed: %v", err)
	}
	if err := RegisterStorageProvider(ctx, dht3); err != nil {
		t.Fatalf("RegisterStorageProvider h3 failed: %v", err)
	}

	// 4. Create Query DHT Node
	h4, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create h4: %v", err)
	}
	defer h4.Close()

	dht4, err := NewDHT(h4, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht4: %v", err)
	}
	defer dht4.Close()

	if err := Bootstrap(ctx, dht4, h4, []peer.AddrInfo{h1Info}); err != nil {
		t.Fatalf("dht4 bootstrap failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Test invalid limit check
	_, err = FindProviders(ctx, dht4, StorageProviderNamespace, 0)
	if err == nil {
		t.Fatalf("expected error for PROVIDER_LIMIT = 0, got nil")
	}

	// Record baseline goroutine count
	beforeGoroutines := runtime.NumGoroutine()

	// Query with limit = 1 when 2 providers exist
	discovered, err := FindStorageProviders(ctx, dht4, 1)
	if err != nil {
		t.Fatalf("FindStorageProviders failed: %v", err)
	}

	if len(discovered) != 1 {
		t.Fatalf("expected exactly 1 provider, got %d", len(discovered))
	}

	// Allow goroutine drain to complete
	time.Sleep(500 * time.Millisecond)
	afterGoroutines := runtime.NumGoroutine()

	if afterGoroutines > beforeGoroutines+5 {
		t.Fatalf("potential goroutine leak: before %d, after %d", beforeGoroutines, afterGoroutines)
	}
}
