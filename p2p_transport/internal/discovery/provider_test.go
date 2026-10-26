package discovery

import (
	"cipher/internal/content/core"
	"context"
	"runtime"
	"testing"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p"
)

func TestFindProviders_InvalidLimit(t *testing.T) {
	ctx := context.Background()
	var testID core.ContentID
	copy(testID[:], []byte("test_content_id_12345678901234"))

	_, err := FindProviders(ctx, nil, testID, 0)
	if err == nil {
		t.Fatal("expected error for limit <= 0, got nil")
	}

	_, err = FindProviders(ctx, nil, testID, -1)
	if err == nil {
		t.Fatal("expected error for limit < 0, got nil")
	}
}

func TestFindProviders_LimitReachedAndGoroutineCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Create bootstrapper
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

	// Create 3 provider nodes
	providers := make([]peer.AddrInfo, 3)
	dhts := make([]*dht.IpfsDHT, 3)
	var testID core.ContentID
	copy(testID[:], []byte("test_content_id_provider_drain"))

	for i := 0; i < 3; i++ {
		h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
		if err != nil {
			t.Fatalf("failed to create provider host %d: %v", i, err)
		}
		defer h.Close()

		kdht, err := NewDHT(h, dht.ModeServer)
		if err != nil {
			t.Fatalf("failed to create provider dht %d: %v", i, err)
		}
		defer kdht.Close()

		if err := Bootstrap(ctx, kdht, h, []peer.AddrInfo{h1Info}); err != nil {
			t.Fatalf("bootstrap failed for provider %d: %v", i, err)
		}

		if err := Provide(ctx, kdht, testID); err != nil {
			t.Fatalf("Provide failed for provider %d: %v", i, err)
		}

		providers[i] = peer.AddrInfo{ID: h.ID(), Addrs: h.Addrs()}
		dhts[i] = kdht
	}

	// Create client node
	hClient, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create client host: %v", err)
	}
	defer hClient.Close()

	dhtClient, err := NewDHT(hClient, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create client dht: %v", err)
	}
	defer dhtClient.Close()

	if err := Bootstrap(ctx, dhtClient, hClient, []peer.AddrInfo{h1Info}); err != nil {
		t.Fatalf("client bootstrap failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Perform FindProviders with limit 1, which triggers early loop exit
	found, err := FindProviders(ctx, dhtClient, testID, 1)
	if err != nil {
		t.Fatalf("FindProviders failed: %v", err)
	}

	if len(found) != 1 {
		t.Fatalf("expected 1 provider, got %d", len(found))
	}

	// Record baseline goroutine count after settling
	time.Sleep(200 * time.Millisecond)
	baselineGoroutines := runtime.NumGoroutine()

	// Run FindProviders repeatedly to verify goroutines are cleaned up and do not accumulate
	for i := 0; i < 10; i++ {
		res, err := FindProviders(ctx, dhtClient, testID, 1)
		if err != nil {
			t.Fatalf("iter %d: FindProviders failed: %v", i, err)
		}
		if len(res) != 1 {
			t.Fatalf("iter %d: expected 1 provider, got %d", i, len(res))
		}
	}

	// Give background goroutines a short window to terminate
	deadline := time.Now().Add(2 * time.Second)
	finalGoroutines := runtime.NumGoroutine()
	for finalGoroutines > baselineGoroutines+2 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		finalGoroutines = runtime.NumGoroutine()
	}

	if finalGoroutines > baselineGoroutines+2 {
		t.Fatalf("goroutine leak detected: baseline=%d, final=%d", baselineGoroutines, finalGoroutines)
	}
}
