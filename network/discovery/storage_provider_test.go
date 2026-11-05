package discovery

import (
	"cipher/internal/content/core"
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
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

type mockManifestStore struct {
	manifests []core.ContentID
}

func (m *mockManifestStore) GetManifestBytes(ctx context.Context, id core.ContentID) ([]byte, error) {
	return nil, nil
}

func (m *mockManifestStore) PutManifestBytes(ctx context.Context, id core.ContentID, data []byte) error {
	return nil
}

func (m *mockManifestStore) ListManifests(ctx context.Context) ([]core.ContentID, error) {
	return m.manifests, nil
}

func TestRepublishAll_ConcurrentRepublishing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

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
	if err := Bootstrap(ctx, dht2, h2, []peer.AddrInfo{h1Info}); err != nil {
		t.Fatalf("dht2 bootstrap failed: %v", err)
	}

	manifestCount := 20
	manifests := make([]core.ContentID, manifestCount)
	for i := 0; i < manifestCount; i++ {
		var id core.ContentID
		id[0] = byte(i + 1)
		id[31] = byte(i + 1)
		manifests[i] = id
	}

	store := &mockManifestStore{manifests: manifests}

	// Save original worker config and restore
	origWorkers := MaxRepublishWorkers
	MaxRepublishWorkers = 8
	defer func() { MaxRepublishWorkers = origWorkers }()

	republishAll(ctx, dht2, store)

	time.Sleep(300 * time.Millisecond)

	for i, id := range manifests {
		providers, err := FindProviders(ctx, dht1, id, 5)
		if err != nil {
			t.Fatalf("FindProviders for manifest %d failed: %v", i, err)
		}
		found := false
		for _, p := range providers {
			if p.ID == h2.ID() {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("manifest %d (%x) not found on dht1 from h2", i, id)
		}
	}
}

func TestRepublishAll_ContextCancellationFastHalt(t *testing.T) {
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

	manifestCount := 1000
	manifests := make([]core.ContentID, manifestCount)
	for i := 0; i < manifestCount; i++ {
		var id core.ContentID
		id[0] = byte((i % 255) + 1)
		id[1] = byte((i / 255) + 1)
		manifests[i] = id
	}

	store := &mockManifestStore{manifests: manifests}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		republishAll(ctx, dht1, store)
		done <- time.Since(start)
	}()

	// Allow workers to start then cancel context
	time.Sleep(10 * time.Millisecond)
	cancelStart := time.Now()
	cancel()

	select {
	case elapsed := <-done:
		cancelDuration := time.Since(cancelStart)
		t.Logf("republishAll halted in %v after cancel (total duration %v)", cancelDuration, elapsed)
		if cancelDuration > 100*time.Millisecond {
			t.Fatalf("expected republishAll to halt within 100ms on context cancel, took %v", cancelDuration)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("republishAll failed to return after context cancellation")
	}
}

func TestRepublishAll_PerRecordTimeoutAndErrorTolerance(t *testing.T) {
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

	manifestCount := 10
	manifests := make([]core.ContentID, manifestCount)
	for i := 0; i < manifestCount; i++ {
		var id core.ContentID
		id[0] = byte(i + 1)
		manifests[i] = id
	}

	store := &mockManifestStore{manifests: manifests}

	// Set ultra-short timeout to test sub-context timeout bound
	origTimeout := RepublishItemTimeout
	RepublishItemTimeout = 1 * time.Microsecond
	defer func() { RepublishItemTimeout = origTimeout }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	republishAll(ctx, dht1, store)
	duration := time.Since(start)

	// Even with 10 items, ultra short item timeout means it completes very quickly without blocking
	if duration > 2*time.Second {
		t.Fatalf("republishAll took too long (%v) with short item timeouts", duration)
	}
}
