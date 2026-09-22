package discovery

import (
	"cipher/internal/content/core"
	"context"
	"testing"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
)

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

func TestNewDHTWithCustomOptions(t *testing.T) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	dhtNode, err := NewDHT(h, dht.ModeServer, dht.Concurrency(2))
	if err != nil {
		t.Fatalf("failed to create DHT with custom options: %v", err)
	}
	defer dhtNode.Close()
}

func TestRateLimitedProvideAndLookupContextCancellation(t *testing.T) {
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
		t.Fatalf("bootstrap failed: %v", err)
	}

	mgr := NewManager(Config{
		RepublishRate:  0.1, // Very slow rate
		RepublishBurst: 1,
		LookupRate:     0.1,
		LookupBurst:    1,
	})

	// First provide consumes the single burst token
	var cid1 core.ContentID
	cid1[0] = 0x01
	if err := mgr.Provide(ctx, dht2, cid1); err != nil {
		t.Fatalf("first provide failed: %v", err)
	}

	// Second provide with canceled context should fail immediately due to rate limiter wait
	canceledCtx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()

	var cid2 core.ContentID
	cid2[0] = 0x02
	if err := mgr.Provide(canceledCtx, dht2, cid2); err == nil {
		t.Fatalf("expected rate limiter error on canceled context, got nil")
	}

	// First lookup consumes burst token
	if _, err := mgr.FindProviders(ctx, dht2, cid1, 5); err != nil {
		t.Fatalf("first lookup failed: %v", err)
	}

	// Second lookup with canceled context should fail immediately
	if _, err := mgr.FindProviders(canceledCtx, dht2, cid1, 5); err == nil {
		t.Fatalf("expected rate limiter error on canceled context for lookup, got nil")
	}
}

func TestRepublishAllWorkerPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
		t.Fatalf("bootstrap failed: %v", err)
	}

	mgr := NewManager(Config{
		RepublishRate:    100.0, // fast for testing
		RepublishBurst:   50,
		RepublishWorkers: 2,
	})

	numManifests := 5
	manifests := make([]core.ContentID, numManifests)
	for i := 0; i < numManifests; i++ {
		manifests[i][0] = byte(i + 1)
	}

	store := &mockManifestStore{manifests: manifests}
	mgr.republishAll(ctx, dht2, store)
}
