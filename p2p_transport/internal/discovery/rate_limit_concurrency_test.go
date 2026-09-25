package discovery

import (
	"cipher/internal/content/core"
	"context"
	"strings"
	"testing"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p"
	"golang.org/x/time/rate"
)

type mockManifestStore struct {
	manifests []core.ContentID
}

func (m *mockManifestStore) ListManifests(ctx context.Context) ([]core.ContentID, error) {
	return m.manifests, nil
}

func (m *mockManifestStore) GetManifestBytes(ctx context.Context, id core.ContentID) ([]byte, error) {
	return nil, nil
}

func (m *mockManifestStore) PutManifestBytes(ctx context.Context, id core.ContentID, data []byte) error {
	return nil
}

func (m *mockManifestStore) HasManifest(ctx context.Context, id core.ContentID) (bool, error) {
	return true, nil
}

func (m *mockManifestStore) DeleteManifest(ctx context.Context, id core.ContentID) error {
	return nil
}

func TestNewDHT_FunctionalOptions(t *testing.T) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	// Test 1: NewDHT with query concurrency and standard bucket size (20)
	kdht1, err := NewDHT(
		h,
		dht.ModeServer,
		WithQueryConcurrency(5),
		WithRoutingTableLimit(20),
		WithDHTOption(dht.DisableAutoRefresh()),
	)
	if err != nil {
		t.Fatalf("failed to create DHT with options: %v", err)
	}
	defer kdht1.Close()

	// Test 2: NewDHT with custom protocol prefix and custom bucket size
	kdht2, err := NewDHT(
		h,
		dht.ModeServer,
		WithQueryConcurrency(3),
		WithBucketSize(10),
		WithDHTOption(dht.ProtocolPrefix("/cipher")),
		WithDHTOption(dht.DisableAutoRefresh()),
	)
	if err != nil {
		t.Fatalf("failed to create DHT with custom prefix and bucket size: %v", err)
	}
	defer kdht2.Close()
}

func TestFindProviders_LimitValidationAndCap(t *testing.T) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	kdht, err := NewDHT(h, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht: %v", err)
	}
	defer kdht.Close()

	ctx := context.Background()
	dummyID := core.ContentID{1, 2, 3}

	// 1. PROVIDER_LIMIT <= 0 should fail
	_, err = FindProviders(ctx, kdht, dummyID, 0)
	if err == nil || !strings.Contains(err.Error(), "greater than zero") {
		t.Fatalf("expected error for limit <= 0, got: %v", err)
	}

	_, err = FindProviders(ctx, kdht, dummyID, -5)
	if err == nil || !strings.Contains(err.Error(), "greater than zero") {
		t.Fatalf("expected error for limit <= 0, got: %v", err)
	}

	// 2. PROVIDER_LIMIT > 100 should be capped to MaxProviderLimit (100)
	// (Since no peers provide dummyID, it will return empty slice without error)
	providers, err := FindProviders(ctx, kdht, dummyID, 150)
	if err != nil {
		t.Fatalf("unexpected error for capped limit: %v", err)
	}
	if len(providers) > MaxProviderLimit {
		t.Fatalf("expected at most %d providers, got %d", MaxProviderLimit, len(providers))
	}
}

func TestFindProviders_SemaphoreConcurrency(t *testing.T) {
	SetMaxConcurrentLookups(2)
	defer SetMaxConcurrentLookups(DefaultLookupConcurrency)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Acquire all slots (2 slots)
	release1, err := acquireLookupSemaphore(ctx)
	if err != nil {
		t.Fatalf("failed to acquire slot 1: %v", err)
	}

	release2, err := acquireLookupSemaphore(ctx)
	if err != nil {
		t.Fatalf("failed to acquire slot 2: %v", err)
	}

	// Now 3rd attempt with short timeout should fail/time out
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer shortCancel()

	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	kdht, err := NewDHT(h, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht: %v", err)
	}
	defer kdht.Close()

	_, err = FindProviders(shortCtx, kdht, core.ContentID{1}, 5)
	if err == nil {
		t.Fatalf("expected error when concurrency limit reached, got nil")
	}

	// Release 1 slot
	release1()

	// Now a new query with valid context should acquire the freed slot
	quickCtx, quickCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer quickCancel()

	_, err = FindProviders(quickCtx, kdht, core.ContentID{1}, 5)
	if err != nil {
		t.Fatalf("expected FindProviders to succeed after slot release, got: %v", err)
	}

	release2()
}

func TestStartRepublisher_RateLimiting(t *testing.T) {
	// Temporarily disable direct Provide rate limit so we strictly measure republishAll rate limiter
	SetProvideRateLimit(0, 0)
	defer SetProvideRateLimit(DefaultRepublishRate, DefaultRepublishBurst)

	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	kdht, err := NewDHT(h, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht: %v", err)
	}
	defer kdht.Close()

	// 15 manifests: burst of 5, rate of 10/sec -> first 5 instant, remaining 10 take 1 second
	var manifests []core.ContentID
	for i := 0; i < 15; i++ {
		var id core.ContentID
		id[0] = byte(i + 1)
		manifests = append(manifests, id)
	}
	store := &mockManifestStore{manifests: manifests}

	limiter := rate.NewLimiter(rate.Limit(10.0), 5) // burst = 5

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	republishAll(ctx, kdht, store, limiter)
	duration := time.Since(start)

	// 15 items with burst 5 @ 10/sec means 10 items wait 100ms each = 1.0s total expected delay
	if duration < 800*time.Millisecond {
		t.Fatalf("republishAll completed too quickly (%v), expected rate limiting of ~1s", duration)
	}
}
