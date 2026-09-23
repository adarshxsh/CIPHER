package discovery

import (
	"context"
	"sync"
	"testing"
	"time"

	"cipher/internal/content/core"

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

func (m *mockManifestStore) DeleteManifest(ctx context.Context, id core.ContentID) error {
	return nil
}

func TestNewDHTOptions(t *testing.T) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	kdht, err := NewDHT(h, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create NewDHT: %v", err)
	}
	defer kdht.Close()
}

func TestProvideRateLimiting(t *testing.T) {
	// Set rate limit to 5 per second, burst 1
	SetAnnouncementRateLimit(rate.Limit(5), 1)
	defer SetAnnouncementRateLimit(rate.Limit(10), 10) // Restore default

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	// Call Provide 3 times
	for i := 0; i < 3; i++ {
		var id core.ContentID
		id[0] = byte(i + 1)
		_ = Provide(ctx, kdht, id)
	}
	elapsed := time.Since(start)

	// First call takes token immediately.
	// 2nd call waits ~200ms, 3rd call waits ~200ms -> total >= ~350ms
	if elapsed < 350*time.Millisecond {
		t.Fatalf("expected Provide calls to be rate limited, but completed in %v", elapsed)
	}
}

func TestRepublisherThrottling(t *testing.T) {
	// Rate limit: 5 ops/sec, burst 1
	SetAnnouncementRateLimit(rate.Limit(5), 1)
	defer SetAnnouncementRateLimit(rate.Limit(10), 10)

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

	var ids []core.ContentID
	for i := 0; i < 4; i++ {
		var id core.ContentID
		id[0] = byte(i + 10)
		ids = append(ids, id)
	}
	store := &mockManifestStore{manifests: ids}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	republishAll(ctx, kdht, store)
	elapsed := time.Since(start)

	// 4 items at 5 items/sec (burst 1) => 3 waits of 200ms = ~600ms total
	if elapsed < 500*time.Millisecond {
		t.Fatalf("expected republishAll to be throttled, elapsed: %v", elapsed)
	}
}

func TestFindProvidersSemaphoreLimit(t *testing.T) {
	maxConcurrency := 2
	SetMaxLookupConcurrency(maxConcurrency)
	defer SetMaxLookupConcurrency(5)

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

	var peakConcurrency int32
	var mu sync.Mutex

	numRequests := 6
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			var id core.ContentID
			id[0] = byte(idx + 1)

			_, _ = FindProviders(ctx, kdht, id, 5)

			active := GetActiveLookups()
			mu.Lock()
			if active > peakConcurrency {
				peakConcurrency = active
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	if peakConcurrency > int32(maxConcurrency) {
		t.Fatalf("peak active lookups (%d) exceeded max concurrency (%d)", peakConcurrency, maxConcurrency)
	}
}
