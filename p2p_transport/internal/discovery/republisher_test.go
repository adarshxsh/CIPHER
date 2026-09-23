package discovery

import (
	"cipher/internal/content/core"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
)

type testManifestStore struct {
	manifests []core.ContentID
}

func (m *testManifestStore) GetManifestBytes(ctx context.Context, id core.ContentID) ([]byte, error) {
	return nil, nil
}

func (m *testManifestStore) PutManifestBytes(ctx context.Context, id core.ContentID, data []byte) error {
	return nil
}

func (m *testManifestStore) ListManifests(ctx context.Context) ([]core.ContentID, error) {
	return m.manifests, nil
}

func generateManifests(count int) []core.ContentID {
	result := make([]core.ContentID, count)
	for i := 0; i < count; i++ {
		var id core.ContentID
		id[0] = byte(i >> 24)
		id[1] = byte(i >> 16)
		id[2] = byte(i >> 8)
		id[3] = byte(i)
		id[31] = 0xFF
		result[i] = id
	}
	return result
}

func TestRepublisher_BoundedWorkerPool(t *testing.T) {
	origWorkers := MaxRepublishWorkers
	origTimeout := RepublishItemTimeout
	origProvideFn := provideFn
	defer func() {
		MaxRepublishWorkers = origWorkers
		RepublishItemTimeout = origTimeout
		provideFn = origProvideFn
	}()

	MaxRepublishWorkers = 10
	RepublishItemTimeout = 5 * time.Second

	manifestCount := 50
	store := &testManifestStore{manifests: generateManifests(manifestCount)}

	var currentWorkers int32
	var peakWorkers int32
	var processedCount int32

	provideFn = func(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID) error {
		cur := atomic.AddInt32(&currentWorkers, 1)
		for {
			peak := atomic.LoadInt32(&peakWorkers)
			if cur <= peak || atomic.CompareAndSwapInt32(&peakWorkers, peak, cur) {
				break
			}
		}

		time.Sleep(20 * time.Millisecond)

		atomic.AddInt32(&processedCount, 1)
		atomic.AddInt32(&currentWorkers, -1)
		return nil
	}

	ctx := context.Background()
	start := time.Now()
	republishAll(ctx, nil, store)
	elapsed := time.Since(start)

	if atomic.LoadInt32(&processedCount) != int32(manifestCount) {
		t.Fatalf("expected %d manifests processed, got %d", manifestCount, processedCount)
	}

	peak := atomic.LoadInt32(&peakWorkers)
	if peak > int32(MaxRepublishWorkers) {
		t.Fatalf("peak concurrent workers %d exceeded limit of %d", peak, MaxRepublishWorkers)
	}
	if peak != int32(MaxRepublishWorkers) {
		t.Fatalf("expected peak workers to reach max limit %d, got %d", MaxRepublishWorkers, peak)
	}

	t.Logf("Processed %d manifests in %v with peak workers %d", manifestCount, elapsed, peak)
}

func TestRepublisher_PerItemTimeout(t *testing.T) {
	origWorkers := MaxRepublishWorkers
	origTimeout := RepublishItemTimeout
	origProvideFn := provideFn
	defer func() {
		MaxRepublishWorkers = origWorkers
		RepublishItemTimeout = origTimeout
		provideFn = origProvideFn
	}()

	MaxRepublishWorkers = 5
	RepublishItemTimeout = 50 * time.Millisecond

	manifests := generateManifests(5)
	store := &testManifestStore{manifests: manifests}

	slowManifestID := manifests[2]
	var timedOutCount int32
	var successCount int32

	provideFn = func(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID) error {
		if id == slowManifestID {
			select {
			case <-time.After(200 * time.Millisecond):
				return nil
			case <-ctx.Done():
				atomic.AddInt32(&timedOutCount, 1)
				return ctx.Err()
			}
		}
		atomic.AddInt32(&successCount, 1)
		return nil
	}

	ctx := context.Background()
	start := time.Now()
	republishAll(ctx, nil, store)
	elapsed := time.Since(start)

	if atomic.LoadInt32(&timedOutCount) != 1 {
		t.Fatalf("expected 1 item to time out, got %d", timedOutCount)
	}
	if atomic.LoadInt32(&successCount) != 4 {
		t.Fatalf("expected 4 items to succeed, got %d", successCount)
	}

	if elapsed > 150*time.Millisecond {
		t.Fatalf("expected republishAll to complete fast via item timeout, took %v", elapsed)
	}
}

func TestRepublisher_RootContextCancellation(t *testing.T) {
	origWorkers := MaxRepublishWorkers
	origTimeout := RepublishItemTimeout
	origProvideFn := provideFn
	defer func() {
		MaxRepublishWorkers = origWorkers
		RepublishItemTimeout = origTimeout
		provideFn = origProvideFn
	}()

	MaxRepublishWorkers = 10
	RepublishItemTimeout = 10 * time.Second

	manifests := generateManifests(100)
	store := &testManifestStore{manifests: manifests}

	var activeCount int32
	var startedCount int32

	provideFn = func(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID) error {
		atomic.AddInt32(&startedCount, 1)
		atomic.AddInt32(&activeCount, 1)
		defer atomic.AddInt32(&activeCount, -1)

		select {
		case <-time.After(5 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		republishAll(ctx, nil, store)
		close(done)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	startCancel := time.Now()
	select {
	case <-done:
		shutdownDuration := time.Since(startCancel)
		if shutdownDuration > 1*time.Second {
			t.Fatalf("shutdown took %v, expected under 1s", shutdownDuration)
		}
		t.Logf("Workers stopped within %v after cancellation", shutdownDuration)
	case <-time.After(2 * time.Second):
		t.Fatalf("republishAll failed to stop within 2 seconds after context cancellation")
	}
}

func TestRepublisher_ErrorIsolation(t *testing.T) {
	origWorkers := MaxRepublishWorkers
	origTimeout := RepublishItemTimeout
	origProvideFn := provideFn
	defer func() {
		MaxRepublishWorkers = origWorkers
		RepublishItemTimeout = origTimeout
		provideFn = origProvideFn
	}()

	MaxRepublishWorkers = 4
	RepublishItemTimeout = 2 * time.Second

	manifests := generateManifests(10)
	store := &testManifestStore{manifests: manifests}

	var attempted sync.Map

	provideFn = func(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID) error {
		attempted.Store(id, true)
		if id[3]%2 == 1 {
			return fmt.Errorf("simulated network failure for manifest %x", id)
		}
		return nil
	}

	ctx := context.Background()
	republishAll(ctx, nil, store)

	count := 0
	attempted.Range(func(key, value interface{}) bool {
		count++
		return true
	})

	if count != 10 {
		t.Fatalf("expected all 10 manifests to be attempted despite individual errors, got %d", count)
	}
}

func TestRepublisher_IntegrationWithDHT(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	manifests := generateManifests(5)
	store := &testManifestStore{manifests: manifests}

	StartRepublisher(ctx, dht2, store, 100*time.Millisecond)

	time.Sleep(300 * time.Millisecond)

	for _, id := range manifests {
		discovered, err := FindProviders(ctx, dht1, id, 5)
		if err != nil {
			t.Fatalf("FindProviders failed for %x: %v", id, err)
		}
		found := false
		for _, p := range discovered {
			if p.ID == h2.ID() {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected provider %s to be found for content %x", h2.ID(), id)
		}
	}
}
