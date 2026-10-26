package discovery

import (
	"cipher/internal/content/core"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"
)

var (
	announcementLimiterMu sync.RWMutex
	announcementLimiter   = rate.NewLimiter(rate.Limit(10), 10) // Default 10 CIDs/sec, burst 10
)

// SetAnnouncementRateLimit configures the token bucket rate (ops/sec) and burst capacity for DHT announcements.
func SetAnnouncementRateLimit(r rate.Limit, b int) {
	announcementLimiterMu.Lock()
	defer announcementLimiterMu.Unlock()
	announcementLimiter = rate.NewLimiter(r, b)
}

func getAnnouncementLimiter() *rate.Limiter {
	announcementLimiterMu.RLock()
	defer announcementLimiterMu.RUnlock()
	return announcementLimiter
}

type lookupSemaphore struct {
	mu  sync.RWMutex
	ch  chan struct{}
	max int
}

func newLookupSemaphore(max int) *lookupSemaphore {
	return &lookupSemaphore{
		ch:  make(chan struct{}, max),
		max: max,
	}
}

func (s *lookupSemaphore) Acquire(ctx context.Context) (func(), error) {
	s.mu.RLock()
	ch := s.ch
	s.mu.RUnlock()

	select {
	case ch <- struct{}{}:
		return func() { <-ch }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *lookupSemaphore) SetMax(max int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ch = make(chan struct{}, max)
	s.max = max
}

var (
	lookupSem     = newLookupSemaphore(5) // Default max 5 active queries
	activeLookups int32
)

// SetMaxLookupConcurrency updates the worker semaphore concurrency bound for FindProviders.
func SetMaxLookupConcurrency(max int) {
	if max > 0 {
		lookupSem.SetMax(max)
	}
}

// GetActiveLookups returns the current number of active in-flight FindProviders queries.
func GetActiveLookups() int32 {
	return atomic.LoadInt32(&activeLookups)
}

// StorageProviderNamespace is a well-known identifier used by nodes offering storage capacity
// over the /cipher/push/1.0.0 protocol to register on the Kademlia DHT.
var StorageProviderNamespace = core.ContentID{
	0x43, 0x49, 0x50, 0x48, 0x45, 0x52, 0x5f, 0x53, // "CIPHER_S"
	0x54, 0x4f, 0x52, 0x41, 0x47, 0x45, 0x5f, 0x50, // "TORAGE_P"
	0x52, 0x4f, 0x56, 0x49, 0x44, 0x45, 0x52, 0x5f, // "ROVIDER_"
	0x53, 0x45, 0x52, 0x56, 0x49, 0x43, 0x45, 0x01, // "SERVICE\x01"
}

// Provide announces to the DHT that this node can provide the content identified by the given ContentID.
func Provide(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID) error {
	limiter := getAnnouncementLimiter()
	if err := limiter.Wait(ctx); err != nil {
		return fmt.Errorf("announcement rate limit wait failed: %w", err)
	}

	cid, err := contentIDToCID(id)
	if err != nil {
		return fmt.Errorf("failed to convert ContentID to CID: %w", err)
	}

	if err := kdht.Provide(ctx, cid, true); err != nil {
		return fmt.Errorf("failed to provide content: %w", err)
	}

	return nil
}

// RegisterStorageProvider announces to the DHT that this node is an active storage provider.
func RegisterStorageProvider(ctx context.Context, kdht *dht.IpfsDHT) error {
	return Provide(ctx, kdht, StorageProviderNamespace)
}

// StartStorageProviderHeartbeat periodically re-announces the storage provider service to the DHT.
func StartStorageProviderHeartbeat(ctx context.Context, kdht *dht.IpfsDHT, interval time.Duration) {
	go func() {
		// Initial announcement
		if err := RegisterStorageProvider(ctx, kdht); err != nil {
			fmt.Printf("[DHT] Warning: Failed to register storage provider on DHT: %v\n", err)
		} else {
			fmt.Printf("[DHT] Successfully registered storage provider service on DHT\n")
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := RegisterStorageProvider(ctx, kdht); err != nil {
					fmt.Printf("[DHT] Warning: Failed to refresh storage provider registration: %v\n", err)
				}
			}
		}
	}()
}

// FindStorageProviders queries the DHT for active peers offering storage provider services.
func FindStorageProviders(ctx context.Context, kdht *dht.IpfsDHT, limit int) ([]peer.AddrInfo, error) {
	return FindProviders(ctx, kdht, StorageProviderNamespace, limit)
}

// FindProviders searches the DHT for peers that can provide the content identified by the given ContentID.
func FindProviders(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID, PROVIDER_LIMIT int) ([]peer.AddrInfo, error) {

	if PROVIDER_LIMIT <= 0 {
		return nil, fmt.Errorf("provider limit must be greater than zero")
	}

	release, err := lookupSem.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lookup semaphore: %w", err)
	}
	defer release()

	atomic.AddInt32(&activeLookups, 1)
	defer atomic.AddInt32(&activeLookups, -1)

	cid, err := contentIDToCID(id)
	if err != nil {
		return nil, fmt.Errorf("failed to convert ContentID to CID: %w", err)
	}

	providerCh := kdht.FindProvidersAsync(ctx, cid, PROVIDER_LIMIT)

	var providers []peer.AddrInfo

	for p := range providerCh {
		providers = append(providers, p)

		if len(providers) >= PROVIDER_LIMIT {
			break
		}
	}

	return providers, nil

}

// StartRepublisher begins a background loop that re-announces all locally available manifests to the DHT.
func StartRepublisher(ctx context.Context, kdht *dht.IpfsDHT, store core.ManifestStore, interval time.Duration) {
	go func() {
		// Republish immediately on startup
		republishAll(ctx, kdht, store)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				republishAll(ctx, kdht, store)
			}
		}
	}()
}

func republishAll(ctx context.Context, kdht *dht.IpfsDHT, store core.ManifestStore) {
	manifests, err := store.ListManifests(ctx)
	if err != nil {
		fmt.Printf("[DHT Republisher] Failed to list manifests: %v\n", err)
		return
	}

	if len(manifests) == 0 {
		return
	}

	fmt.Printf("[DHT Republisher] Re-announcing %d manifests...\n", len(manifests))
	for _, id := range manifests {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := Provide(ctx, kdht, id); err != nil {
			fmt.Printf("[DHT Republisher] Failed to provide %x: %v\n", id, err)
		}
	}
}
