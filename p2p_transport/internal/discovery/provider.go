package discovery

import (
	"cipher/internal/content/core"
	"context"
	"fmt"
	"sync"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"
)

const (
	MaxProviderLimit         = 100
	DefaultLookupConcurrency = 16
	DefaultRepublishRate     = 10.0 // 10 announcements per second
	DefaultRepublishBurst    = 10
)

var (
	lookupSemMu sync.RWMutex
	lookupSem   = make(chan struct{}, DefaultLookupConcurrency)

	provideLimiterMu sync.RWMutex
	provideLimiter   *rate.Limiter = rate.NewLimiter(rate.Limit(DefaultRepublishRate), DefaultRepublishBurst)
)

// SetMaxConcurrentLookups configures the maximum concurrent FindProviders queries.
func SetMaxConcurrentLookups(n int) {
	if n <= 0 {
		return
	}
	lookupSemMu.Lock()
	defer lookupSemMu.Unlock()
	lookupSem = make(chan struct{}, n)
}

// SetProvideRateLimit configures the rate limit (events/second) and burst capacity for direct Provide operations.
// Passing limit <= 0 disables rate limiting for Provide.
func SetProvideRateLimit(limit float64, burst int) {
	provideLimiterMu.Lock()
	defer provideLimiterMu.Unlock()
	if limit <= 0 {
		provideLimiter = nil
	} else {
		provideLimiter = rate.NewLimiter(rate.Limit(limit), burst)
	}
}

func getProvideLimiter() *rate.Limiter {
	provideLimiterMu.RLock()
	defer provideLimiterMu.RUnlock()
	return provideLimiter
}

func acquireLookupSemaphore(ctx context.Context) (func(), error) {
	lookupSemMu.RLock()
	sem := lookupSem
	lookupSemMu.RUnlock()

	select {
	case sem <- struct{}{}:
		return func() {
			select {
			case <-sem:
			default:
			}
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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
	limiter := getProvideLimiter()
	if limiter != nil {
		if err := limiter.Wait(ctx); err != nil {
			return fmt.Errorf("provide rate limit wait failed: %w", err)
		}
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

	if PROVIDER_LIMIT > MaxProviderLimit {
		PROVIDER_LIMIT = MaxProviderLimit
	}

	release, err := acquireLookupSemaphore(ctx)
	if err != nil {
		return nil, fmt.Errorf("lookup concurrency limit reached: %w", err)
	}
	defer release()

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

// RepublisherOption configures background manifest republishing behavior.
type RepublisherOption func(*republisherConfig)

type republisherConfig struct {
	limiter *rate.Limiter
}

// WithRepublishRateLimit configures the rate limit (events/second) and burst capacity for background republishing.
func WithRepublishRateLimit(limit float64, burst int) RepublisherOption {
	return func(cfg *republisherConfig) {
		if limit <= 0 {
			cfg.limiter = nil
		} else {
			cfg.limiter = rate.NewLimiter(rate.Limit(limit), burst)
		}
	}
}

// WithRepublishLimiter sets a custom token-bucket rate limiter for background republishing.
func WithRepublishLimiter(limiter *rate.Limiter) RepublisherOption {
	return func(cfg *republisherConfig) {
		cfg.limiter = limiter
	}
}

// StartRepublisher begins a background loop that re-announces all locally available manifests to the DHT.
func StartRepublisher(ctx context.Context, kdht *dht.IpfsDHT, store core.ManifestStore, interval time.Duration, opts ...RepublisherOption) {
	cfg := &republisherConfig{
		limiter: rate.NewLimiter(rate.Limit(DefaultRepublishRate), DefaultRepublishBurst),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}

	go func() {
		// Republish immediately on startup
		republishAll(ctx, kdht, store, cfg.limiter)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				republishAll(ctx, kdht, store, cfg.limiter)
			}
		}
	}()
}

func republishAll(ctx context.Context, kdht *dht.IpfsDHT, store core.ManifestStore, limiter *rate.Limiter) {
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
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				fmt.Printf("[DHT Republisher] Rate limit wait cancelled: %v\n", err)
				return
			}
		}
		if err := Provide(ctx, kdht, id); err != nil {
			fmt.Printf("[DHT Republisher] Failed to provide %x: %v\n", id, err)
		}
	}
}
