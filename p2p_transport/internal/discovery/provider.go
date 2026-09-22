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

// StorageProviderNamespace is a well-known identifier used by nodes offering storage capacity
// over the /cipher/push/1.0.0 protocol to register on the Kademlia DHT.
var StorageProviderNamespace = core.ContentID{
	0x43, 0x49, 0x50, 0x48, 0x45, 0x52, 0x5f, 0x53, // "CIPHER_S"
	0x54, 0x4f, 0x52, 0x41, 0x47, 0x45, 0x5f, 0x50, // "TORAGE_P"
	0x52, 0x4f, 0x56, 0x49, 0x44, 0x45, 0x52, 0x5f, // "ROVIDER_"
	0x53, 0x45, 0x52, 0x56, 0x49, 0x43, 0x45, 0x01, // "SERVICE\x01"
}

// Config defines rate limits and worker pool sizes for the discovery manager.
type Config struct {
	RepublishRate    float64 // announcements per second
	RepublishBurst   int     // burst limit for announcements
	RepublishWorkers int     // worker pool concurrency for republishing
	LookupRate       float64 // lookup queries per second
	LookupBurst      int     // burst limit for lookups
}

// DefaultConfig returns safe default parameters for discovery rate limits and worker pool concurrency.
func DefaultConfig() Config {
	return Config{
		RepublishRate:    5.0,
		RepublishBurst:   10,
		RepublishWorkers: 4,
		LookupRate:       10.0,
		LookupBurst:      20,
	}
}

// Manager controls rate-limited DHT provider registration, republishing worker pools, and query throttling.
type Manager struct {
	config         Config
	provideLimiter *rate.Limiter
	lookupLimiter  *rate.Limiter
}

// NewManager constructs a new discovery Manager with the specified configuration.
func NewManager(cfg Config) *Manager {
	if cfg.RepublishWorkers <= 0 {
		cfg.RepublishWorkers = 4
	}
	if cfg.RepublishRate <= 0 {
		cfg.RepublishRate = 5.0
	}
	if cfg.RepublishBurst <= 0 {
		cfg.RepublishBurst = 10
	}
	if cfg.LookupRate <= 0 {
		cfg.LookupRate = 10.0
	}
	if cfg.LookupBurst <= 0 {
		cfg.LookupBurst = 20
	}

	return &Manager{
		config:         cfg,
		provideLimiter: rate.NewLimiter(rate.Limit(cfg.RepublishRate), cfg.RepublishBurst),
		lookupLimiter:  rate.NewLimiter(rate.Limit(cfg.LookupRate), cfg.LookupBurst),
	}
}

var (
	defaultManagerMu sync.RWMutex
	defaultManager   = NewManager(DefaultConfig())
)

// Configure updates the package-level default discovery manager settings.
func Configure(cfg Config) {
	defaultManagerMu.Lock()
	defer defaultManagerMu.Unlock()
	defaultManager = NewManager(cfg)
}

// GetConfig returns the current configuration of the package-level default discovery manager.
func GetConfig() Config {
	defaultManagerMu.RLock()
	defer defaultManagerMu.RUnlock()
	return defaultManager.config
}

// SetRepublishRate updates the provider announcement rate limit on the default manager.
func SetRepublishRate(r float64) {
	defaultManagerMu.Lock()
	defer defaultManagerMu.Unlock()
	cfg := defaultManager.config
	cfg.RepublishRate = r
	defaultManager = NewManager(cfg)
}

// SetRepublishWorkers updates the republish worker pool concurrency on the default manager.
func SetRepublishWorkers(w int) {
	defaultManagerMu.Lock()
	defer defaultManagerMu.Unlock()
	cfg := defaultManager.config
	cfg.RepublishWorkers = w
	defaultManager = NewManager(cfg)
}

// SetLookupRate updates the lookup query rate limit on the default manager.
func SetLookupRate(r float64) {
	defaultManagerMu.Lock()
	defer defaultManagerMu.Unlock()
	cfg := defaultManager.config
	cfg.LookupRate = r
	defaultManager = NewManager(cfg)
}

func getDefaultManager() *Manager {
	defaultManagerMu.RLock()
	defer defaultManagerMu.RUnlock()
	return defaultManager
}

// Provide announces to the DHT that this node can provide the content identified by the given ContentID.
func (m *Manager) Provide(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID) error {
	if err := m.provideLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter wait failed: %w", err)
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

// Provide announces to the DHT that this node can provide the content identified by the given ContentID.
func Provide(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID) error {
	return getDefaultManager().Provide(ctx, kdht, id)
}

// RegisterStorageProvider announces to the DHT that this node is an active storage provider.
func (m *Manager) RegisterStorageProvider(ctx context.Context, kdht *dht.IpfsDHT) error {
	return m.Provide(ctx, kdht, StorageProviderNamespace)
}

// RegisterStorageProvider announces to the DHT that this node is an active storage provider.
func RegisterStorageProvider(ctx context.Context, kdht *dht.IpfsDHT) error {
	return getDefaultManager().RegisterStorageProvider(ctx, kdht)
}

// StartStorageProviderHeartbeat periodically re-announces the storage provider service to the DHT.
func (m *Manager) StartStorageProviderHeartbeat(ctx context.Context, kdht *dht.IpfsDHT, interval time.Duration) {
	go func() {
		// Initial announcement
		if err := m.RegisterStorageProvider(ctx, kdht); err != nil {
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
				if err := m.RegisterStorageProvider(ctx, kdht); err != nil {
					fmt.Printf("[DHT] Warning: Failed to refresh storage provider registration: %v\n", err)
				}
			}
		}
	}()
}

// StartStorageProviderHeartbeat periodically re-announces the storage provider service to the DHT.
func StartStorageProviderHeartbeat(ctx context.Context, kdht *dht.IpfsDHT, interval time.Duration) {
	getDefaultManager().StartStorageProviderHeartbeat(ctx, kdht, interval)
}

// FindStorageProviders queries the DHT for active peers offering storage provider services.
func (m *Manager) FindStorageProviders(ctx context.Context, kdht *dht.IpfsDHT, limit int) ([]peer.AddrInfo, error) {
	return m.FindProviders(ctx, kdht, StorageProviderNamespace, limit)
}

// FindStorageProviders queries the DHT for active peers offering storage provider services.
func FindStorageProviders(ctx context.Context, kdht *dht.IpfsDHT, limit int) ([]peer.AddrInfo, error) {
	return getDefaultManager().FindStorageProviders(ctx, kdht, limit)
}

// FindProviders searches the DHT for peers that can provide the content identified by the given ContentID.
func (m *Manager) FindProviders(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID, PROVIDER_LIMIT int) ([]peer.AddrInfo, error) {
	if PROVIDER_LIMIT <= 0 {
		return nil, fmt.Errorf("provider limit must be greater than zero")
	}

	if err := m.lookupLimiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limiter wait failed: %w", err)
	}

	cid, err := contentIDToCID(id)
	if err != nil {
		return nil, fmt.Errorf("failed to convert ContentID to CID: %w", err)
	}

	providerCh := kdht.FindProvidersAsync(ctx, cid, PROVIDER_LIMIT)

	var providers []peer.AddrInfo

	for {
		select {
		case <-ctx.Done():
			return providers, ctx.Err()
		case p, ok := <-providerCh:
			if !ok {
				return providers, nil
			}
			providers = append(providers, p)
			if len(providers) >= PROVIDER_LIMIT {
				return providers, nil
			}
		}
	}
}

// FindProviders searches the DHT for peers that can provide the content identified by the given ContentID.
func FindProviders(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID, PROVIDER_LIMIT int) ([]peer.AddrInfo, error) {
	return getDefaultManager().FindProviders(ctx, kdht, id, PROVIDER_LIMIT)
}

// StartRepublisher begins a background loop that re-announces all locally available manifests to the DHT.
func (m *Manager) StartRepublisher(ctx context.Context, kdht *dht.IpfsDHT, store core.ManifestStore, interval time.Duration) {
	go func() {
		// Republish immediately on startup
		m.republishAll(ctx, kdht, store)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.republishAll(ctx, kdht, store)
			}
		}
	}()
}

// StartRepublisher begins a background loop that re-announces all locally available manifests to the DHT.
func StartRepublisher(ctx context.Context, kdht *dht.IpfsDHT, store core.ManifestStore, interval time.Duration) {
	getDefaultManager().StartRepublisher(ctx, kdht, store, interval)
}

func (m *Manager) republishAll(ctx context.Context, kdht *dht.IpfsDHT, store core.ManifestStore) {
	manifests, err := store.ListManifests(ctx)
	if err != nil {
		fmt.Printf("[DHT Republisher] Failed to list manifests: %v\n", err)
		return
	}

	if len(manifests) == 0 {
		return
	}

	fmt.Printf("[DHT Republisher] Re-announcing %d manifests...\n", len(manifests))

	numWorkers := m.config.RepublishWorkers
	if numWorkers <= 0 {
		numWorkers = 4
	}

	jobs := make(chan core.ContentID, len(manifests))
	for _, id := range manifests {
		jobs <- id
	}
	close(jobs)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case id, ok := <-jobs:
					if !ok {
						return
					}
					if err := m.Provide(ctx, kdht, id); err != nil {
						fmt.Printf("[DHT Republisher] Failed to provide %x: %v\n", id, err)
					}
				}
			}
		}()
	}
	wg.Wait()
}

func republishAll(ctx context.Context, kdht *dht.IpfsDHT, store core.ManifestStore) {
	getDefaultManager().republishAll(ctx, kdht, store)
}
