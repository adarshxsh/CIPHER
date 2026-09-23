package discovery

import (
	"cipher/internal/content/core"
	"context"
	"fmt"
	"sync"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
)

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

	cid, err := contentIDToCID(id)
	if err != nil {
		return fmt.Errorf("failed to convert ContentID to CID: %w", err)
	}

	subCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	if err := kdht.Provide(subCtx, cid, true); err != nil {
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

const defaultWorkerCount = 10
const batchTimeout = 5 * time.Minute

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

	batchCtx, cancel := context.WithTimeout(ctx, batchTimeout)
	defer cancel()

	numWorkers := defaultWorkerCount
	if len(manifests) < numWorkers {
		numWorkers = len(manifests)
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
			for id := range jobs {
				select {
				case <-batchCtx.Done():
					return
				default:
				}
				if err := Provide(batchCtx, kdht, id); err != nil {
					fmt.Printf("[DHT Republisher] Failed to provide %x: %v\n", id, err)
				}
			}
		}()
	}
	wg.Wait()
}
