package discovery

import (
	"cipher/internal/content/core"
	"context"
	"fmt"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Provide announces to the DHT that this node can provide the content identified by the given ContentID.
func Provide(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID) error {

	cid, err := contentIDToCID(id)
	if err != nil {
		return fmt.Errorf("failed to convert ContentID to CID: %w", err)
	}

	if err := kdht.Provide(ctx, cid, true); err != nil {
		return fmt.Errorf("failed to provide content: %w", err)
	}

	return nil
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
		if err := Provide(ctx, kdht, id); err != nil {
			fmt.Printf("[DHT Republisher] Failed to provide %x: %v\n", id, err)
		}
	}
}
