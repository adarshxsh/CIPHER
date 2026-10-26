package retrieval

import (
	"context"
	"fmt"
	"log"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/reputation"
	"cipher/internal/transport"
)

type ResolveOption func(*resolveOptions)

type resolveOptions struct {
	tracker *reputation.PeerReputationTracker
}

func WithResolverTracker(tracker *reputation.PeerReputationTracker) ResolveOption {
	return func(o *resolveOptions) {
		o.tracker = tracker
	}
}

// ResolveManifest takes a content ID, queries the DHT for providers who have that content, and attempts to retrieve the manifest from those providers, it connects to each provider, creates a chunk client, and requests the manifest. If successful, it returns the manifest; otherwise, it returns an error after trying all providers.
func ResolveManifest(
	ctx context.Context,
	id core.ContentID,
	kdht *dht.IpfsDHT,
	t *transport.Transport,
	eng *engine.ContentEngine,
	providers []peer.ID,
	opts ...ResolveOption,
) (*manifest.Manifest, error) {
	var ro resolveOptions
	for _, opt := range opts {
		opt(&ro)
	}

	var lastErr error

	for _, provider := range providers {
		if ro.tracker != nil && ro.tracker.IsBanned(provider) {
			log.Printf("[DHT] Skipping provider %s due to bad reputation score", provider)
			continue
		}

		client, err := chunk.NewClient(ctx, t, provider, eng, chunk.WithTracker(ro.tracker))
		if err != nil {
			log.Printf(
				"[DHT] Failed to create chunk client for %s: %v",
				provider,
				err,
			)
			if ro.tracker != nil {
				ro.tracker.RecordConnectionFailure(provider)
			}
			lastErr = err
			continue
		}

		manifestData, err := client.Resolve(ctx, id)
		client.Close()

		if err != nil {
			log.Printf(
				"[DHT] Failed to resolve manifest from provider %s: %v",
				provider,
				err,
			)
			if ro.tracker != nil {
				ro.tracker.RecordConnectionFailure(provider)
			}
			lastErr = err
			continue
		}

		m, err := manifest.Deserialize(manifestData)
		if err != nil {
			log.Printf(
				"[DHT] Provider %s returned invalid manifest: %v",
				provider,
				err,
			)
			if ro.tracker != nil {
				ro.tracker.RecordIntegrityFault(provider)
			}
			lastErr = err
			continue
		}

		if ro.tracker != nil {
			ro.tracker.RecordSuccess(provider)
		}

		log.Printf(
			"[DHT] Successfully resolved manifest from provider %s",
			provider,
		)

		return m, nil
	}

	return nil, fmt.Errorf(
		"failed to resolve manifest from all providers: %w",
		lastErr,
	)
}
