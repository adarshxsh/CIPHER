package retrieval

import (
	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
	"context"
	"fmt"
	"log"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
)

type resolveResult struct {
	manifest *manifest.Manifest
	err      error
}

// ResolveManifest takes a content ID, queries the DHT for providers who have that content, and attempts to retrieve the manifest from those providers in parallel. It uses context race fan-out and worker goroutines to query providers concurrently. As soon as a valid manifest is retrieved, remaining requests are canceled.
func ResolveManifest(
	ctx context.Context,
	id core.ContentID,
	kdht *dht.IpfsDHT,
	t *transport.Transport,
	eng *engine.ContentEngine,
	providers []peer.ID,
) (*manifest.Manifest, error) {

	if len(providers) == 0 {
		return nil, fmt.Errorf(
			"failed to resolve manifest from all providers: %w",
			fmt.Errorf("no providers available"),
		)
	}

	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan resolveResult, len(providers))

	for _, provider := range providers {
		go func(p peer.ID) {
			client, err := chunk.NewClient(raceCtx, t, p, eng)
			if err != nil {
				log.Printf(
					"[DHT] Failed to create chunk client for %s: %v",
					p,
					err,
				)
				results <- resolveResult{err: err}
				return
			}
			defer client.Close()

			manifestData, err := client.Resolve(raceCtx, id)
			if err != nil {
				log.Printf(
					"[DHT] Failed to resolve manifest from provider %s: %v",
					p,
					err,
				)
				results <- resolveResult{err: err}
				return
			}

			m, err := manifest.Deserialize(manifestData)
			if err != nil {
				log.Printf(
					"[DHT] Provider %s returned invalid manifest: %v",
					p,
					err,
				)
				results <- resolveResult{err: err}
				return
			}

			log.Printf(
				"[DHT] Successfully resolved manifest from provider %s",
				p,
			)

			results <- resolveResult{manifest: m}
		}(provider)
	}

	var lastErr error
	failedCount := 0

	for failedCount < len(providers) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-results:
			if res.manifest != nil {
				cancel()
				return res.manifest, nil
			}
			lastErr = res.err
			failedCount++
		}
	}

	return nil, fmt.Errorf(
		"failed to resolve manifest from all providers: %w",
		lastErr,
	)
}
