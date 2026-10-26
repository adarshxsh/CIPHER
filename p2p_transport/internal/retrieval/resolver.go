package retrieval

import (
	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/discovery"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
	"context"
	"encoding/json"
	"fmt"
	"log"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
)

// VerifyProviderRecord verifies the cryptographic signature of a provider record from the DHT.
func VerifyProviderRecord(
	ctx context.Context,
	kdht *dht.IpfsDHT,
	id core.ContentID,
	providerID peer.ID,
) (*discovery.SignedProviderRecord, error) {
	if kdht == nil {
		return nil, fmt.Errorf("DHT instance is nil")
	}

	key := discovery.ProviderRecordKey(id, providerID)
	val, err := kdht.GetValue(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("missing signed provider record in DHT for provider %s: %w", providerID, err)
	}

	var rec discovery.SignedProviderRecord
	if err := json.Unmarshal(val, &rec); err != nil {
		return nil, fmt.Errorf("malformed signed provider record from provider %s: %w", providerID, err)
	}

	if rec.ContentID != id {
		return nil, fmt.Errorf("content ID mismatch in provider record from provider %s", providerID)
	}

	if rec.ProviderPeerID != providerID {
		return nil, fmt.Errorf("provider peer ID mismatch in provider record from provider %s", providerID)
	}

	if err := rec.Verify(); err != nil {
		return nil, fmt.Errorf("signature verification failed for provider %s: %w", providerID, err)
	}

	return &rec, nil
}

// ResolveManifest takes a content ID, queries the DHT for providers who have that content, verifies their signed provider records, and attempts to retrieve the manifest from those verified providers. Unverified or spoofed provider addresses are discarded without retrying.
func ResolveManifest(
	ctx context.Context,
	id core.ContentID,
	kdht *dht.IpfsDHT,
	t *transport.Transport,
	eng *engine.ContentEngine,
	providers []peer.ID,
) (*manifest.Manifest, error) {

	// Filter out spoofed or unverified provider records prior to initiating manifest/chunk transfers
	var verifiedProviders []peer.ID
	if kdht != nil {
		for _, provider := range providers {
			if _, err := VerifyProviderRecord(ctx, kdht, id, provider); err != nil {
				log.Printf(
					"[DHT] Discarding spoofed or unverified provider %s for ContentID %x: %v",
					provider,
					id,
					err,
				)
				continue
			}
			log.Printf(
				"[DHT] Verified provider %s with valid publisher signature for ContentID %x",
				provider,
				id,
			)
			verifiedProviders = append(verifiedProviders, provider)
		}
	} else {
		verifiedProviders = providers
	}

	if len(verifiedProviders) == 0 {
		return nil, fmt.Errorf(
			"no authentic providers with valid publisher signatures found for ContentID %x",
			id,
		)
	}

	var lastErr error

	for _, provider := range verifiedProviders {

		client, err := chunk.NewClient(ctx, t, provider, eng)
		if err != nil {
			log.Printf(
				"[DHT] Failed to create chunk client for %s: %v",
				provider,
				err,
			)
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
			lastErr = err
			continue
		}

		log.Printf(
			"[DHT] Successfully resolved manifest from provider %s",
			provider,
		)

		return m, nil
	}

	return nil, fmt.Errorf(
		"failed to resolve manifest from all verified providers: %w",
		lastErr,
	)
}
