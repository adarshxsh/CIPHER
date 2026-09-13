package retrieval

import (
	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/discovery"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
	"context"
	"fmt"
	"log"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ResolveManifest takes a content ID, queries candidate providers, retrieves the manifest,
// verifies the publisher signature and provider ownership attestation, and returns the verified manifest.
func ResolveManifest(
	ctx context.Context,
	id core.ContentID,
	kdht *dht.IpfsDHT,
	t *transport.Transport,
	eng *engine.ContentEngine,
	providers []peer.ID,
) (*manifest.Manifest, error) {

	var lastErr error

	for _, provider := range providers {

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
				"[DHT] Provider %s returned invalid manifest bytes: %v",
				provider,
				err,
			)
			lastErr = err
			continue
		}

		if m.Descriptor.ID != id {
			err := fmt.Errorf("content ID mismatch in manifest: expected %x, got %x", id, m.Descriptor.ID)
			log.Printf("[DHT] Provider %s returned manifest with content ID mismatch: %v", provider, err)
			lastErr = err
			continue
		}

		// 1. Verify Publisher Manifest Signature
		if err := m.VerifySignature(); err != nil {
			log.Printf(
				"[DHT] Provider %s returned manifest with invalid signature: %v",
				provider,
				err,
			)
			lastErr = fmt.Errorf("invalid manifest signature from provider %s: %w", provider, err)
			continue
		}

		// 2. Verify Provider Authority (Is Provider Publisher or holds valid Attestation?)
		pubPeerID, err := m.PublisherPeerID()
		if err != nil {
			log.Printf(
				"[DHT] Failed to derive publisher peer ID for provider %s: %v",
				provider,
				err,
			)
			lastErr = err
			continue
		}

		if provider != pubPeerID {
			// Provider is NOT the publisher, verify Ownership Attestation
			if m.Attestation == nil {
				err := fmt.Errorf("provider %s is not publisher %s and provided no ownership attestation", provider, pubPeerID)
				log.Printf("[DHT] Unauthenticated provider %s rejected: %v", provider, err)
				lastErr = err
				continue
			}

			pubKey, err := m.PublisherPublicKey()
			if err != nil {
				log.Printf("[DHT] Failed to get publisher public key for provider %s: %v", provider, err)
				lastErr = err
				continue
			}

			if err := discovery.VerifyAttestation(m.Attestation, pubKey, id, provider); err != nil {
				log.Printf("[DHT] Provider %s ownership attestation verification failed: %v", provider, err)
				lastErr = fmt.Errorf("provider %s ownership attestation invalid: %w", provider, err)
				continue
			}
		}

		log.Printf(
			"[DHT] Successfully resolved and verified manifest from provider %s (Publisher: %s)",
			provider,
			pubPeerID,
		)

		return m, nil
	}

	return nil, fmt.Errorf(
		"failed to resolve manifest from all providers: %w",
		lastErr,
	)
}
