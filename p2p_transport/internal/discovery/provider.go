package discovery

import (
	"cipher/internal/content/core"
	"context"
	"fmt"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
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

// Attestation represents a publisher delegation authorizing a provider to host content.
type Attestation struct {
	ContentID  core.ContentID `json:"content_id"`
	ProviderID peer.ID        `json:"provider_id"`
	Timestamp  int64          `json:"timestamp"`
	Signature  []byte         `json:"signature"`
}

// AttestationSigningBytes constructs the canonical payload for attestation signing and verification.
func AttestationSigningBytes(contentID core.ContentID, providerID peer.ID, timestamp int64) []byte {
	return []byte(fmt.Sprintf("attestation:%x:%s:%d", contentID, providerID, timestamp))
}

// CreateAttestation generates a signed ownership attestation authorizing providerID for contentID.
func CreateAttestation(publisherPrivKey crypto.PrivKey, contentID core.ContentID, providerID peer.ID) (*Attestation, error) {
	if publisherPrivKey == nil {
		return nil, fmt.Errorf("publisher private key is required")
	}
	timestamp := time.Now().Unix()
	signingData := AttestationSigningBytes(contentID, providerID, timestamp)
	sig, err := publisherPrivKey.Sign(signingData)
	if err != nil {
		return nil, fmt.Errorf("failed to sign attestation: %w", err)
	}
	return &Attestation{
		ContentID:  contentID,
		ProviderID: providerID,
		Timestamp:  timestamp,
		Signature:  sig,
	}, nil
}

// VerifyAttestation validates that att was signed by publisherPubKey for expectedContentID and expectedProviderID.
func VerifyAttestation(att *Attestation, publisherPubKey crypto.PubKey, expectedContentID core.ContentID, expectedProviderID peer.ID) error {
	if att == nil {
		return fmt.Errorf("attestation is nil")
	}
	if publisherPubKey == nil {
		return fmt.Errorf("publisher public key is required")
	}
	if att.ContentID != expectedContentID {
		return fmt.Errorf("attestation content ID mismatch: expected %x, got %x", expectedContentID, att.ContentID)
	}
	if att.ProviderID != expectedProviderID {
		return fmt.Errorf("attestation provider ID mismatch: expected %s, got %s", expectedProviderID, att.ProviderID)
	}
	signingData := AttestationSigningBytes(att.ContentID, att.ProviderID, att.Timestamp)
	valid, err := publisherPubKey.Verify(signingData, att.Signature)
	if err != nil {
		return fmt.Errorf("failed to verify attestation signature: %w", err)
	}
	if !valid {
		return fmt.Errorf("invalid attestation signature")
	}
	return nil
}

// Provide announces to the DHT that this node can provide the content identified by the given ContentID.
func Provide(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID) error {
	return ProvideWithAttestation(ctx, kdht, id, nil)
}

// ProvideWithAttestation announces content availability to the DHT, supporting optional ownership attestations.
func ProvideWithAttestation(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID, att *Attestation) error {
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
