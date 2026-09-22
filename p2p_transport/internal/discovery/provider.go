package discovery

import (
	"cipher/internal/content/core"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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

var (
	ErrAuthorizationFailed = errors.New("authorization failed: invalid provider record signature")
	ErrInvalidSignature    = errors.New("invalid signature")
)

// SignedProviderRecord represents a cryptographically signed provider announcement.
type SignedProviderRecord struct {
	ContentID          core.ContentID `json:"content_id"`
	ProviderPeerID     peer.ID        `json:"provider_peer_id"`
	Addresses          []string       `json:"addresses,omitempty"`
	ExpiryTimestamp    int64          `json:"expiry_timestamp"`
	PublisherPublicKey []byte         `json:"publisher_public_key"`
	Signature          []byte         `json:"signature"`
}

// ConstructSigningData creates the canonical bytes to be signed for a provider record.
func ConstructSigningData(contentID core.ContentID, providerPeerID peer.ID, expiryTimestamp int64) []byte {
	peerBytes := []byte(providerPeerID)
	buf := make([]byte, 32+len(peerBytes)+8)
	copy(buf[0:32], contentID[:])
	copy(buf[32:32+len(peerBytes)], peerBytes)
	binary.BigEndian.PutUint64(buf[32+len(peerBytes):], uint64(expiryTimestamp))
	return buf
}

// NewSignedProviderRecord creates and signs a new provider record using the publisher Ed25519 private key.
func NewSignedProviderRecord(contentID core.ContentID, providerPeerID peer.ID, privKey crypto.PrivKey, ttl time.Duration) (*SignedProviderRecord, error) {
	if privKey == nil {
		return nil, fmt.Errorf("%w: private key is required to sign provider record", ErrAuthorizationFailed)
	}

	var expiry int64
	if ttl != 0 {
		expiry = time.Now().Add(ttl).Unix()
	}

	pubKeyBytes, err := crypto.MarshalPublicKey(privKey.GetPublic())
	if err != nil {
		return nil, fmt.Errorf("failed to marshal public key: %w", err)
	}

	signingData := ConstructSigningData(contentID, providerPeerID, expiry)
	sig, err := privKey.Sign(signingData)
	if err != nil {
		return nil, fmt.Errorf("failed to sign provider record: %w", err)
	}

	rec := &SignedProviderRecord{
		ContentID:          contentID,
		ProviderPeerID:     providerPeerID,
		ExpiryTimestamp:    expiry,
		PublisherPublicKey: pubKeyBytes,
		Signature:          sig,
	}

	return rec, nil
}

// Verify validates the signature and expiration timestamp of the provider record.
func (r *SignedProviderRecord) Verify() error {
	if r.ExpiryTimestamp > 0 && time.Now().Unix() > r.ExpiryTimestamp {
		return fmt.Errorf("%w: provider record expired", ErrAuthorizationFailed)
	}

	if len(r.PublisherPublicKey) == 0 || len(r.Signature) == 0 {
		return fmt.Errorf("%w: missing public key or signature payload", ErrAuthorizationFailed)
	}

	pubKey, err := crypto.UnmarshalPublicKey(r.PublisherPublicKey)
	if err != nil {
		return fmt.Errorf("%w: invalid publisher public key: %v", ErrAuthorizationFailed, err)
	}

	signingData := ConstructSigningData(r.ContentID, r.ProviderPeerID, r.ExpiryTimestamp)
	valid, err := pubKey.Verify(signingData, r.Signature)
	if err != nil || !valid {
		return fmt.Errorf("%w: signature verification failed", ErrAuthorizationFailed)
	}

	return nil
}

// ProviderRecordValidator handles DHT record validation for provider records.
type ProviderRecordValidator struct{}

func (v ProviderRecordValidator) Validate(key string, value []byte) error {
	var rec SignedProviderRecord
	if err := json.Unmarshal(value, &rec); err != nil {
		return fmt.Errorf("invalid provider record format: %w", err)
	}

	if err := rec.Verify(); err != nil {
		return fmt.Errorf("provider record verification failed: %w", err)
	}

	return nil
}

func (v ProviderRecordValidator) Select(key string, values [][]byte) (int, error) {
	if len(values) == 0 {
		return -1, fmt.Errorf("no values to select")
	}

	for i, val := range values {
		var rec SignedProviderRecord
		if err := json.Unmarshal(val, &rec); err == nil && rec.Verify() == nil {
			return i, nil
		}
	}

	return 0, nil
}

// ProviderRecordKey returns the canonical DHT value store key for a provider record.
func ProviderRecordKey(contentID core.ContentID, peerID peer.ID) string {
	return fmt.Sprintf("/cipher/provider/%x/%s", contentID, peerID.String())
}

// Provide announces to the DHT that this node can provide the content identified by the given ContentID,
// signing the provider announcement with the publisher Ed25519 private key.
func Provide(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID, privKey crypto.PrivKey) error {
	if privKey == nil {
		return fmt.Errorf("%w: private key required for signed provider announcement", ErrAuthorizationFailed)
	}

	peerID := kdht.PeerID()
	rec, err := NewSignedProviderRecord(id, peerID, privKey, 24*time.Hour)
	if err != nil {
		return fmt.Errorf("failed to create signed provider record: %w", err)
	}

	if err := rec.Verify(); err != nil {
		return fmt.Errorf("signature verification failed prior to announcement: %w", err)
	}

	recBytes, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("failed to marshal signed provider record: %w", err)
	}

	key := ProviderRecordKey(id, peerID)
	if err := kdht.PutValue(ctx, key, recBytes); err != nil {
		return fmt.Errorf("failed to store signed provider record in DHT: %w", err)
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
func RegisterStorageProvider(ctx context.Context, kdht *dht.IpfsDHT, privKey crypto.PrivKey) error {
	return Provide(ctx, kdht, StorageProviderNamespace, privKey)
}

// StartStorageProviderHeartbeat periodically re-announces the storage provider service to the DHT.
func StartStorageProviderHeartbeat(ctx context.Context, kdht *dht.IpfsDHT, interval time.Duration, privKey crypto.PrivKey) {
	go func() {
		// Initial announcement
		if err := RegisterStorageProvider(ctx, kdht, privKey); err != nil {
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
				if err := RegisterStorageProvider(ctx, kdht, privKey); err != nil {
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

// FindProviders searches the DHT for peers that can provide the content identified by the given ContentID,
// returning only provider addresses with valid publisher signatures.
func FindProviders(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID, PROVIDER_LIMIT int) ([]peer.AddrInfo, error) {
	if PROVIDER_LIMIT <= 0 {
		return nil, fmt.Errorf("provider limit must be greater than zero")
	}

	cid, err := contentIDToCID(id)
	if err != nil {
		return nil, fmt.Errorf("failed to convert ContentID to CID: %w", err)
	}

	providerCh := kdht.FindProvidersAsync(ctx, cid, PROVIDER_LIMIT*2)

	var providers []peer.AddrInfo

	for p := range providerCh {
		recKey := ProviderRecordKey(id, p.ID)
		val, err := kdht.GetValue(ctx, recKey)
		if err != nil {
			log.Printf("[DHT] Discarding provider %s: missing signed provider record in DHT: %v", p.ID, err)
			continue
		}

		var rec SignedProviderRecord
		if err := json.Unmarshal(val, &rec); err != nil {
			log.Printf("[DHT] Discarding provider %s: malformed provider record: %v", p.ID, err)
			continue
		}

		if err := rec.Verify(); err != nil {
			log.Printf("[DHT] Discarding provider %s: signature verification failed: %v", p.ID, err)
			continue
		}

		providers = append(providers, p)

		if len(providers) >= PROVIDER_LIMIT {
			break
		}
	}

	return providers, nil
}

// StartRepublisher begins a background loop that re-announces all locally available manifests to the DHT.
func StartRepublisher(ctx context.Context, kdht *dht.IpfsDHT, store core.ManifestStore, interval time.Duration, privKey crypto.PrivKey) {
	go func() {
		// Republish immediately on startup
		republishAll(ctx, kdht, store, privKey)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				republishAll(ctx, kdht, store, privKey)
			}
		}
	}()
}

func republishAll(ctx context.Context, kdht *dht.IpfsDHT, store core.ManifestStore, privKey crypto.PrivKey) {
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
		if err := Provide(ctx, kdht, id, privKey); err != nil {
			fmt.Printf("[DHT Republisher] Failed to provide %x: %v\n", id, err)
		}
	}
}
