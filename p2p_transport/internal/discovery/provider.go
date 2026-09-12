package discovery

import (
	"cipher/internal/content/core"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

var localRecords sync.Map

// StoreLocalRecord saves a signed provider record in local memory cache.
func StoreLocalRecord(key string, recordBytes []byte) {
	localRecords.Store(key, recordBytes)
}

// RepublishLocalRecords pushes cached local provider records to newly connected DHT peers.
func RepublishLocalRecords(ctx context.Context, kdht *dht.IpfsDHT) {
	if kdht == nil {
		return
	}
	localRecords.Range(func(k, v any) bool {
		key := k.(string)
		bytes := v.([]byte)
		_ = kdht.PutValue(ctx, key, bytes)
		return true
	})
}

// SignedProviderRecord represents an authenticated provider record in the DHT.
type SignedProviderRecord struct {
	ContentID       core.ContentID `json:"content_id"`
	PeerID          peer.ID        `json:"peer_id"`
	Addresses       []string       `json:"addresses,omitempty"`
	ExpiryTimestamp int64          `json:"expiry_timestamp"`
	Signature       []byte         `json:"signature"`
}

// ConstructSigningData creates deterministic bytes to sign/verify (ContentID + PeerID + ExpiryTimestamp).
func ConstructSigningData(id core.ContentID, peerID peer.ID, expiryTimestamp int64) []byte {
	buf := make([]byte, 0, len(id)+len(peerID)+8)
	buf = append(buf, id[:]...)
	buf = append(buf, []byte(peerID)...)
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(expiryTimestamp))
	buf = append(buf, tsBytes...)
	return buf
}

// ProviderRecordKey constructs the DHT value key for a provider record.
func ProviderRecordKey(id core.ContentID, peerID peer.ID) string {
	return fmt.Sprintf("/provider/%s/%s", hex.EncodeToString(id[:]), peerID.String())
}

// SignProviderRecord creates a signed provider record using an Ed25519 private key.
func SignProviderRecord(privKey crypto.PrivKey, id core.ContentID, peerID peer.ID, addrs []string, ttl time.Duration) (*SignedProviderRecord, error) {
	if privKey == nil {
		return nil, errors.New("private key is required to sign provider record")
	}
	if ttl == 0 {
		ttl = 24 * time.Hour
	}
	expiry := time.Now().Add(ttl).Unix()
	data := ConstructSigningData(id, peerID, expiry)

	sig, err := privKey.Sign(data)
	if err != nil {
		return nil, fmt.Errorf("failed to sign provider record: %w", err)
	}

	return &SignedProviderRecord{
		ContentID:       id,
		PeerID:          peerID,
		Addresses:       addrs,
		ExpiryTimestamp: expiry,
		Signature:       sig,
	}, nil
}

// VerifyProviderRecord verifies the Ed25519 signature and expiration timestamp of a provider record.
func VerifyProviderRecord(record *SignedProviderRecord) error {
	if record == nil {
		return errors.New("nil provider record")
	}
	if len(record.Signature) == 0 {
		return errors.New("missing signature in provider record")
	}
	if time.Now().Unix() > record.ExpiryTimestamp {
		return fmt.Errorf("provider record expired at %d (current time %d)", record.ExpiryTimestamp, time.Now().Unix())
	}

	pubKey, err := record.PeerID.ExtractPublicKey()
	if err != nil {
		return fmt.Errorf("failed to extract public key from PeerID %s: %w", record.PeerID, err)
	}

	data := ConstructSigningData(record.ContentID, record.PeerID, record.ExpiryTimestamp)
	ok, err := pubKey.Verify(data, record.Signature)
	if err != nil {
		return fmt.Errorf("signature verification error: %w", err)
	}
	if !ok {
		return errors.New("signature verification failed: invalid signature for record")
	}

	return nil
}

// Provide announces to the DHT that this node can provide the content identified by the given ContentID.
// It creates a signed provider record and stores it in the DHT value store before calling kdht.Provide.
func Provide(ctx context.Context, kdht *dht.IpfsDHT, privKey crypto.PrivKey, id core.ContentID) error {
	cid, err := ContentIDToCID(id)
	if err != nil {
		return fmt.Errorf("failed to convert ContentID to CID: %w", err)
	}

	peerID := kdht.PeerID()
	record, err := SignProviderRecord(privKey, id, peerID, nil, 24*time.Hour)
	if err != nil {
		return fmt.Errorf("failed to create signed provider record: %w", err)
	}

	recordBytes, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal signed provider record: %w", err)
	}

	recKey := ProviderRecordKey(id, peerID)
	StoreLocalRecord(recKey, recordBytes)
	if err := kdht.PutValue(ctx, recKey, recordBytes); err != nil {
		fmt.Printf("[DHT] Warning: failed to PutValue for provider record %s: %v\n", recKey, err)
	}

	if err := kdht.Provide(ctx, cid, true); err != nil {
		return fmt.Errorf("failed to provide content: %w", err)
	}

	return nil
}

// VerifyProviderForContent fetches and validates the signed provider record for a given ContentID and Provider PeerID.
func VerifyProviderForContent(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID, providerID peer.ID) error {
	if kdht == nil {
		return nil
	}

	recKey := ProviderRecordKey(id, providerID)

	var val []byte
	if localVal, ok := localRecords.Load(recKey); ok {
		val = localVal.([]byte)
	}

	if val == nil {
		getCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		var err error
		val, err = kdht.GetValue(getCtx, recKey)
		if err != nil {
			if localVal, ok := localRecords.Load(recKey); ok {
				val = localVal.([]byte)
			} else {
				return fmt.Errorf("missing signed provider record for peer %s: %w", providerID, err)
			}
		}
	}

	var record SignedProviderRecord
	if err := json.Unmarshal(val, &record); err != nil {
		return fmt.Errorf("invalid provider record JSON format for peer %s: %w", providerID, err)
	}

	if record.ContentID != id {
		return fmt.Errorf("provider record ContentID mismatch for peer %s", providerID)
	}

	if record.PeerID != providerID {
		return fmt.Errorf("provider record PeerID mismatch for peer %s", providerID)
	}

	if err := VerifyProviderRecord(&record); err != nil {
		return fmt.Errorf("provider record signature/expiry verification failed for peer %s: %w", providerID, err)
	}

	return nil
}

// FindProviders searches the DHT for peers that can provide the content identified by the given ContentID
// and filters out records with invalid or missing signatures.
func FindProviders(ctx context.Context, kdht *dht.IpfsDHT, id core.ContentID, PROVIDER_LIMIT int) ([]peer.AddrInfo, error) {
	if PROVIDER_LIMIT <= 0 {
		return nil, fmt.Errorf("provider limit must be greater than zero")
	}

	cid, err := ContentIDToCID(id)
	if err != nil {
		return nil, fmt.Errorf("failed to convert ContentID to CID: %w", err)
	}

	providerCh := kdht.FindProvidersAsync(ctx, cid, PROVIDER_LIMIT*2)

	var providers []peer.AddrInfo

	for p := range providerCh {
		if err := VerifyProviderForContent(ctx, kdht, id, p.ID); err != nil {
			fmt.Printf("[DHT] FindProviders filtering out provider %s: %v\n", p.ID, err)
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
func StartRepublisher(ctx context.Context, kdht *dht.IpfsDHT, privKey crypto.PrivKey, store core.ManifestStore, interval time.Duration) {
	go func() {
		// Republish immediately on startup
		republishAll(ctx, kdht, privKey, store)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				republishAll(ctx, kdht, privKey, store)
			}
		}
	}()
}

func republishAll(ctx context.Context, kdht *dht.IpfsDHT, privKey crypto.PrivKey, store core.ManifestStore) {
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
		if err := Provide(ctx, kdht, privKey, id); err != nil {
			fmt.Printf("[DHT Republisher] Failed to provide %x: %v\n", id, err)
		}
	}
}

