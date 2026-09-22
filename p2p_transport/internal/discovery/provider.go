package discovery

import (
	"bytes"
	"cipher/internal/content/core"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/multiformats/go-multiaddr"
)

// StorageProviderNamespace is a well-known identifier used by nodes offering storage capacity
// over the /cipher/push/1.0.0 protocol to register on the Kademlia DHT.
var StorageProviderNamespace = core.ContentID{
	0x43, 0x49, 0x50, 0x48, 0x45, 0x52, 0x5f, 0x53, // "CIPHER_S"
	0x54, 0x4f, 0x52, 0x41, 0x47, 0x45, 0x5f, 0x50, // "TORAGE_P"
	0x52, 0x4f, 0x56, 0x49, 0x44, 0x45, 0x52, 0x5f, // "ROVIDER_"
	0x53, 0x45, 0x52, 0x56, 0x49, 0x43, 0x45, 0x01, // "SERVICE\x01"
}

const ProvideProtocolID = "/cipher/dht/provide/1.0.0"

var ErrAuthorizationFailed = fmt.Errorf("authorization error: unauthenticated or invalid provider announcement signature")

// SignedProviderRecord represents a cryptographically signed Provide announcement payload.
type SignedProviderRecord struct {
	ContentID  core.ContentID `json:"content_id"`
	PeerID     peer.ID        `json:"peer_id"`
	Multiaddrs []string       `json:"multiaddrs"`
	Timestamp  int64          `json:"timestamp"`
	PubKey     []byte         `json:"pub_key"`
	Signature  []byte         `json:"signature"`
}

// SigningBytes constructs the canonical byte payload for signing and signature verification.
func (r *SignedProviderRecord) SigningBytes() []byte {
	var buf bytes.Buffer
	buf.WriteString("CIPHER-PROVIDE-V1:")
	buf.Write(r.ContentID[:])
	buf.WriteString(":")
	buf.WriteString(r.PeerID.String())
	buf.WriteString(":")

	addrs := make([]string, len(r.Multiaddrs))
	copy(addrs, r.Multiaddrs)
	sort.Strings(addrs)
	for _, a := range addrs {
		buf.WriteString(a)
		buf.WriteString(",")
	}
	buf.WriteString(":")
	buf.WriteString(fmt.Sprintf("%d", r.Timestamp))
	return buf.Bytes()
}

// NewSignedProviderRecord creates and signs a provider announcement payload.
func NewSignedProviderRecord(contentID core.ContentID, pID peer.ID, addrs []multiaddr.Multiaddr, privKey crypto.PrivKey) (*SignedProviderRecord, error) {
	if privKey == nil {
		return nil, fmt.Errorf("%w: missing private key for signature generation", ErrAuthorizationFailed)
	}

	pubKey := privKey.GetPublic()
	pubBytes, err := crypto.MarshalPublicKey(pubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal public key: %w", err)
	}

	addrStrs := make([]string, len(addrs))
	for i, a := range addrs {
		addrStrs[i] = a.String()
	}

	rec := &SignedProviderRecord{
		ContentID:  contentID,
		PeerID:     pID,
		Multiaddrs: addrStrs,
		Timestamp:  time.Now().UnixNano(),
		PubKey:     pubBytes,
	}

	sig, err := privKey.Sign(rec.SigningBytes())
	if err != nil {
		return nil, fmt.Errorf("failed to sign provider payload: %w", err)
	}
	rec.Signature = sig

	return rec, nil
}

// VerifySignedProviderRecord validates the cryptographic signature of a SignedProviderRecord.
func VerifySignedProviderRecord(rec *SignedProviderRecord) error {
	if rec == nil {
		return fmt.Errorf("%w: nil record", ErrAuthorizationFailed)
	}
	if len(rec.Signature) == 0 || len(rec.PubKey) == 0 {
		return fmt.Errorf("%w: unsigned record or missing public key", ErrAuthorizationFailed)
	}

	pubKey, err := crypto.UnmarshalPublicKey(rec.PubKey)
	if err != nil {
		return fmt.Errorf("%w: invalid public key: %v", ErrAuthorizationFailed, err)
	}

	expectedPeerID, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		return fmt.Errorf("%w: failed to derive peer ID from public key: %v", ErrAuthorizationFailed, err)
	}

	if expectedPeerID != rec.PeerID {
		return fmt.Errorf("%w: peer ID %s does not match public key peer ID %s", ErrAuthorizationFailed, rec.PeerID, expectedPeerID)
	}

	valid, err := pubKey.Verify(rec.SigningBytes(), rec.Signature)
	if err != nil || !valid {
		return fmt.Errorf("%w: signature verification failed", ErrAuthorizationFailed)
	}

	return nil
}

// RegisterProvideHandler registers the stream handler for signed provider announcements on the host.
func RegisterProvideHandler(h host.Host, kdht *dht.IpfsDHT) {
	if h == nil {
		return
	}

	h.SetStreamHandler(ProvideProtocolID, func(s network.Stream) {
		defer s.Close()
		start := time.Now()

		_ = s.SetDeadline(time.Now().Add(5 * time.Second))
		var rec SignedProviderRecord
		if err := json.NewDecoder(s).Decode(&rec); err != nil {
			log.Printf("[DHT Security] REJECTED Provide announcement from %s: failed to decode payload: %v", s.Conn().RemotePeer(), err)
			return
		}

		if s.Conn().RemotePeer() != rec.PeerID {
			log.Printf("[DHT Security] REJECTED Provide announcement: sender peer ID %s does not match record peer ID %s", s.Conn().RemotePeer(), rec.PeerID)
			return
		}

		if err := VerifySignedProviderRecord(&rec); err != nil {
			log.Printf("[DHT Security] REJECTED unauthenticated Provide announcement for ContentID %x from peer %s: %v", rec.ContentID, rec.PeerID, err)
			return
		}

		elapsed := time.Since(start)
		log.Printf("[DHT Security] VERIFIED signature for ContentID %x from peer %s in %v", rec.ContentID, rec.PeerID, elapsed)

		var maddrs []multiaddr.Multiaddr
		for _, addrStr := range rec.Multiaddrs {
			if ma, err := multiaddr.NewMultiaddr(addrStr); err == nil {
				maddrs = append(maddrs, ma)
			}
		}

		if len(maddrs) > 0 {
			h.Peerstore().AddAddrs(rec.PeerID, maddrs, peerstore.AddressTTL)
		}

		if kdht != nil {
			cid, err := contentIDToCID(rec.ContentID)
			if err == nil {
				if err := kdht.ProviderStore().AddProvider(context.Background(), cid.Hash(), peer.AddrInfo{
					ID:    rec.PeerID,
					Addrs: maddrs,
				}); err != nil {
					log.Printf("[DHT Security] Warning: failed to store provider record from %s: %v", rec.PeerID, err)
				}
			}
			_, _ = kdht.RoutingTable().TryAddPeer(rec.PeerID, true, true)
		}
	})
}

// Provide announces to the DHT that this node can provide the content identified by the given ContentID.
func Provide(ctx context.Context, kdht *dht.IpfsDHT, h host.Host, priv crypto.PrivKey, id core.ContentID) error {
	if priv == nil {
		log.Printf("[DHT Security] REJECTED Provide call for ContentID %x: unsigned (missing private key)", id)
		return fmt.Errorf("%w: unsigned Provide call - private key required", ErrAuthorizationFailed)
	}

	var peerID peer.ID
	var addrs []multiaddr.Multiaddr
	if h != nil {
		peerID = h.ID()
		addrs = h.Addrs()
	} else if kdht != nil {
		peerID = kdht.PeerID()
	}

	record, err := NewSignedProviderRecord(id, peerID, addrs, priv)
	if err != nil {
		return err
	}

	if err := VerifySignedProviderRecord(record); err != nil {
		log.Printf("[DHT Security] REJECTED local Provide call: %v", err)
		return err
	}

	cid, err := contentIDToCID(id)
	if err != nil {
		return fmt.Errorf("failed to convert ContentID to CID: %w", err)
	}

	if kdht != nil {
		if err := kdht.ProviderStore().AddProvider(ctx, cid.Hash(), peer.AddrInfo{ID: peerID, Addrs: addrs}); err != nil {
			log.Printf("[DHT Security] Warning: failed to add provider to local store: %v", err)
		}
		_, _ = kdht.RoutingTable().TryAddPeer(peerID, true, true)
		if err := kdht.Provide(ctx, cid, true); err != nil {
			log.Printf("[DHT Security] Warning: kdht.Provide error: %v", err)
		}
	}

	if h != nil {
		broadcastSignedProvide(ctx, h, kdht, record)
	}

	return nil
}

// RegisterStorageProvider announces to the DHT that this node is an active storage provider.
func RegisterStorageProvider(ctx context.Context, kdht *dht.IpfsDHT, h host.Host, priv crypto.PrivKey) error {
	return Provide(ctx, kdht, h, priv, StorageProviderNamespace)
}

// StartStorageProviderHeartbeat periodically re-announces the storage provider service to the DHT.
func StartStorageProviderHeartbeat(ctx context.Context, h host.Host, kdht *dht.IpfsDHT, priv crypto.PrivKey, interval time.Duration) {
	go func() {
		// Initial announcement
		if err := RegisterStorageProvider(ctx, kdht, h, priv); err != nil {
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
				if err := RegisterStorageProvider(ctx, kdht, h, priv); err != nil {
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

func broadcastSignedProvide(ctx context.Context, h host.Host, kdht *dht.IpfsDHT, record *SignedProviderRecord) {
	peerMap := make(map[peer.ID]bool)
	if kdht != nil {
		for _, p := range kdht.RoutingTable().ListPeers() {
			if p != h.ID() {
				peerMap[p] = true
			}
		}
	}
	for _, p := range h.Network().Peers() {
		if p != h.ID() {
			peerMap[p] = true
		}
	}

	payload, err := json.Marshal(record)
	if err != nil {
		return
	}

	for p := range peerMap {
		go func(targetPeer peer.ID) {
			sCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			s, err := h.NewStream(sCtx, targetPeer, ProvideProtocolID)
			if err != nil {
				return
			}
			defer s.Close()

			_ = s.SetDeadline(time.Now().Add(5 * time.Second))
			_, _ = s.Write(append(payload, '\n'))
		}(p)
	}
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
func StartRepublisher(ctx context.Context, h host.Host, kdht *dht.IpfsDHT, priv crypto.PrivKey, store core.ManifestStore, interval time.Duration) {
	go func() {
		// Republish immediately on startup
		republishAll(ctx, h, kdht, priv, store)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				republishAll(ctx, h, kdht, priv, store)
			}
		}
	}()
}

func republishAll(ctx context.Context, h host.Host, kdht *dht.IpfsDHT, priv crypto.PrivKey, store core.ManifestStore) {
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
		if err := Provide(ctx, kdht, h, priv, id); err != nil {
			fmt.Printf("[DHT Republisher] Failed to provide %x: %v\n", id, err)
		}
	}
}
