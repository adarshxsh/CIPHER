package availability

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"cipher/network/content/manifest"
	"proof-of-request/cacheAnnouncement"
	"proof-of-request/demand"
	"proof-of-request/model"
	"proof-of-request/replica"
	"proof-of-request/request"
	"proof-of-request/security"
)

// DemandAndReplicaManager coordinates proof-of-request components:
// cache announcements, replica tracking across providers, and request demand with PoW.
type DemandAndReplicaManager struct {
	replicaTracker *replica.Tracker
}

// NewDemandAndReplicaManager creates a new demand and replica manager.
func NewDemandAndReplicaManager() *DemandAndReplicaManager {
	return &DemandAndReplicaManager{
		replicaTracker: replica.NewTracker(),
	}
}

// CreateSignedCacheAnnouncement generates and signs a cache announcement for a manifest.
func CreateSignedCacheAnnouncement(
	providerID string,
	privKey ed25519.PrivateKey,
	m *manifest.Manifest,
	ttl time.Duration,
) (model.SignedCacheAnnouncement, error) {
	if m == nil {
		return model.SignedCacheAnnouncement{}, errors.New("manifest cannot be nil")
	}

	fileID := hex.EncodeToString(m.Descriptor.ID[:])
	merkleRoot := hex.EncodeToString(m.MerkleRoot[:])
	expiry := time.Now().UTC().Add(ttl)

	ann, err := cacheAnnouncement.CreateCacheAnnouncement(
		providerID,
		fileID,
		merkleRoot,
		uint64(m.Version),
		expiry,
	)
	if err != nil {
		return model.SignedCacheAnnouncement{}, fmt.Errorf("create cache announcement: %w", err)
	}

	signed, err := cacheAnnouncement.SignCacheAnnouncement(ann, privKey)
	if err != nil {
		return model.SignedCacheAnnouncement{}, fmt.Errorf("sign cache announcement: %w", err)
	}

	return signed, nil
}

// VerifyCacheAnnouncement validates the Ed25519 signature of a cache announcement.
func VerifyCacheAnnouncement(signed model.SignedCacheAnnouncement, pubKey ed25519.PublicKey) bool {
	return cacheAnnouncement.VerifyCacheAnnouncement(signed, pubKey)
}

// IngestAnnouncement validates an announcement and updates the replica tracker.
func (m *DemandAndReplicaManager) IngestAnnouncement(
	signed model.SignedCacheAnnouncement,
	pubKey ed25519.PublicKey,
) error {
	if !VerifyCacheAnnouncement(signed, pubKey) {
		return errors.New("invalid cache announcement signature")
	}

	ann := signed.Announcement
	if time.Now().UTC().After(ann.Expiry) {
		return errors.New("cache announcement has expired")
	}

	return m.replicaTracker.AddOrUpdateReplica(model.Replica{
		ProviderID: ann.ProviderID,
		FileID:     ann.FileID,
		Version:    ann.Version,
		State:      model.Active,
	})
}

// GetReplicaCount returns the number of active replicas tracked for a file.
func (m *DemandAndReplicaManager) GetReplicaCount(fileID string, version uint64) (int, error) {
	return m.replicaTracker.GetReplicaCount(fileID, version)
}

// ValidateAndTrackRequest verifies a consumer request's Proof-of-Work (Hashcash)
// before recording it in the pending request store.
func (m *DemandAndReplicaManager) ValidateAndTrackRequest(
	req model.Request,
	pow model.PoW,
	lifetime time.Duration,
) (model.Request, error) {
	// 1. Verify Proof-of-Work
	if !security.VerifyPoW(req, pow) {
		return model.Request{}, errors.New("proof of work verification failed")
	}

	// 2. Track request in the pending request pool
	trackedReq, _, err := request.GetOrCreateRequest(req.FileID, req.ClientID, lifetime)
	if err != nil {
		return model.Request{}, fmt.Errorf("track request: %w", err)
	}

	return trackedReq, nil
}

// CalculateDemand returns the active demand for a file.
func (m *DemandAndReplicaManager) CalculateDemand(fileID string) (int, error) {
	return demand.CalculateDemand(fileID)
}

// ReplicaTracker returns the underlying replica tracker.
func (m *DemandAndReplicaManager) ReplicaTracker() *replica.Tracker {
	return m.replicaTracker
}
