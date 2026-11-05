package cacheAnnouncement

import (
	"errors"
	"time"

	"proof-of-request/model"
)

func CreateCacheAnnouncement(
	providerID string,
	fileID string,
	merkleRoot string,
	version uint64,
	expiry time.Time,
) (model.CacheAnnouncement, error) {

	if providerID == "" {
		return model.CacheAnnouncement{}, errors.New("provider ID cannot be empty")
	}

	if fileID == "" {
		return model.CacheAnnouncement{}, errors.New("file ID cannot be empty")
	}

	if merkleRoot == "" {
		return model.CacheAnnouncement{}, errors.New("Merkle root cannot be empty")
	}

	if expiry.IsZero() {
		return model.CacheAnnouncement{}, errors.New("expiry cannot be zero")
	}

	return model.CacheAnnouncement{
		ProviderID: providerID,
		FileID:     fileID,
		MerkleRoot: merkleRoot,
		Version:    version,
		Expiry:     expiry,
	}, nil
}