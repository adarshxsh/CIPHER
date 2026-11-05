package demandMetadata

import (
	"errors"
	"time"

	"proof-of-request/model"
)

func CreateDemandMetadata(
	fileID string,
	demand int,
	replicaCount int,
	timestamp time.Time,
	version uint64,
) (model.DemandMetadata, error) {

	if fileID == "" {
		return model.DemandMetadata{}, errors.New("file ID cannot be empty")
	}

	if demand < 0 {
		return model.DemandMetadata{}, errors.New("demand cannot be negative")
	}

	if replicaCount < 0 {
		return model.DemandMetadata{}, errors.New("replica count cannot be negative")
	}

	if timestamp.IsZero() {
		return model.DemandMetadata{}, errors.New("timestamp cannot be zero")
	}

	return model.DemandMetadata{
		FileID:       fileID,
		Demand:       demand,
		ReplicaCount: replicaCount,
		Timestamp:    timestamp,
		Version:      version,
	}, nil
}