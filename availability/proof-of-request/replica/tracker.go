package replica

import (
	"errors"
	"sync"

	"proof-of-request/model"
)

type Tracker struct {
	mu       sync.RWMutex
	replicas []model.Replica
}

func NewTracker() *Tracker {
	return &Tracker{
		replicas: make([]model.Replica, 0),
	}
}

func (t *Tracker) AddOrUpdateReplica(replica model.Replica) error {
	if replica.ProviderID == "" {
		return errors.New("provider ID cannot be empty")
	}

	if replica.FileID == "" {
		return errors.New("file ID cannot be empty")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for i := range t.replicas {
		existing := &t.replicas[i]

		if existing.ProviderID == replica.ProviderID &&
			existing.FileID == replica.FileID &&
			existing.Version == replica.Version {

			existing.State = replica.State
			return nil
		}
	}

	t.replicas = append(t.replicas, replica)

	return nil
}

func (t *Tracker) GetReplicaCount(fileID string, version uint64) (int, error) {
	if fileID == "" {
		return 0, errors.New("file ID cannot be empty")
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	count := 0

	for _, replica := range t.replicas {
		if replica.FileID == fileID &&
			replica.Version == version &&
			replica.State == model.Active {
			count++
		}
	}

	return count, nil
}

func (t *Tracker) RemoveReplica(
	providerID string,
	fileID string,
	version uint64,
) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i := range t.replicas {
		replica := &t.replicas[i]

		if replica.ProviderID == providerID &&
			replica.FileID == fileID &&
			replica.Version == version &&
			replica.State == model.Active {

			replica.State = model.Removed
			return true
		}
	}

	return false
}