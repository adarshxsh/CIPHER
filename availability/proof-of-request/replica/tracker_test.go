package replica

import (
	"testing"

	"proof-of-request/model"
)

func TestGetReplicaCount(t *testing.T) {
	tracker := NewTracker()

	err := tracker.AddOrUpdateReplica(model.Replica{
		ProviderID: "provider-1",
		FileID:     "file-1",
		Version:    1,
		State:      model.Active,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	count, err := tracker.GetReplicaCount("file-1", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected 1 replica, got %d", count)
	}
}

func TestGetReplicaCountMultipleProviders(t *testing.T) {
	tracker := NewTracker()

	replicas := []model.Replica{
		{
			ProviderID: "provider-1",
			FileID:     "file-1",
			Version:    1,
			State:      model.Active,
		},
		{
			ProviderID: "provider-2",
			FileID:     "file-1",
			Version:    1,
			State:      model.Active,
		},
	}

	for _, replica := range replicas {
		if err := tracker.AddOrUpdateReplica(replica); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	count, err := tracker.GetReplicaCount("file-1", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if count != 2 {
		t.Fatalf("expected 2 replicas, got %d", count)
	}
}

func TestAddOrUpdateReplicaDoesNotDuplicate(t *testing.T) {
	tracker := NewTracker()

	replica := model.Replica{
		ProviderID: "provider-1",
		FileID:     "file-1",
		Version:    1,
		State:      model.Active,
	}

	if err := tracker.AddOrUpdateReplica(replica); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := tracker.AddOrUpdateReplica(replica); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	count, err := tracker.GetReplicaCount("file-1", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected 1 replica after duplicate update, got %d", count)
	}
}

func TestRemoveReplica(t *testing.T) {
	tracker := NewTracker()

	err := tracker.AddOrUpdateReplica(model.Replica{
		ProviderID: "provider-1",
		FileID:     "file-1",
		Version:    1,
		State:      model.Active,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	removed := tracker.RemoveReplica("provider-1", "file-1", 1)

	if !removed {
		t.Fatal("expected replica to be removed")
	}

	count, err := tracker.GetReplicaCount("file-1", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if count != 0 {
		t.Fatalf("expected 0 replicas after removal, got %d", count)
	}
}

func TestGetReplicaCountIgnoresDifferentFileAndVersion(t *testing.T) {
	tracker := NewTracker()

	replicas := []model.Replica{
		{
			ProviderID: "provider-1",
			FileID:     "file-1",
			Version:    1,
			State:      model.Active,
		},
		{
			ProviderID: "provider-2",
			FileID:     "file-2",
			Version:    1,
			State:      model.Active,
		},
		{
			ProviderID: "provider-3",
			FileID:     "file-1",
			Version:    2,
			State:      model.Active,
		},
	}

	for _, replica := range replicas {
		if err := tracker.AddOrUpdateReplica(replica); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	count, err := tracker.GetReplicaCount("file-1", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected 1 replica, got %d", count)
	}
}