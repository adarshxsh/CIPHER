package demandMetadata

import (
	"testing"
	"time"
)

func TestCreateDemandMetadata(t *testing.T) {
	timestamp := time.Now()

	metadata, err := CreateDemandMetadata(
		"file-1",
		5,
		3,
		timestamp,
		1,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if metadata.FileID != "file-1" {
		t.Errorf("expected FileID file-1, got %s", metadata.FileID)
	}

	if metadata.Demand != 5 {
		t.Errorf("expected demand 5, got %d", metadata.Demand)
	}

	if metadata.ReplicaCount != 3 {
		t.Errorf("expected replica count 3, got %d", metadata.ReplicaCount)
	}

	if !metadata.Timestamp.Equal(timestamp) {
		t.Errorf("timestamp was not preserved")
	}

	if metadata.Version != 1 {
		t.Errorf("expected version 1, got %d", metadata.Version)
	}
}

func TestCreateDemandMetadataRejectsEmptyFileID(t *testing.T) {
	_, err := CreateDemandMetadata(
		"",
		5,
		3,
		time.Now(),
		1,
	)

	if err == nil {
		t.Fatal("expected error for empty file ID")
	}
}

func TestCreateDemandMetadataRejectsNegativeDemand(t *testing.T) {
	_, err := CreateDemandMetadata(
		"file-1",
		-1,
		3,
		time.Now(),
		1,
	)

	if err == nil {
		t.Fatal("expected error for negative demand")
	}
}

func TestCreateDemandMetadataRejectsNegativeReplicaCount(t *testing.T) {
	_, err := CreateDemandMetadata(
		"file-1",
		5,
		-1,
		time.Now(),
		1,
	)

	if err == nil {
		t.Fatal("expected error for negative replica count")
	}
}

func TestCreateDemandMetadataRejectsZeroTimestamp(t *testing.T) {
	_, err := CreateDemandMetadata(
		"file-1",
		5,
		3,
		time.Time{},
		1,
	)

	if err == nil {
		t.Fatal("expected error for zero timestamp")
	}
}

func TestCreateDemandMetadataAllowsZeroValues(t *testing.T) {
	_, err := CreateDemandMetadata(
		"file-1",
		0,
		0,
		time.Now(),
		0,
	)

	if err != nil {
		t.Fatalf("expected zero demand, replica count, and version to be valid: %v", err)
	}
}