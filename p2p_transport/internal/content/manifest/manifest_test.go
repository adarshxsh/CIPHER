package manifest_test

import (
	"testing"

	"cipher/internal/content/manifest"
)

func TestDeserialize_MaxManifestSizeBound(t *testing.T) {
	maxAllowedData := manifest.MaxManifestSize - manifest.ContentIDSize

	// Data exactly at limit
	validSizeData := make([]byte, maxAllowedData)
	// Fill with empty JSON object to check size pass before unmarshal fail
	copy(validSizeData, []byte("{}"))
	_, err := manifest.Deserialize(validSizeData)
	if err != nil && err.Error() == "manifest data size exceeds maximum limit" {
		t.Fatalf("unexpected size limit error for valid data size: %v", err)
	}

	// Oversized data
	oversizedData := make([]byte, maxAllowedData+1)
	_, err = manifest.Deserialize(oversizedData)
	if err == nil {
		t.Fatalf("expected error for manifest data exceeding size limit, got nil")
	}
}
