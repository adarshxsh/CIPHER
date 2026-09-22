package robustness_test

import (
	"errors"
	"runtime"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
)

// TestManifestRobustness_OversizedMemoryProtection verifies that receiving malicious
// oversized manifest payloads fails gracefully without exhausting node heap memory footprint.
func TestManifestRobustness_OversizedMemoryProtection(t *testing.T) {
	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	// Simulate an adversarial peer sending an oversized 10 MB manifest response
	const oversizedDataLen = 10 * 1024 * 1024 // 10 MB
	maliciousManifestData := make([]byte, oversizedDataLen)
	for i := range maliciousManifestData {
		maliciousManifestData[i] = 'A'
	}

	var contentID core.ContentID
	contentID[0] = 0xDE
	contentID[31] = 0xAD

	// Construct oversized wire payload
	wirePayload := append(contentID[:], maliciousManifestData...)

	// 1. Verify ValidateManifestPayload rejects it
	err := chunk.ValidateManifestPayload(wirePayload)
	if err == nil {
		t.Fatal("expected ValidateManifestPayload to reject 10MB wire payload, got nil")
	}
	if !errors.Is(err, chunk.ErrInvalidPayloadLength) {
		t.Fatalf("expected ErrInvalidPayloadLength, got %v", err)
	}

	// 2. Verify ParseManifest rejects raw manifest data slice > MaxManifestSize
	_, _, err = chunk.ParseManifest(wirePayload)
	if err == nil {
		t.Fatal("expected ParseManifest to reject 10MB manifest slice, got nil")
	}

	// 3. Verify manifest.Deserialize rejects pre-unmarshaling byte slice > MaxManifestSize
	_, err = manifest.Deserialize(maliciousManifestData)
	if err == nil {
		t.Fatal("expected manifest.Deserialize to reject 10MB raw data, got nil")
	}
	if !errors.Is(err, manifest.ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got %v", err)
	}

	// Release local references and trigger garbage collection
	wirePayload = nil
	maliciousManifestData = nil
	runtime.GC()

	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)

	// Verify heap allocation growth is tightly bounded
	var heapAllocDiff int64
	if msAfter.HeapAlloc > msBefore.HeapAlloc {
		heapAllocDiff = int64(msAfter.HeapAlloc - msBefore.HeapAlloc)
	} else {
		heapAllocDiff = 0
	}

	// Heap growth should remain bounded well under 1 MB after GC
	const maxAllowedHeapGrowth = 1024 * 1024 // 1 MB
	if heapAllocDiff > maxAllowedHeapGrowth {
		t.Fatalf("excessive heap memory retention after rejected oversized manifest: %d bytes (limit: %d bytes)",
			heapAllocDiff, maxAllowedHeapGrowth)
	}
}
