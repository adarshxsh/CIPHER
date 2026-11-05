package availability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

	contract "cipher/availability/availability-contracts/contract"
	"cipher/network/content/core"
	"cipher/network/content/manifest"
	"cipher/network/content/storage"
)

func TestStorageChunkCountResolver(t *testing.T) {
	tempDir := t.TempDir()
	fsStore := storage.NewFSStore(tempDir)

	resolver := NewStorageChunkCountResolver(fsStore)
	if err := resolver.ConfigureGlobal(); err != nil {
		t.Fatalf("failed to globally configure resolver: %v", err)
	}

	// 1. Create a dummy manifest with 5 chunks
	var contentID core.ContentID
	rand.Read(contentID[:])

	chunks := make([]core.ChunkID, 5)
	for i := range chunks {
		rand.Read(chunks[i][:])
	}

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   contentID,
			Type: manifest.TypeFile,
			Size: 5 * 32 * 1024,
		},
		ChunkIDs: chunks,
	}

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize manifest: %v", err)
	}

	// Store manifest in FSStore
	if err := fsStore.PutManifestBytes(context.Background(), contentID, mBytes); err != nil {
		t.Fatalf("failed to store manifest: %v", err)
	}

	contentIDHex := hex.EncodeToString(contentID[:])

	// 2. Query resolver
	count, err := resolver.ChunkCount("provider-1", contentIDHex)
	if err != nil {
		t.Fatalf("ChunkCount failed: %v", err)
	}
	if count != 5 {
		t.Fatalf("expected 5 chunks, got %d", count)
	}

	// 3. Verify Availability Contract integration
	contractID, err := contract.CreateAvailabilityContract("publisher-test", "provider-test", contentIDHex, 500, time.Hour)
	if err != nil {
		t.Fatalf("CreateAvailabilityContract failed: %v", err)
	}
	if contractID == "" {
		t.Fatal("expected non-empty contractID")
	}

	// Fund contract
	state, err := contract.FundAvailabilityContract(contractID, 500)
	if err != nil {
		t.Fatalf("FundAvailabilityContract failed: %v", err)
	}
	if state == "" {
		t.Fatal("expected non-empty state after funding")
	}
}
