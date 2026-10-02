package availability

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"cipher/network/content/core"
	"cipher/network/content/manifest"
	"proof-of-request/model"
	"proof-of-request/request"
	"proof-of-request/security"
)

func TestDemandAndReplicaManager(t *testing.T) {
	pubKey1, privKey1, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key 1 failed: %v", err)
	}

	pubKey2, privKey2, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key 2 failed: %v", err)
	}

	var contentID core.ContentID
	rand.Read(contentID[:])

	var rootHash core.Hash
	rand.Read(rootHash[:])

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   contentID,
			Type: manifest.TypeFile,
			Size: 1024,
		},
		MerkleRoot: rootHash,
	}

	mgr := NewDemandAndReplicaManager()

	// 1. Provider 1 creates signed announcement
	ann1, err := CreateSignedCacheAnnouncement("provider-1", privKey1, m, time.Hour)
	if err != nil {
		t.Fatalf("CreateSignedCacheAnnouncement 1 failed: %v", err)
	}

	if err := mgr.IngestAnnouncement(ann1, pubKey1); err != nil {
		t.Fatalf("IngestAnnouncement 1 failed: %v", err)
	}

	// 2. Provider 2 creates signed announcement
	ann2, err := CreateSignedCacheAnnouncement("provider-2", privKey2, m, time.Hour)
	if err != nil {
		t.Fatalf("CreateSignedCacheAnnouncement 2 failed: %v", err)
	}

	if err := mgr.IngestAnnouncement(ann2, pubKey2); err != nil {
		t.Fatalf("IngestAnnouncement 2 failed: %v", err)
	}

	// 3. Verify replica count is 2
	count, err := mgr.GetReplicaCount(ann1.Announcement.FileID, ann1.Announcement.Version)
	if err != nil {
		t.Fatalf("GetReplicaCount failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 active replicas, got %d", count)
	}

	// 4. Test Adversarial forged signature
	tamperedAnn := ann1
	tamperedAnn.Signature = make([]byte, len(ann1.Signature))
	copy(tamperedAnn.Signature, ann1.Signature)
	tamperedAnn.Signature[0] ^= 0xFF

	if err := mgr.IngestAnnouncement(tamperedAnn, pubKey1); err == nil {
		t.Fatal("expected forged announcement signature to fail, but succeeded")
	}

	// 5. Test PoW request tracking
	diff := uint8(1) // 1 leading hex zero
	req, err := request.CreateRequest(ann1.Announcement.FileID, "client-123")
	if err != nil {
		t.Fatalf("CreateRequest failed: %v", err)
	}

	pow := security.GeneratePoW(req, diff)

	trackedReq, err := mgr.ValidateAndTrackRequest(req, pow, 10*time.Minute)
	if err != nil {
		t.Fatalf("ValidateAndTrackRequest failed: %v", err)
	}
	if trackedReq.Status != model.Pending {
		t.Fatalf("expected request status Pending, got %s", trackedReq.Status)
	}

	// 6. Check demand calculation
	demandCount, err := mgr.CalculateDemand(ann1.Announcement.FileID)
	if err != nil {
		t.Fatalf("CalculateDemand failed: %v", err)
	}
	if demandCount < 1 {
		t.Fatalf("expected at least 1 active request in demand count, got %d", demandCount)
	}
}
