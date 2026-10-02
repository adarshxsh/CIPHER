package cacheAnnouncement

import (
	"crypto/ed25519"
	"testing"
	"time"

	"proof-of-request/model"
)

func TestSignCacheAnnouncement(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	announcement := model.CacheAnnouncement{
		ProviderID: "provider-1",
		FileID:     "file-1",
		MerkleRoot: "root-123",
		Version:    1,
		Expiry:     time.Now().Add(time.Hour),
	}

	signed, err := SignCacheAnnouncement(announcement, privateKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The original announcement should be preserved.
	if signed.Announcement != announcement {
		t.Fatal("signed announcement does not preserve the original announcement")
	}

	// Ed25519 signatures must have the standard signature size.
	if len(signed.Signature) != ed25519.SignatureSize {
		t.Fatalf(
			"expected signature length %d, got %d",
			ed25519.SignatureSize,
			len(signed.Signature),
		)
	}

	// The generated signature should actually verify.
	payload, err := buildSigningPayload(announcement)
	if err != nil {
		t.Fatalf("unexpected payload error: %v", err)
	}

	if !ed25519.Verify(publicKey, payload, signed.Signature) {
		t.Fatal("generated signature failed verification")
	}
}

func TestSignCacheAnnouncementRejectsInvalidAnnouncement(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	announcement := model.CacheAnnouncement{
		ProviderID: "provider-1",
		// FileID intentionally empty.
		MerkleRoot: "root-123",
		Version:    1,
		Expiry:     time.Now().Add(time.Hour),
	}

	_, err = SignCacheAnnouncement(announcement, privateKey)
	if err == nil {
		t.Fatal("expected error for empty file ID")
	}

	announcementWithEmptyProvider := model.CacheAnnouncement{
		ProviderID: "",
		FileID:     "file-1",
		MerkleRoot: "root-123",
		Version:    1,
		Expiry:     time.Now().Add(time.Hour),
	}

	_, err = SignCacheAnnouncement(announcementWithEmptyProvider, privateKey)
	if err == nil {
		t.Fatal("expected error for empty provider ID")
	}
}

func TestVerifyCacheAnnouncement(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	announcement := model.CacheAnnouncement{
		ProviderID: "provider-1",
		FileID:     "file-1",
		MerkleRoot: "root-123",
		Version:    1,
		Expiry:     time.Now().Add(time.Hour),
	}

	signed, err := SignCacheAnnouncement(announcement, privateKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !VerifyCacheAnnouncement(signed, publicKey) {
		t.Fatal("valid signature was rejected")
	}
}

func TestVerifyCacheAnnouncementRejectsModifiedProviderID(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	announcement := model.CacheAnnouncement{
		ProviderID: "provider-1",
		FileID:     "file-1",
		MerkleRoot: "root-123",
		Version:    1,
		Expiry:     time.Now().Add(time.Hour),
	}

	signed, err := SignCacheAnnouncement(announcement, privateKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	signed.Announcement.ProviderID = "provider-2"

	if VerifyCacheAnnouncement(signed, publicKey) {
		t.Fatal("modified ProviderID was accepted")
	}
}

func TestVerifyCacheAnnouncementRejectsModifiedFileID(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	announcement := model.CacheAnnouncement{
		ProviderID: "provider-1",
		FileID:     "file-1",
		MerkleRoot: "root-123",
		Version:    1,
		Expiry:     time.Now().Add(time.Hour),
	}

	signed, err := SignCacheAnnouncement(announcement, privateKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	signed.Announcement.FileID = "file-2"

	if VerifyCacheAnnouncement(signed, publicKey) {
		t.Fatal("modified FileID was accepted")
	}
}

func TestVerifyCacheAnnouncementRejectsModifiedMerkleRoot(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	announcement := model.CacheAnnouncement{
		ProviderID: "provider-1",
		FileID:     "file-1",
		MerkleRoot: "root-123",
		Version:    1,
		Expiry:     time.Now().Add(time.Hour),
	}

	signed, err := SignCacheAnnouncement(announcement, privateKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	signed.Announcement.MerkleRoot = "root-456"

	if VerifyCacheAnnouncement(signed, publicKey) {
		t.Fatal("modified Merkle root was accepted")
	}
}

func TestVerifyCacheAnnouncementRejectsModifiedVersion(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	announcement := model.CacheAnnouncement{
		ProviderID: "provider-1",
		FileID:     "file-1",
		MerkleRoot: "root-123",
		Version:    1,
		Expiry:     time.Now().Add(time.Hour),
	}

	signed, err := SignCacheAnnouncement(announcement, privateKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	signed.Announcement.Version = 2

	if VerifyCacheAnnouncement(signed, publicKey) {
		t.Fatal("modified version was accepted")
	}
}

func TestVerifyCacheAnnouncementRejectsModifiedExpiry(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	announcement := model.CacheAnnouncement{
		ProviderID: "provider-1",
		FileID:     "file-1",
		MerkleRoot: "root-123",
		Version:    1,
		Expiry:     time.Now().Add(time.Hour),
	}

	signed, err := SignCacheAnnouncement(announcement, privateKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	signed.Announcement.Expiry = signed.Announcement.Expiry.Add(time.Hour)

	if VerifyCacheAnnouncement(signed, publicKey) {
		t.Fatal("modified expiry was accepted")
	}
}

func TestVerifyCacheAnnouncementRejectsInvalidAnnouncement(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	announcement := model.CacheAnnouncement{
		ProviderID: "provider-1",
		FileID:     "file-1",
		MerkleRoot: "root-123",
		Version:    1,
		Expiry:     time.Now().Add(time.Hour),
	}

	signed, err := SignCacheAnnouncement(announcement, privateKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Make the announcement invalid after signing.
	signed.Announcement.FileID = ""

	if VerifyCacheAnnouncement(signed, publicKey) {
		t.Fatal("invalid announcement was accepted")
	}
}
