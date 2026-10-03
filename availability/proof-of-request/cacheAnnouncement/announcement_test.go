package cacheAnnouncement

import (
	"testing"
	"time"


)

func TestCreateCacheAnnouncement(t *testing.T) {
	expiry := time.Now().Add(time.Hour)

	announcement, err := CreateCacheAnnouncement(
		"provider-1",
		"file-1",
		"merkle-root-1",
		1,
		expiry,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if announcement.ProviderID != "provider-1" {
		t.Fatalf("expected provider-1, got %s", announcement.ProviderID)
	}

	if announcement.FileID != "file-1" {
		t.Fatalf("expected file-1, got %s", announcement.FileID)
	}

	if announcement.MerkleRoot != "merkle-root-1" {
		t.Fatalf("unexpected Merkle root: %s", announcement.MerkleRoot)
	}

	if announcement.Version != 1 {
		t.Fatalf("expected version 1, got %d", announcement.Version)
	}

	if !announcement.Expiry.Equal(expiry) {
		t.Fatalf("unexpected expiry")
	}
}

func TestCreateCacheAnnouncementRejectsEmptyProviderID(t *testing.T) {
	_, err := CreateCacheAnnouncement(
		"",
		"file-1",
		"merkle-root-1",
		1,
		time.Now().Add(time.Hour),
	)

	if err == nil {
		t.Fatal("expected error for empty provider ID")
	}
}

func TestCreateCacheAnnouncementRejectsEmptyFileID(t *testing.T) {
	_, err := CreateCacheAnnouncement(
		"provider-1",
		"",
		"merkle-root-1",
		1,
		time.Now().Add(time.Hour),
	)

	if err == nil {
		t.Fatal("expected error for empty file ID")
	}
}

func TestCreateCacheAnnouncementRejectsEmptyMerkleRoot(t *testing.T) {
	_, err := CreateCacheAnnouncement(
		"provider-1",
		"file-1",
		"",
		1,
		time.Now().Add(time.Hour),
	)

	if err == nil {
		t.Fatal("expected error for empty Merkle root")
	}
}

func TestCreateCacheAnnouncementRejectsZeroExpiry(t *testing.T) {
	_, err := CreateCacheAnnouncement(
		"provider-1",
		"file-1",
		"merkle-root-1",
		1,
		time.Time{},
	)

	if err == nil {
		t.Fatal("expected error for zero expiry")
	}
}

func TestCreateCacheAnnouncementAllowsVersionZero(t *testing.T) {
	announcement, err := CreateCacheAnnouncement(
		"provider-1",
		"file-1",
		"merkle-root-1",
		0,
		time.Now().Add(time.Hour),
	)

	if err != nil {
		t.Fatalf("version 0 should currently be allowed: %v", err)
	}

	if announcement.Version != 0 {
		t.Fatalf("expected version 0, got %d", announcement.Version)
	}
}

func TestCreateCacheAnnouncementAllowsExpiredTime(t *testing.T) {
	expiry := time.Now().Add(-time.Hour)

	announcement, err := CreateCacheAnnouncement(
		"provider-1",
		"file-1",
		"merkle-root-1",
		1,
		expiry,
	)

	if err != nil {
		t.Fatalf("expired time should currently be allowed during construction: %v", err)
	}

	if !announcement.Expiry.Equal(expiry) {
		t.Fatal("expiry was not preserved")
	}
}

