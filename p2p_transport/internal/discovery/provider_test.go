package discovery_test

import (
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/discovery"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestAttestationCreationAndVerification(t *testing.T) {
	pubPriv, pubPubKey, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate publisher key: %v", err)
	}

	_, provPubKey, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate provider key: %v", err)
	}

	provID, err := peer.IDFromPublicKey(provPubKey)
	if err != nil {
		t.Fatalf("failed to get provider ID: %v", err)
	}

	var contentID core.ContentID
	copy(contentID[:], []byte("sample_content_id_for_attest_12"))

	att, err := discovery.CreateAttestation(pubPriv, contentID, provID)
	if err != nil {
		t.Fatalf("CreateAttestation failed: %v", err)
	}

	if err := discovery.VerifyAttestation(att, pubPubKey, contentID, provID); err != nil {
		t.Fatalf("VerifyAttestation failed: %v", err)
	}
}

func TestAttestationVerificationMismatches(t *testing.T) {
	pubPriv, pubPubKey, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate publisher key: %v", err)
	}

	_, provPubKey, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate provider key: %v", err)
	}
	provID, _ := peer.IDFromPublicKey(provPubKey)

	_, otherProvPubKey, _ := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	otherProvID, _ := peer.IDFromPublicKey(otherProvPubKey)

	var contentID core.ContentID
	copy(contentID[:], []byte("sample_content_id_for_attest_12"))

	var otherContentID core.ContentID
	copy(otherContentID[:], []byte("other_content_id_for_attest_123"))

	att, err := discovery.CreateAttestation(pubPriv, contentID, provID)
	if err != nil {
		t.Fatalf("CreateAttestation failed: %v", err)
	}

	// 1. Content ID mismatch
	if err := discovery.VerifyAttestation(att, pubPubKey, otherContentID, provID); err == nil {
		t.Fatalf("expected failure on content ID mismatch, got nil")
	}

	// 2. Provider ID mismatch
	if err := discovery.VerifyAttestation(att, pubPubKey, contentID, otherProvID); err == nil {
		t.Fatalf("expected failure on provider ID mismatch, got nil")
	}

	// 3. Wrong publisher public key
	_, wrongPubPubKey, _ := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err := discovery.VerifyAttestation(att, wrongPubPubKey, contentID, provID); err == nil {
		t.Fatalf("expected failure on wrong publisher key, got nil")
	}
}
