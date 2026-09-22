package chunk_test

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/identity"
	"cipher/internal/protocol/chunk"
)

func TestProviderAttestation_SignAndVerify(t *testing.T) {
	priv, err := identity.GenerateEphemeral()
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	pub := priv.GetPublic()
	peerID, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("Failed to derive peer ID: %v", err)
	}

	var contentID core.ContentID
	contentID[0] = 0x12

	att, err := chunk.NewProviderAttestation(contentID, peerID, time.Now().Unix(), priv)
	if err != nil {
		t.Fatalf("Failed to create attestation: %v", err)
	}

	if err := att.Verify(contentID, peerID); err != nil {
		t.Fatalf("Attestation verification failed: %v", err)
	}
}

func TestProviderAttestation_SpoofedProviderID(t *testing.T) {
	priv1, _ := identity.GenerateEphemeral()
	priv2, _ := identity.GenerateEphemeral()

	peerID1, _ := peer.IDFromPublicKey(priv1.GetPublic())
	peerID2, _ := peer.IDFromPublicKey(priv2.GetPublic())

	var contentID core.ContentID
	contentID[0] = 0x34

	// Provider 1 signs attestation but claims to be Provider 2
	att, err := chunk.NewProviderAttestation(contentID, peerID2, time.Now().Unix(), priv1)
	if err != nil {
		t.Fatalf("Failed to create attestation: %v", err)
	}

	// Connected peer is Provider 1
	if err := att.Verify(contentID, peerID1); err == nil {
		t.Fatalf("Expected verification failure for spoofed provider peer ID")
	}
}

func TestProviderAttestation_ExpiredTimestamp(t *testing.T) {
	priv, _ := identity.GenerateEphemeral()
	peerID, _ := peer.IDFromPublicKey(priv.GetPublic())

	var contentID core.ContentID
	contentID[0] = 0x56

	// Attestation generated 2 hours ago (older than MaxAttestationAgeSeconds = 3600s)
	expiredTimestamp := time.Now().Unix() - 7200
	att, err := chunk.NewProviderAttestation(contentID, peerID, expiredTimestamp, priv)
	if err != nil {
		t.Fatalf("Failed to create attestation: %v", err)
	}

	if err := att.Verify(contentID, peerID); err == nil {
		t.Fatalf("Expected verification failure for expired timestamp")
	}
}

func TestProviderAttestation_ContentIDMismatch(t *testing.T) {
	priv, _ := identity.GenerateEphemeral()
	peerID, _ := peer.IDFromPublicKey(priv.GetPublic())

	var contentID1, contentID2 core.ContentID
	contentID1[0] = 0x01
	contentID2[0] = 0x02

	att, err := chunk.NewProviderAttestation(contentID1, peerID, time.Now().Unix(), priv)
	if err != nil {
		t.Fatalf("Failed to create attestation: %v", err)
	}

	if err := att.Verify(contentID2, peerID); err == nil {
		t.Fatalf("Expected failure when content ID does not match expected")
	}
}
