package discovery

import (
	"cipher/internal/content/core"
	"encoding/json"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestSignedProviderRecord_SignAndVerify(t *testing.T) {
	priv, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}

	peerID, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("failed to get peer ID from pubkey: %v", err)
	}

	var contentID core.ContentID
	for i := range contentID {
		contentID[i] = byte(i + 1)
	}

	rec, err := NewSignedProviderRecord(contentID, peerID, priv, time.Hour)
	if err != nil {
		t.Fatalf("failed to create signed provider record: %v", err)
	}

	if err := rec.Verify(); err != nil {
		t.Fatalf("expected signature verification to pass, got: %v", err)
	}
}

func TestSignedProviderRecord_TamperedData(t *testing.T) {
	priv, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}

	peerID, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("failed to get peer ID: %v", err)
	}

	var contentID core.ContentID
	for i := range contentID {
		contentID[i] = byte(i + 1)
	}

	rec, err := NewSignedProviderRecord(contentID, peerID, priv, time.Hour)
	if err != nil {
		t.Fatalf("failed to create signed provider record: %v", err)
	}

	// Tamper signature
	rec.Signature[0] ^= 0xFF
	if err := rec.Verify(); err == nil {
		t.Fatal("expected signature verification to fail for tampered signature, but passed")
	}

	// Re-create and tamper ContentID
	rec, _ = NewSignedProviderRecord(contentID, peerID, priv, time.Hour)
	rec.ContentID[0] ^= 0xFF
	if err := rec.Verify(); err == nil {
		t.Fatal("expected signature verification to fail for tampered content ID, but passed")
	}
}

func TestSignedProviderRecord_WrongKey(t *testing.T) {
	privA, pubA, _ := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	_, pubB, _ := crypto.GenerateKeyPair(crypto.Ed25519, -1)

	peerIDA, _ := peer.IDFromPublicKey(pubA)
	var contentID core.ContentID

	rec, _ := NewSignedProviderRecord(contentID, peerIDA, privA, time.Hour)

	// Replace public key with pubB
	pubBBytes, _ := crypto.MarshalPublicKey(pubB)
	rec.PublisherPublicKey = pubBBytes

	if err := rec.Verify(); err == nil {
		t.Fatal("expected signature verification to fail with mismatched public key, but passed")
	}
}

func TestSignedProviderRecord_Expired(t *testing.T) {
	priv, pub, _ := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	peerID, _ := peer.IDFromPublicKey(pub)
	var contentID core.ContentID

	rec, _ := NewSignedProviderRecord(contentID, peerID, priv, -time.Minute)
	if err := rec.Verify(); err == nil {
		t.Fatal("expected expired provider record verification to fail, but passed")
	}
}

func TestProviderRecordValidator(t *testing.T) {
	validator := ProviderRecordValidator{}
	priv, pub, _ := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	peerID, _ := peer.IDFromPublicKey(pub)
	var contentID core.ContentID

	rec, _ := NewSignedProviderRecord(contentID, peerID, priv, time.Hour)
	data, _ := json.Marshal(rec)

	key := ProviderRecordKey(contentID, peerID)

	if err := validator.Validate(key, data); err != nil {
		t.Fatalf("validator failed for valid record: %v", err)
	}

	// Invalid json
	if err := validator.Validate(key, []byte("invalid json")); err == nil {
		t.Fatal("validator should fail for invalid json")
	}
}
