package transport

import (
	"crypto/rand"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
)

func TestNewHost(t *testing.T) {
	// Generate a temporary key for the test
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("Failed to generate test key: %v", err)
	}

	// Create the host using port 0 (random)
	h, err := NewHost(priv, 0)
	if err != nil {
		t.Fatalf("Failed to create host: %v", err)
	}
	defer h.Close()

	// Verify the host was created and has a Peer ID
	if h.ID().String() == "" {
		t.Fatal("Expected host to have a valid Peer ID")
	}

	// Ensure the host is listening on at least one address
	addrs := h.Addrs()
	if len(addrs) == 0 {
		t.Fatal("Expected host to have listening addresses")
	}
}
