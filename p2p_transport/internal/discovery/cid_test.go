package discovery

import (
	"crypto/sha256"
	"testing"

	"cipher/internal/content/core"
)

func TestContentIDToCID(t *testing.T) {
	var id core.ContentID

	for i := range id {
		id[i] = byte(i)
	}

	c, err := ContentIDToCID(id)
	if err != nil {
		t.Fatalf("ContentIDToCID failed: %v", err)
	}

	if !c.Defined() {
		t.Fatal("expected defined CID")
	}

	if c.Version() != 1 {
		t.Fatalf("expected CIDv1, got CIDv%d", c.Version())
	}
}

func TestCIDFromManifestBytes(t *testing.T) {
	manifestBytes := []byte(`{"version":1,"descriptor":{"type":"file","size":100}}`)
	expectedHash := sha256.Sum256(manifestBytes)

	c1, err := CIDFromManifestBytes(manifestBytes)
	if err != nil {
		t.Fatalf("CIDFromManifestBytes failed: %v", err)
	}

	c2, err := ContentIDToCID(expectedHash)
	if err != nil {
		t.Fatalf("ContentIDToCID failed: %v", err)
	}

	if c1.String() != c2.String() {
		t.Fatalf("CID mismatch: %s != %s", c1.String(), c2.String())
	}
}
