package discovery

import (
	"crypto/sha256"

	"cipher/internal/content/core"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// ContentIDToCID converts a 32-byte ContentID digest into a CIDv1 with SHA2-256 multihash.
func ContentIDToCID(id core.ContentID) (cid.Cid, error) {
	mh, err := multihash.Encode(id[:], multihash.SHA2_256)

	if err != nil {
		return cid.Undef, err
	}

	return cid.NewCidV1(cid.Raw, mh), nil
}

func contentIDToCID(id core.ContentID) (cid.Cid, error) {
	return ContentIDToCID(id)
}

// CIDFromManifestBytes computes the SHA-256 digest of canonical manifest bytes and returns its CIDv1 multihash representation.
func CIDFromManifestBytes(data []byte) (cid.Cid, error) {
	hash := sha256.Sum256(data)
	return ContentIDToCID(hash)
}
