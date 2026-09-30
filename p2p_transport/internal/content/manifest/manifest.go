package manifest

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"cipher/internal/content/core"
)

type ContentType string

const (
	TypeFile ContentType = "file"
)

const (
	// MaxManifestJSONSize limits manifest payload size to prevent resource exhaustion (256 KiB).
	MaxManifestJSONSize = 256 * 1024
	// MaxSupportedChunkCount prevents unbounded chunk allocation (1 << 32).
	MaxSupportedChunkCount uint64 = 1 << 32
)

var (
	ErrEmptyManifest      = errors.New("empty manifest data")
	ErrManifestTooLarge   = errors.New("manifest data exceeds maximum size limit")
	ErrMerkleRootMismatch = errors.New("merkle root mismatch: manifest chunk IDs tampered or invalid")
	ErrWholeHashMismatch  = errors.New("whole hash mismatch: manifest chunk IDs tampered or invalid")
	ErrInvalidChunkCount  = errors.New("invalid or excessive chunk count in manifest")
)

type ContentDescriptor struct {
	ID   core.ContentID `json:"id"`
	Type ContentType    `json:"type"`
	Size uint64         `json:"size"`
}

type CryptoDescriptor struct {
	Algorithm      string `json:"algorithm"`
	Version        uint16 `json:"version"`
	ChunkNonceSize uint16 `json:"chunk_nonce_size"`
	KeyID          string `json:"key_id"` // Reference to the key
}

// Manifest represents the capability to understand the immutable content.
type Manifest struct {
	Version    uint16            `json:"version"`
	Descriptor ContentDescriptor `json:"descriptor"`
	ChunkIDs   []core.ChunkID    `json:"chunk_ids"`
	MerkleRoot core.Hash         `json:"merkle_root"` // Recalculated and verified upon deserialization
	WholeHash  core.Hash         `json:"whole_hash"`
	Crypto     CryptoDescriptor  `json:"crypto"`
}

// UserMetadata represents mutable metadata not essential to the content's integrity.
type UserMetadata struct {
	Filename  string `json:"filename"`
	MimeType  string `json:"mime_type"`
	CreatedAt int64  `json:"created_at"`
}

// CalculateMerkleRoot recalculates the root cryptographic digest tree over ChunkIDs.
func (m *Manifest) CalculateMerkleRoot() core.Hash {
	var idConcat []byte
	for _, id := range m.ChunkIDs {
		idConcat = append(idConcat, id[:]...)
	}
	h := sha256.Sum256(idConcat)
	var root core.Hash
	copy(root[:], h[:])
	return root
}

// CalculateWholeHash calculates the digest over all concatenated chunk IDs.
func (m *Manifest) CalculateWholeHash() core.Hash {
	return m.CalculateMerkleRoot()
}

// CanonicalBytes returns canonical JSON serialization with Descriptor.ID zeroed for ContentID calculation.
func (m *Manifest) CanonicalBytes() ([]byte, error) {
	clone := *m
	clone.Descriptor.ID = core.ContentID{}
	return json.Marshal(clone)
}

// ComputeContentID derives content identifier deterministically as SHA-256 over canonical manifest bytes.
func (m *Manifest) ComputeContentID() (core.ContentID, error) {
	cb, err := m.CanonicalBytes()
	if err != nil {
		return core.ContentID{}, err
	}
	h := sha256.Sum256(cb)
	var cid core.ContentID
	copy(cid[:], h[:])
	return cid, nil
}

func (m *Manifest) Serialize() ([]byte, error) {
	return json.Marshal(m)
}

// Deserialize parses, enforces memory bounds, and recalculates root cryptographic digest trees to verify manifest integrity.
func Deserialize(data []byte) (*Manifest, error) {
	if len(data) == 0 {
		return nil, ErrEmptyManifest
	}
	if len(data) > MaxManifestJSONSize {
		return nil, ErrManifestTooLarge
	}

	var m Manifest
	dec := json.NewDecoder(io.LimitReader(bytes.NewReader(data), int64(MaxManifestJSONSize)))
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("failed to decode manifest JSON: %w", err)
	}

	if uint64(len(m.ChunkIDs)) > MaxSupportedChunkCount {
		return nil, ErrInvalidChunkCount
	}

	// Recalculate root cryptographic digest tree (Merkle root)
	expectedMerkleRoot := m.CalculateMerkleRoot()

	// Verify MerkleRoot if present
	if m.MerkleRoot != (core.Hash{}) {
		if subtle.ConstantTimeCompare(m.MerkleRoot[:], expectedMerkleRoot[:]) != 1 {
			return nil, ErrMerkleRootMismatch
		}
	} else {
		m.MerkleRoot = expectedMerkleRoot
	}

	// Verify WholeHash if present
	if m.WholeHash != (core.Hash{}) {
		if subtle.ConstantTimeCompare(m.WholeHash[:], expectedMerkleRoot[:]) != 1 {
			return nil, ErrWholeHashMismatch
		}
	} else {
		m.WholeHash = expectedMerkleRoot
	}

	return &m, nil
}
