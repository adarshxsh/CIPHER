package manifest

import (
	"bytes"
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
	// MaxManifestSize enforces a hard byte limit during manifest deserialization (512 KiB).
	MaxManifestSize int64 = 512 * 1024

	// MaxSupportedChunkCount caps the maximum chunk count allowed in a manifest.
	MaxSupportedChunkCount uint64 = 1 << 32
)

var (
	ErrOversizedManifest = errors.New("manifest payload exceeds maximum byte size limit")
	ErrTooManyChunks     = errors.New("manifest chunk count exceeds maximum supported limit")
	ErrNilReader         = errors.New("reader is nil")
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
	MerkleRoot core.Hash         `json:"merkle_root"` // Set to WholeHash for Milestone 7
	WholeHash  core.Hash         `json:"whole_hash"`
	Crypto     CryptoDescriptor  `json:"crypto"`
}

// UserMetadata represents mutable metadata not essential to the content's integrity.
type UserMetadata struct {
	Filename  string `json:"filename"`
	MimeType  string `json:"mime_type"`
	CreatedAt int64  `json:"created_at"`
}

func (m *Manifest) Serialize() ([]byte, error) {
	return json.Marshal(m)
}

func (m *Manifest) Validate() error {
	if m == nil {
		return errors.New("manifest is nil")
	}
	if uint64(len(m.ChunkIDs)) > MaxSupportedChunkCount {
		return fmt.Errorf("%w: chunk count %d exceeds maximum %d", ErrTooManyChunks, len(m.ChunkIDs), MaxSupportedChunkCount)
	}
	return nil
}

type countingReader struct {
	r     io.Reader
	count int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.count += int64(n)
	return n, err
}

func DeserializeReader(r io.Reader) (*Manifest, error) {
	if r == nil {
		return nil, ErrNilReader
	}

	cr := &countingReader{r: r}
	limitReader := io.LimitReader(cr, MaxManifestSize+1)

	dec := json.NewDecoder(limitReader)
	dec.DisallowUnknownFields()

	var m Manifest
	if err := dec.Decode(&m); err != nil {
		if cr.count > MaxManifestSize {
			return nil, fmt.Errorf("%w: read %d bytes (limit %d)", ErrOversizedManifest, cr.count, MaxManifestSize)
		}
		return nil, fmt.Errorf("failed to decode manifest JSON: %w", err)
	}

	if cr.count > MaxManifestSize {
		return nil, fmt.Errorf("%w: read %d bytes (limit %d)", ErrOversizedManifest, cr.count, MaxManifestSize)
	}

	if err := m.Validate(); err != nil {
		return nil, err
	}

	return &m, nil
}

func Deserialize(data []byte) (*Manifest, error) {
	if int64(len(data)) > MaxManifestSize {
		return nil, fmt.Errorf("%w: received %d bytes (limit %d)", ErrOversizedManifest, len(data), MaxManifestSize)
	}
	return DeserializeReader(bytes.NewReader(data))
}

