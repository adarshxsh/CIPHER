package core

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

type ChunkID [32]byte

func (c ChunkID) MarshalJSON() ([]byte, error) {
	return []byte(`"` + hex.EncodeToString(c[:]) + `"`), nil
}

func (c *ChunkID) UnmarshalJSON(data []byte) error {
	return unmarshalHexOrArray(data, c[:])
}

type ContentID [32]byte

func (c ContentID) MarshalJSON() ([]byte, error) {
	return []byte(`"` + hex.EncodeToString(c[:]) + `"`), nil
}

func (c *ContentID) UnmarshalJSON(data []byte) error {
	return unmarshalHexOrArray(data, c[:])
}

type Hash [32]byte

func (h Hash) MarshalJSON() ([]byte, error) {
	return []byte(`"` + hex.EncodeToString(h[:]) + `"`), nil
}

func (h *Hash) UnmarshalJSON(data []byte) error {
	return unmarshalHexOrArray(data, h[:])
}

func unmarshalHexOrArray(data []byte, b []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty JSON data")
	}
	if data[0] == '"' {
		// String format, expect hex
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		dec, err := hex.DecodeString(s)
		if err != nil {
			return err
		}
		if len(dec) != 32 {
			return fmt.Errorf("expected 32 bytes for hex string, got %d", len(dec))
		}
		copy(b, dec)
		return nil
	} else if data[0] == '[' {
		// Array format, expect 32 bytes
		var arr []uint8
		if err := json.Unmarshal(data, &arr); err != nil {
			return err
		}
		if len(arr) != 32 {
			return fmt.Errorf("expected 32 bytes for array, got %d", len(arr))
		}
		copy(b, arr)
		return nil
	}
	return fmt.Errorf("unexpected JSON format for 32-byte identifier")
}

type ChunkHeader struct {
	Version    uint16
	ID         ChunkID
	Index      uint32
	Offset     int64
	PlainSize  uint32
	CipherSize uint32
	Nonce      [24]byte
}

type Chunk struct {
	Header ChunkHeader
	Data   []byte // Represents current payload (plaintext or ciphertext)
}

type ChunkSource interface {
	HasChunk(ctx context.Context, id ChunkID) (bool, error)
	GetChunk(ctx context.Context, id ChunkID) (*Chunk, error)
}

type ChunkSink interface {
	PutChunk(ctx context.Context, chunk *Chunk) error
}

type Digest interface {
	Sum(data []byte) Hash
	Verify(data []byte, hash Hash) bool
	Algorithm() string
}

type Encryptor interface {
	EncryptChunk(key []byte, chunk *Chunk) error
	DecryptChunk(key []byte, chunk *Chunk) error
}

type KeyProvider interface {
	Get(ctx context.Context, id ContentID) ([]byte, error)
	Put(ctx context.Context, id ContentID, key []byte) error
	Delete(ctx context.Context, id ContentID) error
}

type Scheduler interface{}

type EngineConfig struct {
	ChunkSize uint32
}
