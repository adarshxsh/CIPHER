package core

import (
	"context"
	"runtime"
)

// Zeroize overwrites the provided byte slice with zero bytes to clear key material from memory.
func Zeroize(b []byte) {
	for i := range b {
		b[i] = 0
	}
	runtime.KeepAlive(b)
}

// KeyWrapper provides a zeroizing wrapper for sensitive key byte slices.
type KeyWrapper struct {
	key []byte
}

func NewKeyWrapper(key []byte) *KeyWrapper {
	return &KeyWrapper{key: key}
}

func (w *KeyWrapper) Bytes() []byte {
	if w == nil {
		return nil
	}
	return w.key
}

func (w *KeyWrapper) Release() {
	if w != nil && w.key != nil {
		Zeroize(w.key)
		w.key = nil
	}
}

func (w *KeyWrapper) Delete() {
	w.Release()
}

func (w *KeyWrapper) Zeroize() {
	w.Release()
}

// KeyHandle is an alias to KeyWrapper for convenience.
type KeyHandle = KeyWrapper

func NewKeyHandle(key []byte) *KeyHandle {
	return NewKeyWrapper(key)
}

type ChunkID [32]byte
type ContentID [32]byte
type Hash [32]byte

type ChunkHeader struct {
	Version    uint16
	ID         ChunkID
	Index      uint32
	Offset     int64
	PlainSize  uint32
	CipherSize uint32
	Nonce      [12]byte
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

type ManifestStore interface {
	GetManifestBytes(ctx context.Context, id ContentID) ([]byte, error)
	PutManifestBytes(ctx context.Context, id ContentID, data []byte) error
	ListManifests(ctx context.Context) ([]ContentID, error)
}

type EngineConfig struct {
	ChunkSize uint32
}
