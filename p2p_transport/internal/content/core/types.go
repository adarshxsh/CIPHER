package core

import (
	"context"
	"runtime"
	"sync"
)

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

// KeyHandle represents a protected key memory handle with explicit release hooks.
type KeyHandle interface {
	Bytes() []byte
	Release()
}

type localKeyHandle struct {
	mu  sync.Mutex
	key []byte
}

func NewKeyHandle(key []byte) KeyHandle {
	if key == nil {
		return &localKeyHandle{}
	}
	k := make([]byte, len(key))
	copy(k, key)
	return &localKeyHandle{key: k}
}

func (h *localKeyHandle) Bytes() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.key
}

func (h *localKeyHandle) Release() {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.key != nil {
		Wipe(h.key)
		h.key = nil
	}
}

// Wipe overwrites the provided slice with zeros and prevents compiler dead-store optimization.
func Wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
	runtime.KeepAlive(b)
}

type Encryptor interface {
	EncryptChunk(key KeyHandle, chunk *Chunk) error
	DecryptChunk(key KeyHandle, chunk *Chunk) error
}

type KeyProvider interface {
	GetHandle(ctx context.Context, id ContentID) (KeyHandle, error)
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
