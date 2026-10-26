package engine

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"io"

	"cipher/internal/content/chunker"
	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

type ContentEngine struct {
	config    core.EngineConfig
	chunker   *chunker.Chunker
	encryptor core.Encryptor
	digest    core.Digest
	source    core.ChunkSource
	sink      core.ChunkSink
	manifestStore core.ManifestStore
	keys      core.KeyProvider
}

func NewContentEngine(
	config core.EngineConfig,
	encryptor core.Encryptor,
	digest core.Digest,
	source core.ChunkSource,
	sink core.ChunkSink,
	keys core.KeyProvider,
	manifestStore core.ManifestStore,
) *ContentEngine {
	return &ContentEngine{
		config:        config,
		chunker:       chunker.NewChunker(config),
		encryptor:     encryptor,
		digest:        digest,
		source:        source,
		sink:          sink,
		manifestStore: manifestStore,
		keys:          keys,
	}
}

// Ingest reads a file, chunks it, encrypts it, stores it, and returns the manifest.
func (e *ContentEngine) Ingest(ctx context.Context, r io.Reader, mtype manifest.ContentType) (*manifest.Manifest, error) {
	chunkCh, errCh := e.chunker.Split(r)

	// Generate a unique ContentID for this upload
	var contentID core.ContentID
	if _, err := rand.Read(contentID[:]); err != nil {
		return nil, fmt.Errorf("failed to generate content id: %w", err)
	}

	// Generate a new encryption key
	key := make([]byte, 32) // ChaCha20-Poly1305 takes a 32-byte key
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}

	// Store key
	if err := e.keys.Put(ctx, contentID, key); err != nil {
		return nil, fmt.Errorf("failed to store key: %w", err)
	}

	var chunkIDs []core.ChunkID
	var totalSize uint64

	// Read all chunks, encrypt, hash, and store
	for chunk := range chunkCh {
		// Encrypt the chunk
		if err := e.encryptor.EncryptChunk(key, chunk); err != nil {
			return nil, fmt.Errorf("failed to encrypt chunk: %w", err)
		}

		// Hash the ciphertext to get the ChunkID (content-addressing)
		chunkHash := e.digest.Sum(chunk.Data)
		var chunkID core.ChunkID
		copy(chunkID[:], chunkHash[:])
		chunk.Header.ID = chunkID

		// Store the chunk
		if err := e.sink.PutChunk(ctx, chunk); err != nil {
			return nil, fmt.Errorf("failed to store chunk: %w", err)
		}

		chunkIDs = append(chunkIDs, chunkID)
		totalSize += uint64(chunk.Header.PlainSize)
	}

	if err := <-errCh; err != nil {
		return nil, fmt.Errorf("error reading stream: %w", err)
	}

	// For Milestone 7, MerkleRoot is just a hash of the concatenated chunk IDs
	// In the future this will be an actual Merkle Tree root.
	var idConcat []byte
	for _, id := range chunkIDs {
		idConcat = append(idConcat, id[:]...)
	}
	wholeHash := e.digest.Sum(idConcat)

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   contentID,
			Type: mtype,
			Size: totalSize,
		},
		ChunkIDs:   chunkIDs,
		MerkleRoot: wholeHash,
		WholeHash:  wholeHash,
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}

	return m, nil
}

// Reassemble reads the manifest, fetches chunks sequentially, decrypts them, verifies integrity,
// and writes chunk bytes directly to w using a 64KB fixed-size streaming buffer without buffering in RAM.
// Upon completion, it flushes and syncs the destination file handle.
func (e *ContentEngine) Reassemble(ctx context.Context, m *manifest.Manifest, w io.Writer) error {
	// Retrieve key
	key, err := e.keys.Get(ctx, m.Descriptor.ID)
	if err != nil {
		return fmt.Errorf("failed to get content key: %w", err)
	}

	// Requirement 2: Fixed-size streaming buffer (64KB)
	const streamingBufferSize = 64 * 1024
	bw := bufio.NewWriterSize(w, streamingBufferSize)

	for _, chunkID := range m.ChunkIDs {
		// Requirement 1: Stream chunk bytes directly to destination
		chunk, err := e.source.GetChunk(ctx, chunkID)
		if err != nil {
			return fmt.Errorf("failed to get chunk %x: %w", chunkID, err)
		}

		// Verify chunk hash matches ID
		hash := e.digest.Sum(chunk.Data)
		if hash != core.Hash(chunkID) {
			return fmt.Errorf("corrupted chunk %x: hash mismatch", chunkID)
		}

		// Decrypt chunk in place
		if err := e.encryptor.DecryptChunk(key, chunk); err != nil {
			return fmt.Errorf("failed to decrypt chunk %x: %w", chunkID, err)
		}

		// Requirement 1: Write chunk bytes directly to destination buffer/handle
		if _, err := bw.Write(chunk.Data); err != nil {
			return fmt.Errorf("failed to write decrypted chunk: %w", err)
		}

		// Release memory immediately to ensure flat O(1) memory overhead
		chunk.Data = nil
	}

	// Requirement 3: System must flush and sync file handles upon completion
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	type syncer interface {
		Sync() error
	}
	if s, ok := w.(syncer); ok {
		if err := s.Sync(); err != nil {
			return fmt.Errorf("failed to sync file: %w", err)
		}
	}

	return nil
}

// ReassembleChunkAt decrypts a chunk and writes it directly to wAt at chunk.Header.Offset.
// This supports concurrent chunk receipt and out-of-order writes via sparse file offsets.
func (e *ContentEngine) ReassembleChunkAt(ctx context.Context, contentID core.ContentID, chunk *core.Chunk, wAt io.WriterAt) error {
	key, err := e.keys.Get(ctx, contentID)
	if err != nil {
		return fmt.Errorf("failed to get content key: %w", err)
	}

	// Verify chunk hash matches ID
	hash := e.digest.Sum(chunk.Data)
	if hash != core.Hash(chunk.Header.ID) {
		return fmt.Errorf("corrupted chunk %x: hash mismatch", chunk.Header.ID)
	}

	// Decrypt
	if err := e.encryptor.DecryptChunk(key, chunk); err != nil {
		return fmt.Errorf("failed to decrypt chunk %x: %w", chunk.Header.ID, err)
	}

	// Sparse write out-of-order chunk directly at destination file offset
	if _, err := wAt.WriteAt(chunk.Data, chunk.Header.Offset); err != nil {
		return fmt.Errorf("failed to write chunk %x at offset %d: %w", chunk.Header.ID, chunk.Header.Offset, err)
	}

	// Clear chunk memory immediately
	chunk.Data = nil
	return nil
}

// -- Transport Layer APIs --

// HasChunk returns whether the chunk exists in the underlying store.
func (e *ContentEngine) HasChunk(ctx context.Context, id core.ChunkID) (bool, error) {
	return e.source.HasChunk(ctx, id)
}

// GetChunk reads a chunk from the underlying local store.
func (e *ContentEngine) GetChunk(ctx context.Context, id core.ChunkID) (*core.Chunk, error) {
	return e.source.GetChunk(ctx, id)
}

func (e *ContentEngine) PutChunk(ctx context.Context, chunk *core.Chunk) error {
	return e.sink.PutChunk(ctx, chunk)
}

func (e *ContentEngine) GetManifestBytes(ctx context.Context, id core.ContentID) ([]byte, error) {
	return e.manifestStore.GetManifestBytes(ctx, id)
}

func (e *ContentEngine) PutManifestBytes(ctx context.Context, id core.ContentID, data []byte) error {
	return e.manifestStore.PutManifestBytes(ctx, id, data)
}
