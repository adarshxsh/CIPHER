package storage

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"cipher/internal/content/core"
)

const (
	// MaxChunkResponseSize is the upper bound on chunk payload size (2 MiB frame minus 3 bytes frame header).
	MaxChunkResponseSize = 2097149

	// MaxCiphertextSize is the maximum payload size for a standard encrypted chunk.
	MaxCiphertextSize = 32796

	// MaxManifestSize is the maximum size allowed for manifest payloads.
	MaxManifestSize = 2097149
)

var (
	ErrInvalidChunkSize   = errors.New("invalid chunk payload size")
	ErrChunkTooLarge      = errors.New("chunk payload size exceeds limit")
	ErrHeaderSizeMismatch = errors.New("chunk header payload size mismatch")
	ErrManifestTooLarge   = errors.New("manifest size exceeds protocol limit")
)

// FSStorage implements core.ChunkSource and core.ChunkSink using local filesystem.
type FSStorage struct {
	baseDir string
}

func NewFSStorage(baseDir string) error {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return err
	}
	return nil
}

func NewFSStore(baseDir string) *FSStorage {
	return &FSStorage{baseDir: baseDir}
}

func (s *FSStorage) pathForChunk(id core.ChunkID) string {
	encoded := hex.EncodeToString(id[:])
	if len(encoded) >= 4 {
		// Shard by first 2 bytes (4 hex chars), e.g. ab/cd/abcdef...
		return filepath.Join(s.baseDir, encoded[0:2], encoded[2:4], encoded)
	}
	return filepath.Join(s.baseDir, encoded)
}

func (s *FSStorage) HasChunk(ctx context.Context, id core.ChunkID) (bool, error) {
	_, err := os.Stat(s.pathForChunk(id))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (s *FSStorage) PutChunk(ctx context.Context, chunk *core.Chunk) error {
	path := s.pathForChunk(chunk.Header.ID)

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create shard dir: %w", err)
	}

	tmpPath := fmt.Sprintf("%s.%d.tmp", path, os.Getpid())
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	// Serialize Header
	if err := binary.Write(f, binary.LittleEndian, &chunk.Header); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write chunk header: %w", err)
	}

	// Write Data
	if _, err := f.Write(chunk.Data); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write chunk data: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to fsync chunk: %w", err)
	}
	f.Close()

	return os.Rename(tmpPath, path)
}

func (s *FSStorage) GetChunk(ctx context.Context, id core.ChunkID) (*core.Chunk, error) {
	path := s.pathForChunk(id)

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat chunk file: %w", err)
	}
	fileSize := info.Size()

	chunk := &core.Chunk{}
	headerSize := int64(binary.Size(&chunk.Header))

	if fileSize < headerSize {
		return nil, fmt.Errorf("chunk file size %d smaller than header size %d: %w", fileSize, headerSize, ErrInvalidChunkSize)
	}

	payloadSize := fileSize - headerSize

	// Pre-allocation validation against protocol bounds
	if payloadSize > MaxChunkResponseSize {
		return nil, fmt.Errorf("chunk payload size %d exceeds MaxChunkResponseSize (%d): %w", payloadSize, MaxChunkResponseSize, ErrChunkTooLarge)
	}

	// Read Header
	if err := binary.Read(f, binary.LittleEndian, &chunk.Header); err != nil {
		return nil, fmt.Errorf("failed to read chunk header: %w", err)
	}

	// Validate payload size against chunk header metadata (CipherSize/PlainSize)
	var expectedPayloadSize int64
	if chunk.Header.CipherSize > 0 {
		expectedPayloadSize = int64(chunk.Header.CipherSize)
	} else if chunk.Header.PlainSize > 0 {
		expectedPayloadSize = int64(chunk.Header.PlainSize)
	}

	if expectedPayloadSize > 0 && payloadSize != expectedPayloadSize {
		return nil, fmt.Errorf("payload size %d does not match header metadata size %d: %w", payloadSize, expectedPayloadSize, ErrHeaderSizeMismatch)
	}
	if expectedPayloadSize == 0 && payloadSize > 0 {
		return nil, fmt.Errorf("payload size is %d but header metadata specifies size 0: %w", payloadSize, ErrHeaderSizeMismatch)
	}

	// Exact single byte slice allocation
	data := make([]byte, payloadSize)
	limitReader := io.LimitReader(f, payloadSize)
	if _, err := io.ReadFull(limitReader, data); err != nil {
		return nil, fmt.Errorf("failed to read chunk data: %w", err)
	}

	chunk.Data = data

	return chunk, nil
}

func (s *FSStorage) manifestPath(id core.ContentID) string {
	encoded := hex.EncodeToString(id[:])
	return filepath.Join(s.baseDir, "manifests", encoded+".json")
}

func (s *FSStorage) GetManifestBytes(ctx context.Context, id core.ContentID) ([]byte, error) {
	path := s.manifestPath(id)

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("manifest not found: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat manifest file: %w", err)
	}

	fileSize := info.Size()
	if fileSize > MaxManifestSize {
		return nil, fmt.Errorf("manifest size %d exceeds MaxManifestSize (%d): %w", fileSize, MaxManifestSize, ErrManifestTooLarge)
	}

	data := make([]byte, fileSize)
	limitReader := io.LimitReader(f, MaxManifestSize)
	if _, err := io.ReadFull(limitReader, data); err != nil {
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}

	return data, nil
}

func (s *FSStorage) PutManifestBytes(ctx context.Context, id core.ContentID, data []byte) error {
	path := s.manifestPath(id)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create manifest dir: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write manifest: %w", err)
	}
	return nil
}

func (s *FSStorage) ListManifests(ctx context.Context) ([]core.ContentID, error) {
	manifestDir := filepath.Join(s.baseDir, "manifests")
	var ids []core.ContentID

	entries, err := os.ReadDir(manifestDir)
	if err != nil {
		if os.IsNotExist(err) {
			return ids, nil
		}
		return nil, fmt.Errorf("failed to read manifest dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		name := entry.Name()
		encoded := name[:len(name)-5] // Strip .json
		
		idBytes, err := hex.DecodeString(encoded)
		if err != nil || len(idBytes) != 32 {
			continue
		}

		var id core.ContentID
		copy(id[:], idBytes)
		ids = append(ids, id)
	}

	return ids, nil
}
