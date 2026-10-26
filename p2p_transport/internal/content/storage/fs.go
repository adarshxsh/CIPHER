package storage

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"cipher/internal/content/core"
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
	headerSize := int64(binary.Size(core.ChunkHeader{}))

	if fileSize < headerSize {
		return nil, fmt.Errorf("chunk file size %d is smaller than header size %d", fileSize, headerSize)
	}

	c := &core.Chunk{}
	if err := binary.Read(f, binary.LittleEndian, &c.Header); err != nil {
		return nil, fmt.Errorf("failed to read chunk header: %w", err)
	}

	var expectedDataSize uint32
	if c.Header.CipherSize > 0 {
		expectedDataSize = c.Header.CipherSize
	} else {
		expectedDataSize = c.Header.PlainSize
	}

	if expectedDataSize > uint32(core.MaxCiphertextSize) {
		return nil, fmt.Errorf("chunk payload size %d exceeds maximum limit %d", expectedDataSize, core.MaxCiphertextSize)
	}

	expectedTotalSize := headerSize + int64(expectedDataSize)
	if fileSize < expectedTotalSize {
		return nil, fmt.Errorf("chunk file size %d is smaller than expected total size %d", fileSize, expectedTotalSize)
	}
	if fileSize > expectedTotalSize {
		return nil, fmt.Errorf("chunk file size %d exceeds expected total size %d", fileSize, expectedTotalSize)
	}

	// Wrap reader with io.LimitReader set to payload size + 1 to detect trailing garbage
	limitReader := io.LimitReader(f, int64(expectedDataSize)+1)
	data := make([]byte, expectedDataSize)
	if _, err := io.ReadFull(limitReader, data); err != nil {
		return nil, fmt.Errorf("failed to read chunk data: %w", err)
	}

	var extra [1]byte
	if n, _ := limitReader.Read(extra[:]); n > 0 {
		return nil, fmt.Errorf("chunk file contains trailing garbage")
	}

	c.Data = data
	return c, nil
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
	if fileSize > int64(core.MaxManifestSize) {
		return nil, fmt.Errorf("manifest file size %d exceeds maximum limit %d", fileSize, core.MaxManifestSize)
	}

	data := make([]byte, fileSize)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, fmt.Errorf("failed to read manifest data: %w", err)
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
