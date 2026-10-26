package manager

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	DefaultPageLimit = 50
	MaxPageLimit     = 1000
)

type SessionStatus string

const (
	StatusInProgress SessionStatus = "IN_PROGRESS"
	StatusPaused     SessionStatus = "PAUSED"
	StatusCompleted  SessionStatus = "COMPLETED"
	StatusFailed     SessionStatus = "FAILED"
)

// TransferSession tracks the local state of a download.
type TransferSession struct {
	ContentID   core.ContentID `json:"content_id"`
	TargetPeer  peer.ID        `json:"target_peer"`
	TotalChunks int            `json:"total_chunks"`
	StartedAt   time.Time      `json:"started_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	Status      SessionStatus  `json:"status"`
	Completed   []bool         `json:"completed"` // true if the chunk at the same index in the manifest is completed
}

// CompletedCount returns the number of chunks downloaded.
func (s *TransferSession) CompletedCount() int {
	count := 0
	for _, c := range s.Completed {
		if c {
			count++
		}
	}
	return count
}

// SessionHeader represents lightweight session metadata without chunk completion arrays.
type SessionHeader struct {
	ContentID       core.ContentID `json:"content_id"`
	TargetPeer      peer.ID        `json:"target_peer"`
	TotalChunks     int            `json:"total_chunks"`
	CompletedChunks int            `json:"completed_chunks"`
	StartedAt       time.Time      `json:"started_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
	Status          SessionStatus  `json:"status"`
}

// CompletedCount returns the number of completed chunks.
func (s *SessionHeader) CompletedCount() int {
	return s.CompletedChunks
}

type SessionManager interface {
	Open(id core.ContentID) (*TransferSession, error)
	Save(session *TransferSession) error
	Close(id core.ContentID) error
	Delete(id core.ContentID) error
	List(offset, limit int) ([]*SessionHeader, error)
}

// FileSessionManager implements SessionManager by writing JSON to disk.
type FileSessionManager struct {
	dir          string
	mu           sync.RWMutex
	modTimeCache map[string]time.Time
}

func NewFileSessionManager(dir string) (*FileSessionManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &FileSessionManager{
		dir:          dir,
		modTimeCache: make(map[string]time.Time),
	}, nil
}

func (m *FileSessionManager) getPath(id core.ContentID) string {
	return filepath.Join(m.dir, fmt.Sprintf("%x.json", id))
}

func (m *FileSessionManager) Open(id core.ContentID) (*TransferSession, error) {
	path := m.getPath(id)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No session found
		}
		return nil, err
	}
	var s TransferSession
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (m *FileSessionManager) Save(session *TransferSession) error {
	session.UpdatedAt = time.Now()
	b, err := json.MarshalIndent(session, "", "  ")
	if err != nil {
		return err
	}
	path := m.getPath(session.ContentID)
	// Write to temporary file and rename for atomicity
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	m.mu.Lock()
	m.modTimeCache[filepath.Base(path)] = session.UpdatedAt
	m.mu.Unlock()
	return nil
}

func (m *FileSessionManager) Close(id core.ContentID) error {
	// For file-backed, close is a no-op as state is always saved atomically
	return nil
}

func (m *FileSessionManager) Delete(id core.ContentID) error {
	path := m.getPath(id)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	m.mu.Lock()
	delete(m.modTimeCache, filepath.Base(path))
	m.mu.Unlock()
	return nil
}

var bufioReaderPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewReaderSize(nil, 4096)
	},
}

// DecodeSessionHeader reads a session stream and extracts metadata and completed chunk count
// without allocating chunk completion arrays.
func DecodeSessionHeader(r io.Reader) (*SessionHeader, error) {
	br := bufioReaderPool.Get().(*bufio.Reader)
	br.Reset(r)
	defer func() {
		br.Reset(nil)
		bufioReaderPool.Put(br)
	}()

	dec := json.NewDecoder(br)

	var header SessionHeader

	t, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := t.(json.Delim)
	if !ok || delim != '{' {
		return nil, fmt.Errorf("expected '{', got %v", t)
	}

	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			break
		}
		key, ok := t.(string)
		if !ok {
			break
		}

		switch key {
		case "content_id":
			if err := dec.Decode(&header.ContentID); err != nil {
				return nil, err
			}
		case "target_peer":
			if err := dec.Decode(&header.TargetPeer); err != nil {
				return nil, err
			}
		case "status":
			if err := dec.Decode(&header.Status); err != nil {
				return nil, err
			}
		case "total_chunks":
			if err := dec.Decode(&header.TotalChunks); err != nil {
				return nil, err
			}
		case "started_at":
			if err := dec.Decode(&header.StartedAt); err != nil {
				return nil, err
			}
		case "updated_at":
			if err := dec.Decode(&header.UpdatedAt); err != nil {
				return nil, err
			}
		case "completed":
			t, err := dec.Token()
			if err != nil {
				return nil, err
			}
			if delim, ok := t.(json.Delim); !ok || delim != '[' {
				return nil, fmt.Errorf("expected '[', got %v", t)
			}
			for dec.More() {
				tok, err := dec.Token()
				if err != nil {
					break
				}
				if b, ok := tok.(bool); ok && b {
					header.CompletedChunks++
				}
			}
			// Read closing ']'
			_, _ = dec.Token()
		default:
			var raw json.RawMessage
			_ = dec.Decode(&raw)
		}
	}

	return &header, nil
}

type fileModTime struct {
	name    string
	modTime time.Time
}

func (m *FileSessionManager) List(offset, limit int) ([]*SessionHeader, error) {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = DefaultPageLimit
	}
	if limit > MaxPageLimit {
		limit = MaxPageLimit
	}

	entries, err := os.ReadDir(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var files []fileModTime
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			name := entry.Name()
			m.mu.RLock()
			cachedTime, exists := m.modTimeCache[name]
			m.mu.RUnlock()

			if exists {
				files = append(files, fileModTime{name: name, modTime: cachedTime})
			} else {
				info, err := entry.Info()
				if err != nil {
					continue
				}
				mt := info.ModTime()
				m.mu.Lock()
				m.modTimeCache[name] = mt
				m.mu.Unlock()
				files = append(files, fileModTime{name: name, modTime: mt})
			}
		}
	}

	// Sort by modification time descending (most recent first)
	sort.Slice(files, func(i, j int) bool {
		if files[i].modTime.Equal(files[j].modTime) {
			return files[i].name < files[j].name
		}
		return files[i].modTime.After(files[j].modTime)
	})

	if offset >= len(files) {
		return []*SessionHeader{}, nil
	}

	end := offset + limit
	if end > len(files) {
		end = len(files)
	}

	pageFiles := files[offset:end]
	headers := make([]*SessionHeader, 0, len(pageFiles))

	for _, f := range pageFiles {
		filePath := filepath.Join(m.dir, f.name)
		file, err := os.Open(filePath)
		if err != nil {
			continue
		}
		header, err := DecodeSessionHeader(file)
		_ = file.Close()
		if err == nil && header != nil {
			headers = append(headers, header)
		}
	}

	return headers, nil
}
