package manager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	DefaultMaxSessionFileSize int64 = 1024 * 1024 // 1 MB
	DefaultMaxSessionEntries  int   = 100
)

type SessionOption func(*FileSessionManager)

// WithMaxFileSize sets the maximum size in bytes for session files parsed during List.
func WithMaxFileSize(size int64) SessionOption {
	return func(m *FileSessionManager) {
		if size > 0 {
			m.maxFileSize = size
		}
	}
}

// WithMaxEntries sets the maximum number of recent session entries parsed during List.
func WithMaxEntries(entries int) SessionOption {
	return func(m *FileSessionManager) {
		if entries > 0 {
			m.maxEntries = entries
		}
	}
}

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
	Completed   []bool         `json:"completed"` // true if the chunk at the same index in the manifest is completed
	TotalChunks int            `json:"total_chunks"`
	StartedAt   time.Time      `json:"started_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	Status      SessionStatus  `json:"status"`
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

type SessionManager interface {
	Open(id core.ContentID) (*TransferSession, error)
	Save(session *TransferSession) error
	Close(id core.ContentID) error
	Delete(id core.ContentID) error
	List() ([]*TransferSession, error)
}

// FileSessionManager implements SessionManager by writing JSON to disk.
type FileSessionManager struct {
	dir         string
	maxFileSize int64
	maxEntries  int
}

func NewFileSessionManager(dir string, opts ...SessionOption) (*FileSessionManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	m := &FileSessionManager{
		dir:         dir,
		maxFileSize: DefaultMaxSessionFileSize,
		maxEntries:  DefaultMaxSessionEntries,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	return m, nil
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
	return os.Rename(tmpPath, path)
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
	return nil
}

type sessionFileEntry struct {
	name    string
	modTime time.Time
}

func (m *FileSessionManager) List() ([]*TransferSession, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	maxSize := m.maxFileSize
	if maxSize <= 0 {
		maxSize = DefaultMaxSessionFileSize
	}

	maxEntries := m.maxEntries
	if maxEntries <= 0 {
		maxEntries = DefaultMaxSessionEntries
	}

	var candidates []sessionFileEntry
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.Size() > maxSize {
			continue
		}
		candidates = append(candidates, sessionFileEntry{
			name:    entry.Name(),
			modTime: info.ModTime(),
		})
	}

	// Sort candidates by modification timestamp (most recent first).
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].modTime.Equal(candidates[j].modTime) {
			return candidates[i].name < candidates[j].name
		}
		return candidates[i].modTime.After(candidates[j].modTime)
	})

	if len(candidates) > maxEntries {
		candidates = candidates[:maxEntries]
	}

	var sessions []*TransferSession
	for _, c := range candidates {
		b, err := os.ReadFile(filepath.Join(m.dir, c.name))
		if err != nil {
			continue
		}
		var s TransferSession
		if err := json.Unmarshal(b, &s); err == nil {
			sessions = append(sessions, &s)
		}
	}
	return sessions, nil
}
