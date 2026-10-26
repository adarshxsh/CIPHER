package manager

import (
	"encoding/json"
	"fmt"
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
	ListSessions(offset, limit int) ([]*TransferSession, error)
	ListSessionsPaginated(offset, limit int) ([]*TransferSession, error)
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

func (m *FileSessionManager) List() ([]*TransferSession, error) {
	return m.ListSessionsPaginated(0, DefaultPageLimit)
}

func (m *FileSessionManager) ListSessions(offset, limit int) ([]*TransferSession, error) {
	return m.ListSessionsPaginated(offset, limit)
}

type fileModTime struct {
	name    string
	modTime time.Time
}

func (m *FileSessionManager) ListSessionsPaginated(offset, limit int) ([]*TransferSession, error) {
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
		return []*TransferSession{}, nil
	}

	end := offset + limit
	if end > len(files) || end < 0 {
		end = len(files)
	}

	pageFiles := files[offset:end]
	sessions := make([]*TransferSession, 0, len(pageFiles))

	for _, f := range pageFiles {
		b, err := os.ReadFile(filepath.Join(m.dir, f.name))
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

