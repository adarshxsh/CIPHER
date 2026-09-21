package manager

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/peer"
)

type SessionStatus string

const (
	StatusInProgress SessionStatus = "IN_PROGRESS"
	StatusPaused     SessionStatus = "PAUSED"
	StatusCompleted  SessionStatus = "COMPLETED"
	StatusFailed     SessionStatus = "FAILED"
)

const (
	DefaultMaxListSessions    = 100
	DefaultMaxSessionFileSize = 10 * 1024 * 1024 // 10 MB limit for listing
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
	dir                string
	maxListSessions    int
	maxSessionFileSize int64
}

func NewFileSessionManager(dir string) (*FileSessionManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &FileSessionManager{
		dir:                dir,
		maxListSessions:    DefaultMaxListSessions,
		maxSessionFileSize: DefaultMaxSessionFileSize,
	}, nil
}

func (m *FileSessionManager) SetMaxListSessions(n int) {
	if n > 0 {
		m.maxListSessions = n
	}
}

func (m *FileSessionManager) SetMaxSessionFileSize(size int64) {
	if size > 0 {
		m.maxSessionFileSize = size
	}
}

func (m *FileSessionManager) getPath(id core.ContentID) string {
	return filepath.Join(m.dir, fmt.Sprintf("%x.json", id))
}

func (m *FileSessionManager) Open(id core.ContentID) (*TransferSession, error) {
	path := m.getPath(id)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No session found
		}
		return nil, err
	}
	defer f.Close()

	var s TransferSession
	if err := json.NewDecoder(f).Decode(&s); err != nil {
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

type sessionFileInfo struct {
	path    string
	modTime time.Time
	size    int64
}

func (m *FileSessionManager) List() ([]*TransferSession, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	maxList := m.maxListSessions
	if maxList <= 0 {
		maxList = DefaultMaxListSessions
	}
	maxFileSize := m.maxSessionFileSize
	if maxFileSize <= 0 {
		maxFileSize = DefaultMaxSessionFileSize
	}

	var files []sessionFileInfo
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		files = append(files, sessionFileInfo{
			path:    filepath.Join(m.dir, entry.Name()),
			modTime: info.ModTime(),
			size:    info.Size(),
		})
	}

	// Sort files by modification time descending (newest first)
	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime.After(files[j].modTime)
	})

	var sessions []*TransferSession
	for _, file := range files {
		if len(sessions) >= maxList {
			break
		}
		if file.size > maxFileSize {
			continue
		}

		f, err := os.Open(file.path)
		if err != nil {
			continue
		}

		limitedReader := io.LimitReader(f, maxFileSize)
		var s TransferSession
		err = json.NewDecoder(limitedReader).Decode(&s)
		f.Close()

		if err == nil {
			sessions = append(sessions, &s)
		}
	}

	return sessions, nil
}
