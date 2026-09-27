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

const (
	maxSessionFileSize int64 = 1 * 1024 * 1024 // 1 MB
	maxSessionsList          = 100
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
}

// FileSessionManager implements SessionManager by writing JSON to disk.
type FileSessionManager struct {
	dir string
}

func NewFileSessionManager(dir string) (*FileSessionManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &FileSessionManager{dir: dir}, nil
}

func (m *FileSessionManager) getPath(id core.ContentID) string {
	return filepath.Join(m.dir, fmt.Sprintf("%x.json", id))
}

func (m *FileSessionManager) Open(id core.ContentID) (*TransferSession, error) {
	path := m.getPath(id)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No session found
		}
		return nil, err
	}
	if info.Size() > maxSessionFileSize {
		return nil, fmt.Errorf("session file %s exceeds maximum size of %d bytes", path, maxSessionFileSize)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var s TransferSession
	if err := json.NewDecoder(io.LimitReader(f, maxSessionFileSize)).Decode(&s); err != nil {
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

	var files []sessionFileInfo
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
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
	}

	// Sort files by modification time descending (newest entries first)
	sort.Slice(files, func(i, j int) bool {
		if files[i].modTime.Equal(files[j].modTime) {
			return files[i].path > files[j].path
		}
		return files[i].modTime.After(files[j].modTime)
	})

	var sessions []*TransferSession
	for _, fInfo := range files {
		// Enforce size check before reading file content
		if fInfo.size > maxSessionFileSize || fInfo.size == 0 {
			continue
		}

		f, err := os.Open(fInfo.path)
		if err != nil {
			continue
		}

		var s TransferSession
		// Stream JSON decoding using io.LimitReader
		err = json.NewDecoder(io.LimitReader(f, maxSessionFileSize)).Decode(&s)
		f.Close()

		if err == nil {
			sessions = append(sessions, &s)
			if len(sessions) >= maxSessionsList {
				break
			}
		}
	}

	return sessions, nil
}
