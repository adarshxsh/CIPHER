package manager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return nil, err
	}
	return &FileSessionManager{dir: dir}, nil
}

func (m *FileSessionManager) getPath(id core.ContentID) string {
	return filepath.Join(m.dir, fmt.Sprintf("%x.json", id))
}

func (m *FileSessionManager) Open(id core.ContentID) (*TransferSession, error) {
	path := m.getPath(id)
	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No session found
		}
		return nil, err
	}
	if fi.Mode().Perm() != 0600 {
		if err := os.Chmod(path, 0600); err != nil {
			return nil, err
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
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
	if err := os.WriteFile(tmpPath, b, 0600); err != nil {
		return err
	}
	if err := os.Chmod(tmpPath, 0600); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Chmod(path, 0600); err != nil {
		return err
	}
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
	return nil
}

func (m *FileSessionManager) List() ([]*TransferSession, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var sessions []*TransferSession
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			filePath := filepath.Join(m.dir, entry.Name())
			if info, err := entry.Info(); err == nil {
				if info.Mode().Perm() != 0600 {
					_ = os.Chmod(filePath, 0600)
				}
			} else if fi, err := os.Stat(filePath); err == nil && fi.Mode().Perm() != 0600 {
				_ = os.Chmod(filePath, 0600)
			}
			b, err := os.ReadFile(filePath)
			if err != nil {
				continue
			}
			var s TransferSession
			if err := json.Unmarshal(b, &s); err == nil {
				sessions = append(sessions, &s)
			}
		}
	}
	return sessions, nil
}
