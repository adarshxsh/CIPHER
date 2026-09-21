package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	// DefaultMaxSessionFiles defines the maximum number of session files processed by List.
	DefaultMaxSessionFiles = 100
	// DefaultMaxSessionFileSize defines the maximum payload size (in bytes) read per session file.
	DefaultMaxSessionFileSize int64 = 1 * 1024 * 1024 // 1 MB
)

// ErrSessionFileTooLarge indicates that a session file exceeded the maximum configured payload size.
var ErrSessionFileTooLarge = errors.New("session file exceeds maximum allowed size")

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

// FileSessionOption configures FileSessionManager behavior.
type FileSessionOption func(*FileSessionManager)

// WithMaxSessionFiles returns a FileSessionOption that sets the maximum file count during List operations.
func WithMaxSessionFiles(max int) FileSessionOption {
	return func(m *FileSessionManager) {
		m.MaxSessionFiles = max
	}
}

// WithMaxSessionFileSize returns a FileSessionOption that sets the maximum payload size when reading session files.
func WithMaxSessionFileSize(size int64) FileSessionOption {
	return func(m *FileSessionManager) {
		m.MaxSessionFileSize = size
	}
}

// FileSessionManager implements SessionManager by writing JSON to disk.
type FileSessionManager struct {
	dir                string
	MaxSessionFiles    int
	MaxSessionFileSize int64
}

func NewFileSessionManager(dir string, opts ...FileSessionOption) (*FileSessionManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	m := &FileSessionManager{
		dir:                dir,
		MaxSessionFiles:    DefaultMaxSessionFiles,
		MaxSessionFileSize: DefaultMaxSessionFileSize,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m, nil
}

func (m *FileSessionManager) effectiveMaxFiles() int {
	if m.MaxSessionFiles > 0 {
		return m.MaxSessionFiles
	}
	return DefaultMaxSessionFiles
}

func (m *FileSessionManager) effectiveMaxFileSize() int64 {
	if m.MaxSessionFileSize > 0 {
		return m.MaxSessionFileSize
	}
	return DefaultMaxSessionFileSize
}

func (m *FileSessionManager) getPath(id core.ContentID) string {
	return filepath.Join(m.dir, fmt.Sprintf("%x.json", id))
}

func (m *FileSessionManager) Open(id core.ContentID) (*TransferSession, error) {
	path := m.getPath(id)
	return m.readSessionFile(path, m.effectiveMaxFileSize())
}

func (m *FileSessionManager) readSessionFile(path string, maxSize int64) (*TransferSession, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No session found
		}
		return nil, err
	}
	defer f.Close()

	if info, err := f.Stat(); err == nil {
		if info.Size() > maxSize {
			return nil, fmt.Errorf("%w: file size %d exceeds limit %d", ErrSessionFileTooLarge, info.Size(), maxSize)
		}
	}

	lr := io.LimitReader(f, maxSize+1)
	b, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > maxSize {
		return nil, fmt.Errorf("%w: payload size exceeds limit %d", ErrSessionFileTooLarge, maxSize)
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

func (m *FileSessionManager) List() ([]*TransferSession, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	maxFiles := m.effectiveMaxFiles()
	maxSize := m.effectiveMaxFileSize()

	var sessions []*TransferSession
	for _, entry := range entries {
		if len(sessions) >= maxFiles {
			break
		}
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		info, err := entry.Info()
		if err == nil && info.Size() > maxSize {
			continue // Skip oversized files safely
		}

		filePath := filepath.Join(m.dir, entry.Name())
		s, err := m.readSessionFile(filePath, maxSize)
		if err != nil {
			continue // Skip unreadable, oversized, or malformed JSON files safely
		}
		if s != nil {
			sessions = append(sessions, s)
		}
	}
	return sessions, nil
}
