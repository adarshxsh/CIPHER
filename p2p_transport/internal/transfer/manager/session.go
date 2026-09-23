package manager

import (
	"bytes"
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
	DefaultMaxSessionFileSize int64 = 1 * 1024 * 1024 // 1 MB limit
	DefaultPageLimit          int   = 50
	MaxPageLimit              int   = 1000
	MaxListedSessions         int   = 1000
)

// TransferSession tracks the local state of a download.
type TransferSession struct {
	ContentID   core.ContentID `json:"content_id"`
	TargetPeer  peer.ID        `json:"target_peer"`
	Completed   []bool         `json:"completed,omitempty"` // true if the chunk at the same index in the manifest is completed
	TotalChunks int            `json:"total_chunks"`
	StartedAt   time.Time      `json:"started_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	Status      SessionStatus  `json:"status"`

	completedCount int // unexported count set during lightweight JSON summary scanning
}

// CompletedCount returns the number of chunks downloaded.
func (s *TransferSession) CompletedCount() int {
	if len(s.Completed) > 0 {
		count := 0
		for _, c := range s.Completed {
			if c {
				count++
			}
		}
		return count
	}
	return s.completedCount
}

type SessionManager interface {
	Open(id core.ContentID) (*TransferSession, error)
	Save(session *TransferSession) error
	Close(id core.ContentID) error
	Delete(id core.ContentID) error
	List() ([]*TransferSession, error)
	ListPaged(offset, limit int) ([]*TransferSession, error)
}

// FileSessionManager implements SessionManager by writing JSON to disk.
type FileSessionManager struct {
	dir         string
	maxFileSize int64
}

type FileSessionOption func(*FileSessionManager)

func WithMaxFileSize(size int64) FileSessionOption {
	return func(m *FileSessionManager) {
		if size > 0 {
			m.maxFileSize = size
		}
	}
}

func NewFileSessionManager(dir string, opts ...FileSessionOption) (*FileSessionManager, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	_ = os.Chmod(dir, 0700)
	m := &FileSessionManager{
		dir:         dir,
		maxFileSize: DefaultMaxSessionFileSize,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m, nil
}

func (m *FileSessionManager) getPath(id core.ContentID) string {
	return filepath.Join(m.dir, fmt.Sprintf("%x.json", id))
}

func (m *FileSessionManager) Open(id core.ContentID) (*TransferSession, error) {
	path := m.getPath(id)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No session found
		}
		return nil, err
	}
	defer file.Close()

	lr := io.LimitReader(file, m.maxFileSize+1)
	b, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > m.maxFileSize {
		return nil, fmt.Errorf("session file %s exceeds size limit of %d bytes", path, m.maxFileSize)
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

type sessionHeader struct {
	ContentID   core.ContentID `json:"content_id"`
	TargetPeer  peer.ID        `json:"target_peer"`
	TotalChunks int            `json:"total_chunks"`
	StartedAt   time.Time      `json:"started_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	Status      SessionStatus  `json:"status"`
}

func countCompletedChunks(data []byte) int {
	idx := bytes.Index(data, []byte(`"completed"`))
	if idx == -1 {
		return 0
	}
	bracketStart := bytes.IndexByte(data[idx:], '[')
	if bracketStart == -1 {
		return 0
	}
	start := idx + bracketStart

	bracketEnd := bytes.IndexByte(data[start:], ']')
	if bracketEnd == -1 {
		return 0
	}
	end := start + bracketEnd

	count := 0
	for i := start; i < end; i++ {
		if data[i] == 't' {
			count++
		}
	}
	return count
}

func (m *FileSessionManager) readSessionSummaryFile(path string) (*TransferSession, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	lr := io.LimitReader(file, m.maxFileSize+1)
	data, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > m.maxFileSize {
		return nil, fmt.Errorf("session file %s exceeds max size limit of %d bytes", path, m.maxFileSize)
	}

	var hdr sessionHeader
	if err := json.Unmarshal(data, &hdr); err != nil {
		return nil, err
	}

	return &TransferSession{
		ContentID:      hdr.ContentID,
		TargetPeer:     hdr.TargetPeer,
		TotalChunks:    hdr.TotalChunks,
		StartedAt:      hdr.StartedAt,
		UpdatedAt:      hdr.UpdatedAt,
		Status:         hdr.Status,
		completedCount: countCompletedChunks(data),
	}, nil
}

func (m *FileSessionManager) List() ([]*TransferSession, error) {
	return m.ListPaged(0, MaxListedSessions)
}

func (m *FileSessionManager) ListPaged(offset, limit int) ([]*TransferSession, error) {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = DefaultPageLimit
	} else if limit > MaxPageLimit {
		limit = MaxPageLimit
	}

	f, err := os.Open(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	type fileInfo struct {
		name    string
		modTime time.Time
	}
	var files []fileInfo

	// Stream directory entries in bounded batches of 64
	for {
		entries, err := f.ReadDir(64)
		if err != nil && err != io.EOF {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				continue
			}
			// Enforce per-file size limit prior to reading content
			if info.Size() > m.maxFileSize {
				continue
			}
			files = append(files, fileInfo{
				name:    entry.Name(),
				modTime: info.ModTime(),
			})
		}
		if err == io.EOF || len(entries) == 0 {
			break
		}
	}

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
	if end > len(files) {
		end = len(files)
	}

	var sessions []*TransferSession
	for _, fi := range files[offset:end] {
		path := filepath.Join(m.dir, fi.name)
		sess, err := m.readSessionSummaryFile(path)
		if err != nil {
			continue
		}
		sessions = append(sessions, sess)
	}

	return sessions, nil
}
