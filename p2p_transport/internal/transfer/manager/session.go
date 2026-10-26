package manager

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	MaxSessionFileSize int64 = 1 * 1024 * 1024 // 1 MB payload limit per session file
	MaxListedSessions  int   = 500             // Maximum session count processed / returned
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
	if info.Size() > MaxSessionFileSize {
		return nil, fmt.Errorf("session file %s exceeds maximum payload size limit (%d > %d bytes)", path, info.Size(), MaxSessionFileSize)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	b, err := io.ReadAll(io.LimitReader(f, MaxSessionFileSize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > MaxSessionFileSize {
		return nil, fmt.Errorf("session file %s exceeds maximum payload size limit (%d > %d bytes)", path, len(b), MaxSessionFileSize)
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

type sessionCandidate struct {
	entry os.DirEntry
	info  os.FileInfo
}

func (m *FileSessionManager) List() ([]*TransferSession, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var candidates []sessionCandidate
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			candidates = append(candidates, sessionCandidate{
				entry: entry,
				info:  info,
			})
		}
	}

	// Sort candidates by modification timestamp descending (most recent first)
	sort.Slice(candidates, func(i, j int) bool {
		ti := candidates[i].info.ModTime()
		tj := candidates[j].info.ModTime()
		if ti.Equal(tj) {
			return candidates[i].entry.Name() < candidates[j].entry.Name()
		}
		return ti.After(tj)
	})

	if len(candidates) > MaxListedSessions {
		candidates = candidates[:MaxListedSessions]
	}

	var sessions []*TransferSession
	for _, candidate := range candidates {
		if candidate.info.Size() > MaxSessionFileSize {
			log.Printf("Warning: skipping oversized session file %s (size: %d bytes, limit: %d bytes)", candidate.entry.Name(), candidate.info.Size(), MaxSessionFileSize)
			continue
		}

		filePath := filepath.Join(m.dir, candidate.entry.Name())
		f, err := os.Open(filePath)
		if err != nil {
			continue
		}

		b, err := io.ReadAll(io.LimitReader(f, MaxSessionFileSize+1))
		f.Close()
		if err != nil {
			continue
		}
		if int64(len(b)) > MaxSessionFileSize {
			log.Printf("Warning: skipping oversized session file %s (read size: %d bytes, limit: %d bytes)", candidate.entry.Name(), len(b), MaxSessionFileSize)
			continue
		}

		var s TransferSession
		if err := json.Unmarshal(b, &s); err == nil {
			sessions = append(sessions, &s)
		}
	}

	return sessions, nil
}
