package manager

import (
	"encoding/hex"
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

type SessionStatus string

const (
	StatusInProgress SessionStatus = "IN_PROGRESS"
	StatusPaused     SessionStatus = "PAUSED"
	StatusCompleted  SessionStatus = "COMPLETED"
	StatusFailed     SessionStatus = "FAILED"
)

const (
	DefaultMaxSessionFileSize int64 = 1 * 1024 * 1024 // 1 MB per session file
	DefaultMaxListedSessions  int   = 100             // 100 recent sessions cap
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

// SessionSummary represents a lightweight metadata overview of a transfer session.
type SessionSummary struct {
	ContentID       core.ContentID `json:"content_id"`
	TargetPeer      peer.ID        `json:"target_peer"`
	TotalChunks     int            `json:"total_chunks"`
	CompletedChunks int            `json:"completed_chunks"`
	StartedAt       time.Time      `json:"started_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
	Status          SessionStatus  `json:"status"`
}

// CompletedCount returns the number of completed chunks in the summary.
func (s *SessionSummary) CompletedCount() int {
	return s.CompletedChunks
}

type SessionManager interface {
	Open(id core.ContentID) (*TransferSession, error)
	Save(session *TransferSession) error
	Close(id core.ContentID) error
	Delete(id core.ContentID) error
	List() ([]*SessionSummary, error)
}

// FileSessionManager implements SessionManager by writing JSON to disk.
type FileSessionManager struct {
	dir         string
	maxFileSize int64
	maxFiles    int
}

func NewFileSessionManager(dir string) (*FileSessionManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &FileSessionManager{
		dir:         dir,
		maxFileSize: DefaultMaxSessionFileSize,
		maxFiles:    DefaultMaxListedSessions,
	}, nil
}

func (m *FileSessionManager) SetMaxFileSize(size int64) {
	if size > 0 {
		m.maxFileSize = size
	}
}

func (m *FileSessionManager) SetMaxFiles(limit int) {
	if limit > 0 {
		m.maxFiles = limit
	}
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

	if info, err := file.Stat(); err == nil && m.maxFileSize > 0 && info.Size() > m.maxFileSize {
		return nil, fmt.Errorf("session file %s exceeds maximum size limit (%d > %d bytes)", path, info.Size(), m.maxFileSize)
	}

	maxSize := m.maxFileSize
	if maxSize <= 0 {
		maxSize = DefaultMaxSessionFileSize
	}

	lr := io.LimitReader(file, maxSize+1)
	b, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > maxSize {
		return nil, fmt.Errorf("session file %s exceeds maximum size limit (%d bytes)", path, maxSize)
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

type sessionFileCandidate struct {
	path    string
	modTime time.Time
}

func (m *FileSessionManager) List() ([]*SessionSummary, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var candidates []sessionFileCandidate
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			info, err := entry.Info()
			if err != nil {
				log.Printf("[FileSessionManager] Skipping file %s: failed to stat: %v", entry.Name(), err)
				continue
			}
			candidates = append(candidates, sessionFileCandidate{
				path:    filepath.Join(m.dir, entry.Name()),
				modTime: info.ModTime(),
			})
		}
	}

	// Sort by modification time descending (newest session files first)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].modTime.Equal(candidates[j].modTime) {
			return candidates[i].path < candidates[j].path
		}
		return candidates[i].modTime.After(candidates[j].modTime)
	})

	maxFiles := m.maxFiles
	if maxFiles > 0 && len(candidates) > maxFiles {
		candidates = candidates[:maxFiles]
	}

	maxSize := m.maxFileSize
	if maxSize <= 0 {
		maxSize = DefaultMaxSessionFileSize
	}

	var summaries []*SessionSummary
	for _, candidate := range candidates {
		f, err := os.Open(candidate.path)
		if err != nil {
			log.Printf("[FileSessionManager] Skipping session file %s: open failed: %v", candidate.path, err)
			continue
		}

		info, err := f.Stat()
		if err != nil {
			f.Close()
			log.Printf("[FileSessionManager] Skipping session file %s: stat failed: %v", candidate.path, err)
			continue
		}

		if info.Size() > maxSize {
			f.Close()
			log.Printf("[FileSessionManager] Skipping oversized session file %s (%d bytes exceeds limit %d bytes)", candidate.path, info.Size(), maxSize)
			continue
		}

		lr := io.LimitReader(f, maxSize+1)
		summary, err := parseSessionSummary(lr)
		f.Close()
		if err != nil {
			log.Printf("[FileSessionManager] Skipping corrupted session file %s: %v", candidate.path, err)
			continue
		}

		summaries = append(summaries, summary)
	}

	return summaries, nil
}

func parseSessionSummary(r io.Reader) (*SessionSummary, error) {
	dec := json.NewDecoder(r)

	// Read opening '{'
	t, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := t.(json.Delim)
	if !ok || delim != '{' {
		return nil, fmt.Errorf("expected start of JSON object")
	}

	summary := &SessionSummary{}

	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := t.(string)
		if !ok {
			return nil, fmt.Errorf("expected string key in JSON object")
		}

		switch key {
		case "content_id":
			var tok json.RawMessage
			if err := dec.Decode(&tok); err != nil {
				return nil, err
			}
			if err := json.Unmarshal(tok, &summary.ContentID); err != nil {
				var str string
				if errStr := json.Unmarshal(tok, &str); errStr == nil {
					if b, errHex := hex.DecodeString(str); errHex == nil && len(b) == 32 {
						copy(summary.ContentID[:], b)
					}
				}
			}
		case "target_peer":
			var peerStr string
			if err := dec.Decode(&peerStr); err == nil {
				if peerStr != "" {
					pid, err := peer.Decode(peerStr)
					if err == nil {
						summary.TargetPeer = pid
					} else {
						summary.TargetPeer = peer.ID(peerStr)
					}
				}
			}
		case "total_chunks":
			if err := dec.Decode(&summary.TotalChunks); err != nil {
				return nil, err
			}
		case "started_at":
			if err := dec.Decode(&summary.StartedAt); err != nil {
				return nil, err
			}
		case "updated_at":
			if err := dec.Decode(&summary.UpdatedAt); err != nil {
				return nil, err
			}
		case "status":
			if err := dec.Decode(&summary.Status); err != nil {
				return nil, err
			}
		case "completed":
			t, err := dec.Token()
			if err != nil {
				return nil, err
			}
			if t == nil {
				continue
			}
			arrDelim, ok := t.(json.Delim)
			if !ok || arrDelim != '[' {
				return nil, fmt.Errorf("expected start of completed array")
			}
			completedCount := 0
			for dec.More() {
				tok, err := dec.Token()
				if err != nil {
					return nil, err
				}
				if b, ok := tok.(bool); ok && b {
					completedCount++
				}
			}
			// Read the closing ']'
			t, err = dec.Token()
			if err != nil {
				return nil, err
			}
			arrEnd, ok := t.(json.Delim)
			if !ok || arrEnd != ']' {
				return nil, fmt.Errorf("expected end of completed array")
			}
			summary.CompletedChunks = completedCount
		default:
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return nil, err
			}
		}
	}

	// Read closing '}'
	t, err = dec.Token()
	if err != nil {
		return nil, err
	}
	endDelim, ok := t.(json.Delim)
	if !ok || endDelim != '}' {
		return nil, fmt.Errorf("expected end of JSON object")
	}

	return summary, nil
}

