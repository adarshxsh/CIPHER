package manager

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
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

// SessionSummary represents lightweight session metadata without chunk completion arrays.
type SessionSummary struct {
	ContentID       core.ContentID `json:"content_id"`
	TargetPeer      peer.ID        `json:"target_peer"`
	TotalChunks     int            `json:"total_chunks"`
	CompletedChunks int            `json:"completed_chunks"`
	StartedAt       time.Time      `json:"started_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
	Status          SessionStatus  `json:"status"`
}

// CompletedCount returns the number of completed chunks.
func (s *SessionSummary) CompletedCount() int {
	return s.CompletedChunks
}

type SessionManager interface {
	Open(id core.ContentID) (*TransferSession, error)
	Save(session *TransferSession) error
	Close(id core.ContentID) error
	Delete(id core.ContentID) error
	List(offset, limit int) ([]*SessionSummary, error)
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

const MaxSessionFileSize int64 = 64 * 1024 // 64 KB cap per session file

var bufioReaderPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewReaderSize(nil, 4096)
	},
}

// DecodeSessionSummary reads a session stream with a limited reader guard (64KB cap)
// and extracts metadata and completed chunk count without allocating completion arrays.
func DecodeSessionSummary(r io.Reader) (*SessionSummary, error) {
	limitedReader := io.LimitReader(r, MaxSessionFileSize)

	br := bufioReaderPool.Get().(*bufio.Reader)
	br.Reset(limitedReader)
	defer func() {
		br.Reset(nil)
		bufioReaderPool.Put(br)
	}()

	dec := json.NewDecoder(br)

	var summary SessionSummary

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
			_ = dec.Decode(&summary.ContentID)
		case "target_peer":
			_ = dec.Decode(&summary.TargetPeer)
		case "status":
			_ = dec.Decode(&summary.Status)
		case "total_chunks":
			_ = dec.Decode(&summary.TotalChunks)
		case "started_at":
			_ = dec.Decode(&summary.StartedAt)
		case "updated_at":
			_ = dec.Decode(&summary.UpdatedAt)
		case "completed":
			// Fast stream scan for completed booleans without token allocations
			combined := io.MultiReader(dec.Buffered(), br)
			scanBr := bufioReaderPool.Get().(*bufio.Reader)
			scanBr.Reset(combined)

			// Find opening '['
			for {
				b, err := scanBr.ReadByte()
				if err != nil {
					break
				}
				if b == '[' {
					break
				}
			}

			// Count 't' bytes until ']' or EOF
			for {
				b, err := scanBr.ReadByte()
				if err != nil {
					break
				}
				if b == ']' {
					break
				}
				if b == 't' {
					summary.CompletedChunks++
				}
			}

			dec = json.NewDecoder(scanBr)
			defer func() {
				scanBr.Reset(nil)
				bufioReaderPool.Put(scanBr)
			}()
		default:
			var raw json.RawMessage
			_ = dec.Decode(&raw)
		}
	}

	return &summary, nil
}

func (m *FileSessionManager) List(offset, limit int) ([]*SessionSummary, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var jsonFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			jsonFiles = append(jsonFiles, entry.Name())
		}
	}

	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 100
	}
	if offset >= len(jsonFiles) {
		return []*SessionSummary{}, nil
	}

	end := offset + limit
	if end > len(jsonFiles) {
		end = len(jsonFiles)
	}

	var summaries []*SessionSummary
	for _, filename := range jsonFiles[offset:end] {
		filePath := filepath.Join(m.dir, filename)
		f, err := os.Open(filePath)
		if err != nil {
			continue
		}
		summary, err := DecodeSessionSummary(f)
		_ = f.Close()
		if err == nil && summary != nil {
			summaries = append(summaries, summary)
		}
	}

	return summaries, nil
}
