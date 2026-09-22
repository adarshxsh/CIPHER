package manager

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func testPeerID(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}
	pid, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("failed to create peer ID: %v", err)
	}
	return pid
}

func TestFileSessionManager_CRUD(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	var cid core.ContentID
	cid[0] = 1
	cid[1] = 2

	// Open non-existent
	s, err := sm.Open(cid)
	if err != nil {
		t.Fatalf("unexpected error on non-existent session: %v", err)
	}
	if s != nil {
		t.Fatalf("expected nil session, got %v", s)
	}

	// Save
	pid := testPeerID(t)
	sess := &TransferSession{
		ContentID:   cid,
		TargetPeer:  pid,
		Completed:   []bool{true, false, true},
		TotalChunks: 3,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}
	if err := sm.Save(sess); err != nil {
		t.Fatalf("failed to save session: %v", err)
	}

	// Open existing
	loaded, err := sm.Open(cid)
	if err != nil {
		t.Fatalf("failed to open session: %v", err)
	}
	if loaded == nil {
		t.Fatalf("expected non-nil session")
	}
	if loaded.ContentID != cid {
		t.Fatalf("content id mismatch: got %v, want %v", loaded.ContentID, cid)
	}
	if loaded.CompletedCount() != 2 {
		t.Fatalf("completed count mismatch: got %d, want 2", loaded.CompletedCount())
	}

	// List
	list, err := sm.List()
	if err != nil {
		t.Fatalf("failed to list sessions: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 session in list, got %d", len(list))
	}

	// Close
	if err := sm.Close(cid); err != nil {
		t.Fatalf("failed to close session: %v", err)
	}

	// Delete
	if err := sm.Delete(cid); err != nil {
		t.Fatalf("failed to delete session: %v", err)
	}

	listAfterDel, err := sm.List()
	if err != nil {
		t.Fatalf("failed to list after delete: %v", err)
	}
	if len(listAfterDel) != 0 {
		t.Fatalf("expected 0 sessions after delete, got %d", len(listAfterDel))
	}
}

func TestFileSessionManager_LimitReaderAndCorruptFileHandling(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_limit_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	var cidValid, cidOversized, cidCorrupt core.ContentID
	cidValid[0] = 0x01
	cidOversized[0] = 0x02
	cidCorrupt[0] = 0x03

	pid := testPeerID(t)

	// 1. Save valid session (< 64KB)
	validSess := &TransferSession{
		ContentID:   cidValid,
		TargetPeer:  pid,
		Completed:   []bool{true},
		TotalChunks: 1,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}
	if err := sm.Save(validSess); err != nil {
		t.Fatalf("failed to save valid session: %v", err)
	}

	// 2. Create oversized session file (> 64KB)
	oversizedPath := sm.getPath(cidOversized)
	// Create JSON-like data padded to 70KB
	oversizedData := bytes.Repeat([]byte("a"), 70*1024)
	if err := os.WriteFile(oversizedPath, oversizedData, 0644); err != nil {
		t.Fatalf("failed to write oversized file: %v", err)
	}

	// 3. Create corrupt session file
	corruptPath := sm.getPath(cidCorrupt)
	corruptData := []byte("{ invalid json content ...")
	if err := os.WriteFile(corruptPath, corruptData, 0644); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}

	// Test List(): should skip oversized and corrupt files gracefully
	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 valid session, got %d", len(sessions))
	}
	if sessions[0].ContentID != cidValid {
		t.Fatalf("expected valid session content ID, got %v", sessions[0].ContentID)
	}

	// Test Open() on oversized file directly
	_, err = sm.Open(cidOversized)
	if err == nil {
		t.Fatalf("expected error when opening oversized session file, got nil")
	}

	// Test Open() on corrupt file directly
	_, err = sm.Open(cidCorrupt)
	if err == nil {
		t.Fatalf("expected error when opening corrupt session file, got nil")
	}
}

func TestFileSessionManager_MaxSessionLimit(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_max_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	pid := testPeerID(t)

	// Create 600 session files
	totalFiles := 600
	for i := 0; i < totalFiles; i++ {
		var cid core.ContentID
		cid[0] = byte(i >> 8)
		cid[1] = byte(i)
		cid[2] = byte(i * 3)

		sess := &TransferSession{
			ContentID:   cid,
			TargetPeer:  pid,
			Completed:   []bool{true},
			TotalChunks: 1,
			StartedAt:   time.Now(),
			Status:      StatusInProgress,
		}
		if err := sm.Save(sess); err != nil {
			t.Fatalf("failed to save session %d: %v", i, err)
		}
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List() failed: %v", err)
	}

	if len(sessions) != MaxListedSessions {
		t.Fatalf("expected List() to return %d sessions, got %d", MaxListedSessions, len(sessions))
	}
}

func TestFileSessionManager_MemoryAllocationUnderScale(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_mem_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	pidStr := testPeerID(t).String()

	// Create 10,000 session files on disk
	for i := 0; i < 10000; i++ {
		fileName := fmt.Sprintf("%08x.json", i)
		path := filepath.Join(tempDir, fileName)
		data := []byte(fmt.Sprintf(`{"content_id":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,%d,%d],"target_peer":%q,"completed":[true],"total_chunks":1,"status":"COMPLETED"}`, i>>8, i&0xff, pidStr))
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatalf("failed to write test file %d: %v", i, err)
		}
	}

	runtime.GC()
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List() failed: %v", err)
	}

	runtime.ReadMemStats(&m2)

	if len(sessions) != MaxListedSessions {
		t.Fatalf("expected %d sessions, got %d", MaxListedSessions, len(sessions))
	}

	// Verify heap allocation delta during status query operation stays well under 5 MB
	heapAllocDelta := int64(m2.TotalAlloc) - int64(m1.TotalAlloc)
	t.Logf("Heap total allocation delta during List(): %d bytes (%.2f MB)", heapAllocDelta, float64(heapAllocDelta)/(1024*1024))

	maxAllowedAlloc := int64(5 * 1024 * 1024) // 5 MB limit
	if heapAllocDelta > maxAllowedAlloc {
		t.Fatalf("Heap allocation delta %d bytes exceeded max limit of %d bytes", heapAllocDelta, maxAllowedAlloc)
	}
}
