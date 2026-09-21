package manager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestFileSessionManager_Basic(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	var cid core.ContentID
	copy(cid[:], []byte("12345678901234567890123456789012"))

	peerID, _ := peer.Decode("12D3KooWBuEB2Ud3S3oA3fG4y4H43W34X4y4H43W34X4y4H43W34")

	session := &TransferSession{
		ContentID:   cid,
		TargetPeer:  peerID,
		Completed:   []bool{true, false, true},
		TotalChunks: 3,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := sm.Save(session); err != nil {
		t.Fatalf("failed to save session: %v", err)
	}

	loaded, err := sm.Open(cid)
	if err != nil {
		t.Fatalf("failed to open session: %v", err)
	}
	if loaded == nil {
		t.Fatalf("expected session to be loaded, got nil")
	}
	if loaded.CompletedCount() != 2 {
		t.Errorf("expected completed count 2, got %d", loaded.CompletedCount())
	}
	if loaded.Status != StatusInProgress {
		t.Errorf("expected status IN_PROGRESS, got %s", loaded.Status)
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("failed to list sessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Errorf("expected 1 session, got %d", len(sessions))
	}

	if err := sm.Delete(cid); err != nil {
		t.Fatalf("failed to delete session: %v", err)
	}

	loadedAfterDelete, err := sm.Open(cid)
	if err != nil {
		t.Fatalf("unexpected error after delete: %v", err)
	}
	if loadedAfterDelete != nil {
		t.Errorf("expected nil session after delete, got %v", loadedAfterDelete)
	}
}

func TestFileSessionManager_CappedListAndSorting(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}
	sm.SetMaxListSessions(5) // Cap at 5 sessions

	testPeerID, _ := peer.Decode("12D3KooWBuEB2Ud3S3oA3fG4y4H43W34X4y4H43W34X4y4H43W34")

	// Create 10 session files with distinct mod times
	for i := 1; i <= 10; i++ {
		var cid core.ContentID
		copy(cid[:], fmt.Sprintf("session_id_%02d_padding_32bytes!!", i))
		sess := &TransferSession{
			ContentID:   cid,
			TargetPeer:  testPeerID,
			TotalChunks: i,
			Status:      StatusInProgress,
		}
		if err := sm.Save(sess); err != nil {
			t.Fatalf("failed to save session %d: %v", i, err)
		}
		// Explicitly set modification time
		filePath := sm.getPath(cid)
		modTime := time.Now().Add(time.Duration(i) * time.Hour)
		_ = os.Chtimes(filePath, modTime, modTime)
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("failed to list sessions: %v", err)
	}

	if len(sessions) != 5 {
		t.Fatalf("expected capped count of 5 sessions, got %d", len(sessions))
	}

	// Verify sorting by ModTime descending: session 10 should be first (newest), session 6 fifth
	for idx, s := range sessions {
		expectedChunks := 10 - idx
		if s.TotalChunks != expectedChunks {
			t.Errorf("at index %d: expected TotalChunks=%d, got %d", idx, expectedChunks, s.TotalChunks)
		}
	}
}

func TestFileSessionManager_StreamingLimitAndOversizedFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}
	sm.SetMaxSessionFileSize(1000) // 1000 bytes limit for List

	testPeerID, _ := peer.Decode("12D3KooWBuEB2Ud3S3oA3fG4y4H43W34X4y4H43W34X4y4H43W34")

	// 1. Create a normal small session file
	var cidSmall core.ContentID
	copy(cidSmall[:], []byte("small_session_32_bytes_long_id!"))
	smallSess := &TransferSession{
		ContentID:   cidSmall,
		TargetPeer:  testPeerID,
		TotalChunks: 1,
		Status:      StatusInProgress,
	}
	if err := sm.Save(smallSess); err != nil {
		t.Fatalf("failed to save small session: %v", err)
	}

	// 2. Create an oversized session file manually (>1000 bytes)
	var cidLarge core.ContentID
	copy(cidLarge[:], []byte("large_session_32_bytes_long_id!"))
	largeSess := &TransferSession{
		ContentID:   cidLarge,
		TargetPeer:  testPeerID,
		TotalChunks: 1000,
		Completed:   make([]bool, 1000),
		Status:      StatusInProgress,
	}
	largeData, _ := json.MarshalIndent(largeSess, "", "  ")
	if len(largeData) <= 1000 {
		t.Fatalf("test error: largeData size %d is not > 1000 bytes", len(largeData))
	}
	filePathLarge := sm.getPath(cidLarge)
	if err := os.WriteFile(filePathLarge, largeData, 0644); err != nil {
		t.Fatalf("failed to write oversized session file: %v", err)
	}

	// List() should skip the oversized file and return only the small session
	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("failed to list sessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session listed, got %d", len(sessions))
	}
	if sessions[0].ContentID != cidSmall {
		t.Errorf("expected small session to be returned in List()")
	}

	// Direct Open() on the oversized file MUST still work (not restricted by list file size cap)
	openedLarge, err := sm.Open(cidLarge)
	if err != nil {
		t.Fatalf("direct Open on large session failed: %v", err)
	}
	if openedLarge == nil || openedLarge.TotalChunks != 1000 {
		t.Errorf("expected direct Open to load valid large session file")
	}
}

func TestFileSessionManager_CorruptedFileSkipped(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	testPeerID, _ := peer.Decode("12D3KooWBuEB2Ud3S3oA3fG4y4H43W34X4y4H43W34X4y4H43W34")

	// Valid session
	var cidValid core.ContentID
	copy(cidValid[:], []byte("valid_session_32_bytes_long_id!"))
	validSess := &TransferSession{
		ContentID:   cidValid,
		TargetPeer:  testPeerID,
		TotalChunks: 5,
		Status:      StatusInProgress,
	}
	if err := sm.Save(validSess); err != nil {
		t.Fatalf("failed to save valid session: %v", err)
	}

	// Corrupted session file
	corruptPath := filepath.Join(tempDir, "corrupt_session.json")
	if err := os.WriteFile(corruptPath, []byte("NOT_VALID_JSON{{{"), 0644); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}

	// List should skip the corrupt file gracefully without erroring out
	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed when corrupted file present: %v", err)
	}
	if len(sessions) != 1 {
		t.Errorf("expected 1 valid session, got %d", len(sessions))
	}
	if sessions[0].ContentID != cidValid {
		t.Errorf("expected valid session in List()")
	}
}
