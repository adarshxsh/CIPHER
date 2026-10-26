package manager

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func testPeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("failed to get peer ID: %v", err)
	}
	return pid
}

func TestFileSessionManager_List_Basic(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	peerID := testPeerID(t)

	cID1 := core.ContentID{1}
	cID2 := core.ContentID{2}

	s1 := &TransferSession{
		ContentID:   cID1,
		TargetPeer:  peerID,
		Completed:   []bool{true, false},
		TotalChunks: 2,
		Status:      StatusInProgress,
	}
	s2 := &TransferSession{
		ContentID:   cID2,
		TargetPeer:  peerID,
		Completed:   []bool{true, true},
		TotalChunks: 2,
		Status:      StatusCompleted,
	}

	if err := sm.Save(s1); err != nil {
		t.Fatalf("failed to save s1: %v", err)
	}
	if err := sm.Save(s2); err != nil {
		t.Fatalf("failed to save s2: %v", err)
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}

	if len(sessions) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(sessions))
	}
}

func TestFileSessionManager_List_OversizedFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_oversized_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	peerID := testPeerID(t)
	cID1 := core.ContentID{10}
	s1 := &TransferSession{
		ContentID:   cID1,
		TargetPeer:  peerID,
		Completed:   []bool{true},
		TotalChunks: 1,
		Status:      StatusInProgress,
	}
	if err := sm.Save(s1); err != nil {
		t.Fatalf("failed to save s1: %v", err)
	}

	// Create an oversized .json file > 1MB
	oversizedPath := filepath.Join(tempDir, "oversized.json")
	largeData := bytes.Repeat([]byte("a"), int(MaxSessionFileSize)+1024)
	if err := os.WriteFile(oversizedPath, largeData, 0644); err != nil {
		t.Fatalf("failed to write oversized file: %v", err)
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}

	if len(sessions) != 1 {
		t.Fatalf("expected 1 session (oversized file skipped), got %d", len(sessions))
	}
	if sessions[0].ContentID != cID1 {
		t.Fatalf("expected contentID %v, got %v", cID1, sessions[0].ContentID)
	}

	// Test Open on oversized file
	var oversizedID core.ContentID
	copy(oversizedID[:], []byte("oversized"))
	cIDPath := sm.getPath(oversizedID)
	if err := os.WriteFile(cIDPath, largeData, 0644); err != nil {
		t.Fatalf("failed to write oversized session path: %v", err)
	}

	_, err = sm.Open(oversizedID)
	if err == nil {
		t.Fatalf("expected error when opening oversized session file, got nil")
	}
}

func TestFileSessionManager_List_InvalidJSON(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_invalid_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	peerID := testPeerID(t)
	cID1 := core.ContentID{20}
	s1 := &TransferSession{
		ContentID:   cID1,
		TargetPeer:  peerID,
		Completed:   []bool{true},
		TotalChunks: 1,
		Status:      StatusInProgress,
	}
	if err := sm.Save(s1); err != nil {
		t.Fatalf("failed to save s1: %v", err)
	}

	// Create corrupt JSON file
	invalidPath := filepath.Join(tempDir, "corrupt.json")
	if err := os.WriteFile(invalidPath, []byte("{corrupt json payload..."), 0644); err != nil {
		t.Fatalf("failed to write invalid json file: %v", err)
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}

	if len(sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(sessions))
	}
}

func TestFileSessionManager_List_MaxCountCap(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_cap_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	peerID := testPeerID(t)
	totalFiles := 600
	for i := 0; i < totalFiles; i++ {
		var cID core.ContentID
		copy(cID[:], []byte(fmt.Sprintf("id-%04d", i)))
		s := &TransferSession{
			ContentID:   cID,
			TargetPeer:  peerID,
			Completed:   []bool{true},
			TotalChunks: 1,
			Status:      StatusInProgress,
		}
		if err := sm.Save(s); err != nil {
			t.Fatalf("failed to save session %d: %v", i, err)
		}
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}

	if len(sessions) != MaxListedSessions {
		t.Fatalf("expected capped session count of %d, got %d", MaxListedSessions, len(sessions))
	}
}

func TestFileSessionManager_List_TimestampSorting(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_sort_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	peerID := testPeerID(t)
	baseTime := time.Now().Add(-1 * time.Hour)
	count := 5

	for i := 0; i < count; i++ {
		var cID core.ContentID
		copy(cID[:], []byte(fmt.Sprintf("sort-id-%d", i)))
		s := &TransferSession{
			ContentID:   cID,
			TargetPeer:  peerID,
			Completed:   []bool{true},
			TotalChunks: 1,
			Status:      StatusInProgress,
		}
		if err := sm.Save(s); err != nil {
			t.Fatalf("failed to save session %d: %v", i, err)
		}

		filePath := sm.getPath(cID)
		modTime := baseTime.Add(time.Duration(i*10) * time.Minute)
		if err := os.Chtimes(filePath, modTime, modTime); err != nil {
			t.Fatalf("failed to set chtimes: %v", err)
		}
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}

	if len(sessions) != count {
		t.Fatalf("expected %d sessions, got %d", count, len(sessions))
	}

	// Verify order descending by timestamp (newest first)
	for i := 0; i < count; i++ {
		expectedID := fmt.Sprintf("sort-id-%d", count-1-i)
		var expCID core.ContentID
		copy(expCID[:], []byte(expectedID))
		if sessions[i].ContentID != expCID {
			t.Errorf("at index %d: expected ContentID %s, got %s", i, expectedID, string(sessions[i].ContentID[:]))
		}
	}
}

func TestFileSessionManager_List_Performance(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_perf_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	peerID := testPeerID(t)
	// Create 1500 dummy session files
	dummyCount := 1500
	for i := 0; i < dummyCount; i++ {
		var cID core.ContentID
		copy(cID[:], []byte(fmt.Sprintf("perf-id-%05d", i)))
		s := &TransferSession{
			ContentID:   cID,
			TargetPeer:  peerID,
			Completed:   []bool{true},
			TotalChunks: 1,
			Status:      StatusInProgress,
		}
		if err := sm.Save(s); err != nil {
			t.Fatalf("failed to save session: %v", err)
		}
	}

	start := time.Now()
	sessions, err := sm.List()
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}

	if len(sessions) != MaxListedSessions {
		t.Fatalf("expected %d sessions, got %d", MaxListedSessions, len(sessions))
	}

	if elapsed > 100*time.Millisecond {
		t.Logf("Warning: List took %v for %d files", elapsed, dummyCount)
	}
}
