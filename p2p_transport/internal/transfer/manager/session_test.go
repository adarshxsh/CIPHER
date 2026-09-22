package manager

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestFileSessionManager_Permissions(t *testing.T) {
	tempDir := t.TempDir()
	sessionDir := filepath.Join(tempDir, "sessions")

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	// Check directory permissions (0700)
	dirInfo, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("Stat sessionDir failed: %v", err)
	}
	if perm := dirInfo.Mode().Perm(); perm != 0700 {
		t.Errorf("Expected directory permissions 0700, got %o", perm)
	}

	cid := core.ContentID{1, 2, 3, 4, 5}
	session := &TransferSession{
		ContentID:   cid,
		TotalChunks: 10,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := sm.Save(session); err != nil {
		t.Fatalf("Save session failed: %v", err)
	}

	filePath := sm.getPath(cid)
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Stat session file failed: %v", err)
	}
	if perm := fileInfo.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected file permissions 0600, got %o", perm)
	}
}

func TestFileSessionManager_LegacyPermissionsMigration(t *testing.T) {
	tempDir := t.TempDir()
	sessionDir := filepath.Join(tempDir, "legacy_sessions")

	// Create directory with insecure permissions (0755)
	if err := os.MkdirAll(sessionDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	_ = os.Chmod(sessionDir, 0755)

	subDir := filepath.Join(sessionDir, "subdir")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatalf("MkdirAll subdir failed: %v", err)
	}
	_ = os.Chmod(subDir, 0755)

	legacyFile := filepath.Join(sessionDir, "legacy.json")
	if err := os.WriteFile(legacyFile, []byte("{}"), 0644); err != nil {
		t.Fatalf("WriteFile legacyFile failed: %v", err)
	}
	_ = os.Chmod(legacyFile, 0644)

	// Instantiate manager on existing dir
	_, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	// Verify main directory
	info, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("Expected main dir permissions 0700, got %o", perm)
	}

	// Verify subdirectory
	info, err = os.Stat(subDir)
	if err != nil {
		t.Fatalf("Stat subdir failed: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("Expected subdir permissions 0700, got %o", perm)
	}

	// Verify legacy file
	info, err = os.Stat(legacyFile)
	if err != nil {
		t.Fatalf("Stat legacyFile failed: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected legacy file permissions 0600, got %o", perm)
	}
}

func TestFileSessionManager_ParameterValidation(t *testing.T) {
	_, err := NewFileSessionManager("")
	if err == nil {
		t.Errorf("Expected error when creating NewFileSessionManager with empty path")
	}

	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	if err := sm.Save(nil); err == nil {
		t.Errorf("Expected error when saving nil session")
	}
}

func TestFileSessionManager_CRUD(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	cid := core.ContentID{10, 20, 30}
	s, err := sm.Open(cid)
	if err != nil {
		t.Fatalf("Open non-existent session returned error: %v", err)
	}
	if s != nil {
		t.Errorf("Expected nil session for non-existent id")
	}

	peerID, err := peer.Decode("QmY5A3LMNKGxu6FWga4B8rPkUaBf3Z8sE9PCfGHPcmWTai")
	if err != nil {
		t.Fatalf("peer.Decode failed: %v", err)
	}

	session := &TransferSession{
		ContentID:   cid,
		TargetPeer:  peerID,
		Completed:   []bool{true, false, true},
		TotalChunks: 3,
		Status:      StatusInProgress,
	}

	if err := sm.Save(session); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	s, err = sm.Open(cid)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if s == nil {
		t.Fatalf("Expected non-nil session")
	}
	if s.CompletedCount() != 2 {
		t.Errorf("Expected CompletedCount 2, got %d", s.CompletedCount())
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("Expected 1 session in List, got %d", len(sessions))
	}

	if err := sm.Delete(cid); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	s, err = sm.Open(cid)
	if err != nil {
		t.Fatalf("Open after delete returned error: %v", err)
	}
	if s != nil {
		t.Errorf("Expected nil session after delete")
	}
}

func TestFileSessionManager_TempFileCleanup(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	cid := core.ContentID{1, 1, 1}
	sessionPath := sm.getPath(cid)
	tmpPath := sessionPath + ".tmp"

	// Create a directory at sessionPath so os.Rename(tmpPath, sessionPath) will fail
	if err := os.MkdirAll(sessionPath, 0700); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	session := &TransferSession{
		ContentID:   cid,
		TotalChunks: 1,
		Status:      StatusInProgress,
	}

	// Save should fail on rename because destination is a directory
	if err := sm.Save(session); err == nil {
		t.Errorf("Expected Save to fail when destination is a directory")
	}

	// Verify tmp file was cleaned up and does not exist
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("Expected temp file %s to be cleaned up after failure, but it exists", tmpPath)
	}
}
