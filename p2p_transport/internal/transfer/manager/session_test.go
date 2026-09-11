package manager

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestNewFileSessionManager_Permissions(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sessionDir := filepath.Join(tempDir, "sessions")
	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}
	if sm == nil {
		t.Fatal("Expected non-nil SessionManager")
	}

	info, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("Failed to stat session dir: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("Expected directory permissions 0700, got %o", perm)
	}
}

func TestSave_Permissions(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	cid := core.ContentID{1, 2, 3, 4}
	peerID := peer.ID("test-peer")
	session := &TransferSession{
		ContentID:   cid,
		TargetPeer:  peerID,
		Completed:   []bool{true, false},
		TotalChunks: 2,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := sm.Save(session); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	filePath := sm.getPath(cid)
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Failed to stat session file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected file permissions 0600, got %o", perm)
	}
}

func TestRemediation_LegacyPermissions(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Pre-create session directory with permissive 0755 mode
	sessionDir := filepath.Join(tempDir, "sessions")
	if err := os.MkdirAll(sessionDir, 0755); err != nil {
		t.Fatalf("Failed to create session dir: %v", err)
	}
	if err := os.Chmod(sessionDir, 0755); err != nil {
		t.Fatalf("Failed to set 0755 on session dir: %v", err)
	}

	// Pre-create legacy session file with permissive 0644 mode
	legacyFile := filepath.Join(sessionDir, "legacy.json")
	if err := os.WriteFile(legacyFile, []byte("{}"), 0644); err != nil {
		t.Fatalf("Failed to create legacy file: %v", err)
	}
	if err := os.Chmod(legacyFile, 0644); err != nil {
		t.Fatalf("Failed to set 0644 on legacy file: %v", err)
	}

	// Pre-create legacy subdirectory with 0755 mode and nested file with 0644 mode
	subDir := filepath.Join(sessionDir, "subdir")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}
	if err := os.Chmod(subDir, 0755); err != nil {
		t.Fatalf("Failed to set 0755 on subdir: %v", err)
	}
	nestedFile := filepath.Join(subDir, "nested.json")
	if err := os.WriteFile(nestedFile, []byte("{}"), 0644); err != nil {
		t.Fatalf("Failed to create nested file: %v", err)
	}
	if err := os.Chmod(nestedFile, 0644); err != nil {
		t.Fatalf("Failed to set 0644 on nested file: %v", err)
	}

	// Initialize NewFileSessionManager, which should trigger remediation
	_, err = NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed during remediation: %v", err)
	}

	// Verify sessionDir is now 0700
	if info, err := os.Stat(sessionDir); err != nil || info.Mode().Perm() != 0700 {
		t.Errorf("Expected sessionDir perm 0700, got %o (err: %v)", info.Mode().Perm(), err)
	}

	// Verify legacyFile is now 0600
	if info, err := os.Stat(legacyFile); err != nil || info.Mode().Perm() != 0600 {
		t.Errorf("Expected legacyFile perm 0600, got %o (err: %v)", info.Mode().Perm(), err)
	}

	// Verify subDir is now 0700
	if info, err := os.Stat(subDir); err != nil || info.Mode().Perm() != 0700 {
		t.Errorf("Expected subDir perm 0700, got %o (err: %v)", info.Mode().Perm(), err)
	}

	// Verify nestedFile is now 0600
	if info, err := os.Stat(nestedFile); err != nil || info.Mode().Perm() != 0600 {
		t.Errorf("Expected nestedFile perm 0600, got %o (err: %v)", info.Mode().Perm(), err)
	}
}
