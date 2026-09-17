package manager

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestNewFileSessionManager_DirectoryPermissions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sessDir := filepath.Join(tmpDir, "sessions")

	sm, err := NewFileSessionManager(sessDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}
	if sm == nil {
		t.Fatalf("Expected non-nil SessionManager")
	}

	info, err := os.Stat(sessDir)
	if err != nil {
		t.Fatalf("Failed to stat session directory: %v", err)
	}

	perm := info.Mode().Perm()
	if perm != 0700 {
		t.Errorf("Expected session directory mode 0700, got %o", perm)
	}
}

func TestNewFileSessionManager_RemediateExistingDirectory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sessDir := filepath.Join(tmpDir, "sessions_preexisting")

	// Pre-create session directory with mode 0755
	if err := os.MkdirAll(sessDir, 0755); err != nil {
		t.Fatalf("Failed to pre-create session directory: %v", err)
	}
	if err := os.Chmod(sessDir, 0755); err != nil {
		t.Fatalf("Failed to chmod session directory to 0755: %v", err)
	}

	infoBefore, err := os.Stat(sessDir)
	if err != nil {
		t.Fatalf("Failed to stat session directory before: %v", err)
	}
	if infoBefore.Mode().Perm() != 0755 {
		t.Fatalf("Pre-test setup failed: expected 0755 before, got %o", infoBefore.Mode().Perm())
	}

	// NewFileSessionManager should remediate 0755 to 0700
	_, err = NewFileSessionManager(sessDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	infoAfter, err := os.Stat(sessDir)
	if err != nil {
		t.Fatalf("Failed to stat session directory after: %v", err)
	}

	permAfter := infoAfter.Mode().Perm()
	if permAfter != 0700 {
		t.Errorf("Expected remediated session directory mode 0700, got %o", permAfter)
	}
}

func TestFileSessionManager_SaveAndOpen_Permissions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sessDir := filepath.Join(tmpDir, "sessions")
	sm, err := NewFileSessionManager(sessDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	contentID := core.ContentID{0x01, 0x02, 0x03, 0x04}
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}
	targetPeer, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("Failed to get peer ID: %v", err)
	}
	session := &TransferSession{
		ContentID:   contentID,
		TargetPeer:  targetPeer,
		Completed:   []bool{true, false, true},
		TotalChunks: 3,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := sm.Save(session); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	filePath := sm.getPath(contentID)
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Failed to stat saved session file: %v", err)
	}

	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Errorf("Expected saved session file mode 0600, got %o", perm)
	}

	// Verify session can be opened properly
	loaded, err := sm.Open(contentID)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if loaded == nil {
		t.Fatalf("Expected non-nil session on Open")
	}

	if loaded.ContentID != contentID {
		t.Errorf("ContentID mismatch: got %v, want %v", loaded.ContentID, contentID)
	}
	if loaded.Status != StatusInProgress {
		t.Errorf("Status mismatch: got %s, want %s", loaded.Status, StatusInProgress)
	}
	if loaded.CompletedCount() != 2 {
		t.Errorf("CompletedCount mismatch: got %d, want 2", loaded.CompletedCount())
	}
}

func TestFileSessionManager_ListAndDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sessDir := filepath.Join(tmpDir, "sessions")
	sm, err := NewFileSessionManager(sessDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	c1 := core.ContentID{0x10}
	c2 := core.ContentID{0x20}

	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}
	targetPeer, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("Failed to get peer ID: %v", err)
	}

	s1 := &TransferSession{ContentID: c1, TargetPeer: targetPeer, Status: StatusInProgress}
	s2 := &TransferSession{ContentID: c2, TargetPeer: targetPeer, Status: StatusCompleted}

	if err := sm.Save(s1); err != nil {
		t.Fatalf("Save s1 failed: %v", err)
	}
	if err := sm.Save(s2); err != nil {
		t.Fatalf("Save s2 failed: %v", err)
	}

	list, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("Expected 2 sessions in list, got %d", len(list))
	}

	if err := sm.Delete(c1); err != nil {
		t.Fatalf("Delete s1 failed: %v", err)
	}

	loaded, err := sm.Open(c1)
	if err != nil {
		t.Fatalf("Open deleted session failed: %v", err)
	}
	if loaded != nil {
		t.Errorf("Expected nil session for deleted session")
	}

	listAfter, err := sm.List()
	if err != nil {
		t.Fatalf("List after delete failed: %v", err)
	}
	if len(listAfter) != 1 {
		t.Errorf("Expected 1 session in list after delete, got %d", len(listAfter))
	}
}
