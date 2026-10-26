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

func helperGeneratePeerID(t *testing.T) peer.ID {
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

func TestFileSessionManager_NewDirectoryPermissions(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sessionDir := filepath.Join(tempDir, "sessions")
	_, err = NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	info, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("failed to stat session dir: %v", err)
	}

	perm := info.Mode().Perm()
	if perm != 0700 {
		t.Errorf("expected session dir permissions 0700, got %o", perm)
	}
}

func TestFileSessionManager_SaveFilePermissions(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	cid := core.ContentID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	session := &TransferSession{
		ContentID:   cid,
		TargetPeer:  helperGeneratePeerID(t),
		Completed:   []bool{true, false, true},
		TotalChunks: 3,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := sm.Save(session); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	filePath := sm.getPath(cid)
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat session file: %v", err)
	}

	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Errorf("expected session file permissions 0600, got %o", perm)
	}
}

func TestFileSessionManager_RemediateExistingDirectoryAndFiles(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sessionDir := filepath.Join(tempDir, "legacy_sessions")

	// Pre-create session dir with 0755
	if err := os.MkdirAll(sessionDir, 0755); err != nil {
		t.Fatalf("failed to pre-create session dir: %v", err)
	}
	if err := os.Chmod(sessionDir, 0755); err != nil {
		t.Fatalf("failed to chmod session dir to 0755: %v", err)
	}

	// Pre-create legacy file with 0644
	legacyFile := filepath.Join(sessionDir, "legacy.json")
	if err := os.WriteFile(legacyFile, []byte("{}"), 0644); err != nil {
		t.Fatalf("failed to create legacy file: %v", err)
	}
	if err := os.Chmod(legacyFile, 0644); err != nil {
		t.Fatalf("failed to chmod legacy file to 0644: %v", err)
	}

	// Pre-create nested dir with 0755 and file with 0644
	nestedDir := filepath.Join(sessionDir, "nested")
	if err := os.MkdirAll(nestedDir, 0755); err != nil {
		t.Fatalf("failed to create nested dir: %v", err)
	}
	if err := os.Chmod(nestedDir, 0755); err != nil {
		t.Fatalf("failed to chmod nested dir: %v", err)
	}
	nestedFile := filepath.Join(nestedDir, "nested.json")
	if err := os.WriteFile(nestedFile, []byte("{}"), 0644); err != nil {
		t.Fatalf("failed to create nested file: %v", err)
	}
	if err := os.Chmod(nestedFile, 0644); err != nil {
		t.Fatalf("failed to chmod nested file: %v", err)
	}

	// Instantiate NewFileSessionManager which should trigger permission remediation
	_, err = NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	// Check session dir mode
	if info, err := os.Stat(sessionDir); err != nil || info.Mode().Perm() != 0700 {
		t.Errorf("expected session dir permissions 0700, got %o (err: %v)", info.Mode().Perm(), err)
	}

	// Check legacy file mode
	if info, err := os.Stat(legacyFile); err != nil || info.Mode().Perm() != 0600 {
		t.Errorf("expected legacy file permissions 0600, got %o (err: %v)", info.Mode().Perm(), err)
	}

	// Check nested dir mode
	if info, err := os.Stat(nestedDir); err != nil || info.Mode().Perm() != 0700 {
		t.Errorf("expected nested dir permissions 0700, got %o (err: %v)", info.Mode().Perm(), err)
	}

	// Check nested file mode
	if info, err := os.Stat(nestedFile); err != nil || info.Mode().Perm() != 0600 {
		t.Errorf("expected nested file permissions 0600, got %o (err: %v)", info.Mode().Perm(), err)
	}
}

func TestFileSessionManager_LifecycleOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	cid := core.ContentID{9, 9, 9}

	// Open non-existent session
	opened, err := sm.Open(cid)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if opened != nil {
		t.Errorf("expected nil for non-existent session, got %v", opened)
	}

	peerID := helperGeneratePeerID(t)

	// Save session
	sess := &TransferSession{
		ContentID:   cid,
		TargetPeer:  peerID,
		Completed:   []bool{true, true, false},
		TotalChunks: 3,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}
	if err := sm.Save(sess); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Open existing session
	opened, err = sm.Open(cid)
	if err != nil {
		t.Fatalf("Open existing session failed: %v", err)
	}
	if opened == nil {
		t.Fatalf("expected session, got nil")
	}
	if opened.CompletedCount() != 2 {
		t.Errorf("expected CompletedCount 2, got %d", opened.CompletedCount())
	}
	if opened.Status != StatusInProgress {
		t.Errorf("expected Status IN_PROGRESS, got %s", opened.Status)
	}

	// List sessions
	list, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("expected 1 session in list, got %d", len(list))
	}

	// Close session
	if err := sm.Close(cid); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Delete session
	if err := sm.Delete(cid); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	opened, err = sm.Open(cid)
	if err != nil {
		t.Fatalf("Open failed after delete: %v", err)
	}
	if opened != nil {
		t.Errorf("expected nil after delete, got %v", opened)
	}
}
