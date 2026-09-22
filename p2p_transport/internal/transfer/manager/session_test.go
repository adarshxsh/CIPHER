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

func newTestPeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("failed to get peer ID from key: %v", err)
	}
	return pid
}

func TestNewFileSessionManager_DirectoryPermissions(t *testing.T) {
	tmpDir := t.TempDir()
	sessionDir := filepath.Join(tmpDir, "sessions")

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}
	if sm == nil {
		t.Fatal("expected non-nil FileSessionManager")
	}

	info, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("failed to stat session directory: %v", err)
	}

	expectedMode := os.FileMode(0700)
	if info.Mode().Perm() != expectedMode {
		t.Errorf("expected session directory permissions %o, got %o", expectedMode, info.Mode().Perm())
	}
}

func TestNewFileSessionManager_PreExistingDirectoryPermissions(t *testing.T) {
	tmpDir := t.TempDir()
	sessionDir := filepath.Join(tmpDir, "preexisting_sessions")

	// Create pre-existing directory with permissive 0755 mode
	if err := os.MkdirAll(sessionDir, 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}
	if err := os.Chmod(sessionDir, 0755); err != nil {
		t.Fatalf("failed to chmod dir: %v", err)
	}

	// Verify pre-existing mode is 0755
	preInfo, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("failed to stat dir: %v", err)
	}
	if preInfo.Mode().Perm() != os.FileMode(0755) {
		t.Fatalf("expected pre-existing mode 0755, got %o", preInfo.Mode().Perm())
	}

	// Initializing NewFileSessionManager must harden directory to 0700
	_, err = NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	postInfo, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("failed to stat dir: %v", err)
	}

	expectedMode := os.FileMode(0700)
	if postInfo.Mode().Perm() != expectedMode {
		t.Errorf("expected hardened directory permissions %o, got %o", expectedMode, postInfo.Mode().Perm())
	}
}

func TestSave_FilePermissions(t *testing.T) {
	tmpDir := t.TempDir()
	sessionDir := filepath.Join(tmpDir, "sessions")

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	var contentID core.ContentID
	copy(contentID[:], []byte("test-content-id-1234567890123456"))

	testPeer := newTestPeerID(t)

	session := &TransferSession{
		ContentID:   contentID,
		TargetPeer:  testPeer,
		Completed:   []bool{true, false, true},
		TotalChunks: 3,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := sm.Save(session); err != nil {
		t.Fatalf("failed to save session: %v", err)
	}

	filePath := sm.getPath(contentID)
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat saved state file: %v", err)
	}

	expectedFileMode := os.FileMode(0600)
	if info.Mode().Perm() != expectedFileMode {
		t.Errorf("expected session state file permissions %o, got %o", expectedFileMode, info.Mode().Perm())
	}

	// Verify temporary file is cleaned up after atomic write
	tmpPath := filePath + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("expected temporary file %s to be removed, but it exists", tmpPath)
	}
}

func TestSave_AtomicOverwrite(t *testing.T) {
	tmpDir := t.TempDir()
	sessionDir := filepath.Join(tmpDir, "sessions")

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	var contentID core.ContentID
	copy(contentID[:], []byte("test-content-id-overwrite-12345"))

	testPeer := newTestPeerID(t)

	session := &TransferSession{
		ContentID:   contentID,
		TargetPeer:  testPeer,
		Completed:   []bool{false, false},
		TotalChunks: 2,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	// Initial save
	if err := sm.Save(session); err != nil {
		t.Fatalf("initial save failed: %v", err)
	}

	// Update session and save again
	session.Completed[0] = true
	session.Status = StatusCompleted
	if err := sm.Save(session); err != nil {
		t.Fatalf("overwrite save failed: %v", err)
	}

	filePath := sm.getPath(contentID)
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat state file after overwrite: %v", err)
	}

	if info.Mode().Perm() != os.FileMode(0600) {
		t.Errorf("expected file mode 0600 after overwrite, got %o", info.Mode().Perm())
	}

	// Verify loaded content reflects changes
	loaded, err := sm.Open(contentID)
	if err != nil {
		t.Fatalf("failed to open session: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected non-nil session")
	}
	if loaded.Status != StatusCompleted {
		t.Errorf("expected status %s, got %s", StatusCompleted, loaded.Status)
	}
	if !loaded.Completed[0] {
		t.Errorf("expected chunk 0 to be completed")
	}
}

func TestSessionManager_Lifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	sessionDir := filepath.Join(tmpDir, "sessions")

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	var id1, id2 core.ContentID
	copy(id1[:], []byte("content-id-1"))
	copy(id2[:], []byte("content-id-2"))

	testPeer := newTestPeerID(t)

	// Non-existent Open
	s, err := sm.Open(id1)
	if err != nil {
		t.Fatalf("unexpected error on opening non-existent session: %v", err)
	}
	if s != nil {
		t.Fatalf("expected nil for non-existent session, got %v", s)
	}

	s1 := &TransferSession{ContentID: id1, TargetPeer: testPeer, TotalChunks: 5, Status: StatusInProgress}
	s2 := &TransferSession{ContentID: id2, TargetPeer: testPeer, TotalChunks: 10, Status: StatusCompleted}

	if err := sm.Save(s1); err != nil {
		t.Fatalf("failed to save s1: %v", err)
	}
	if err := sm.Save(s2); err != nil {
		t.Fatalf("failed to save s2: %v", err)
	}

	// List
	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("failed to list sessions: %v", err)
	}
	if len(sessions) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(sessions))
	}

	// Close
	if err := sm.Close(id1); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	// Delete
	if err := sm.Delete(id1); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	sessions, err = sm.List()
	if err != nil {
		t.Fatalf("failed to list sessions after delete: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session after delete, got %d", len(sessions))
	}
}
