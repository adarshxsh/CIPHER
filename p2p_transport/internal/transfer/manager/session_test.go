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

func generateTestPeerID(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}
	peerID, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("failed to get peer ID from pubkey: %v", err)
	}
	return peerID
}

func TestFileSessionManager_DirectoryPermissions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sessionDir := filepath.Join(tmpDir, "sessions")

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}
	_ = sm

	info, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("failed to stat session dir: %v", err)
	}

	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("expected session directory mode 0700, got %04o", perm)
	}
}

func TestFileSessionManager_PreexistingDirectoryPermissions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sessionDir := filepath.Join(tmpDir, "sessions_preexisting")
	if err := os.MkdirAll(sessionDir, 0755); err != nil {
		t.Fatalf("failed to create preexisting dir: %v", err)
	}

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}
	_ = sm

	info, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("failed to stat session dir: %v", err)
	}

	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("expected updated session directory mode 0700, got %04o", perm)
	}
}

func TestFileSessionManager_FilePermissions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sessionDir := filepath.Join(tmpDir, "sessions")
	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	cid := core.ContentID{1, 2, 3, 4, 5}
	peerID := generateTestPeerID(t)
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

	filePath := sm.getPath(cid)
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat session file: %v", err)
	}

	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("expected session state file mode 0600, got %04o", perm)
	}
}

func TestFileSessionManager_FixExistingFilePermissions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sessionDir := filepath.Join(tmpDir, "sessions")
	if err := os.MkdirAll(sessionDir, 0700); err != nil {
		t.Fatalf("failed to create session dir: %v", err)
	}

	// Create an existing file with insecure mode 0644
	existingFile := filepath.Join(sessionDir, "insecure.json")
	if err := os.WriteFile(existingFile, []byte("{}"), 0644); err != nil {
		t.Fatalf("failed to create existing file: %v", err)
	}

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to initialize session manager: %v", err)
	}
	_ = sm

	info, err := os.Stat(existingFile)
	if err != nil {
		t.Fatalf("failed to stat existing file: %v", err)
	}

	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("expected existing file mode to be corrected to 0600, got %04o", perm)
	}
}

func TestFileSessionManager_Lifecycle(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sessionDir := filepath.Join(tmpDir, "sessions")
	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	cid := core.ContentID{10, 20, 30}
	s, err := sm.Open(cid)
	if err != nil {
		t.Fatalf("failed to open non-existent session: %v", err)
	}
	if s != nil {
		t.Errorf("expected nil for non-existent session, got %v", s)
	}

	peerID := generateTestPeerID(t)
	sess := &TransferSession{
		ContentID:   cid,
		TargetPeer:  peerID,
		Completed:   []bool{true},
		TotalChunks: 1,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := sm.Save(sess); err != nil {
		t.Fatalf("failed to save session: %v", err)
	}

	loaded, err := sm.Open(cid)
	if err != nil {
		t.Fatalf("failed to open saved session: %v", err)
	}
	if loaded == nil || loaded.Status != StatusInProgress {
		t.Errorf("unexpected loaded session: %v", loaded)
	}

	list, err := sm.List()
	if err != nil {
		t.Fatalf("failed to list sessions: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("expected 1 session, got %d", len(list))
	}

	if err := sm.Delete(cid); err != nil {
		t.Fatalf("failed to delete session: %v", err)
	}

	listAfter, err := sm.List()
	if err != nil {
		t.Fatalf("failed to list sessions after delete: %v", err)
	}
	if len(listAfter) != 0 {
		t.Errorf("expected 0 sessions after delete, got %d", len(listAfter))
	}
}
