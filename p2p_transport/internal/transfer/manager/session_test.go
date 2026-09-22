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
	_, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("failed to get peer ID from public key: %v", err)
	}
	return id
}

func TestNewFileSessionManager_NewDirectory(t *testing.T) {
	tempDir := t.TempDir()
	sessionDir := filepath.Join(tempDir, "sessions")

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("unexpected error creating session manager: %v", err)
	}
	if sm == nil {
		t.Fatal("expected non-nil FileSessionManager")
	}

	info, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("failed to stat session dir: %v", err)
	}

	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("expected session dir mode 0700, got %04o", perm)
	}
}

func TestNewFileSessionManager_ExistingDirectory(t *testing.T) {
	tempDir := t.TempDir()
	sessionDir := filepath.Join(tempDir, "unhardened_sessions")

	if err := os.MkdirAll(sessionDir, 0755); err != nil {
		t.Fatalf("failed to pre-create dir: %v", err)
	}
	if err := os.Chmod(sessionDir, 0755); err != nil {
		t.Fatalf("failed to set 0755 on pre-created dir: %v", err)
	}

	info, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("failed to stat session dir: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0755 {
		t.Fatalf("setup failed: expected pre-created dir mode 0755, got %04o", perm)
	}

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("unexpected error initializing session manager on existing dir: %v", err)
	}
	if sm == nil {
		t.Fatal("expected non-nil FileSessionManager")
	}

	info, err = os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("failed to stat session dir after init: %v", err)
	}

	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("expected existing session dir to be hardened to mode 0700, got %04o", perm)
	}
}

func TestFileSessionManager_Save_FilePermissions(t *testing.T) {
	tempDir := t.TempDir()
	sessionDir := filepath.Join(tempDir, "sessions")

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("unexpected error initializing session manager: %v", err)
	}

	dummyID := core.ContentID{0x01, 0x02, 0x03, 0x04}
	session := &TransferSession{
		ContentID:   dummyID,
		TargetPeer:  generateTestPeerID(t),
		Completed:   []bool{true, false, true},
		TotalChunks: 3,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := sm.Save(session); err != nil {
		t.Fatalf("failed to save session: %v", err)
	}

	filePath := sm.getPath(dummyID)
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat saved session file: %v", err)
	}

	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("expected session file mode 0600, got %04o", perm)
	}
}

func TestFileSessionManager_CRUD(t *testing.T) {
	tempDir := t.TempDir()
	sessionDir := filepath.Join(tempDir, "sessions")

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	dummyID := core.ContentID{0xaa, 0xbb, 0xcc, 0xdd}

	// Open non-existent
	s, err := sm.Open(dummyID)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if s != nil {
		t.Errorf("expected nil session for non-existent ID, got %v", s)
	}

	// Save
	peerID := generateTestPeerID(t)
	session := &TransferSession{
		ContentID:   dummyID,
		TargetPeer:  peerID,
		Completed:   []bool{true, true},
		TotalChunks: 2,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}
	if err := sm.Save(session); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Open existing
	loaded, err := sm.Open(dummyID)
	if err != nil {
		t.Fatalf("Open existing failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected non-nil session")
	}
	if loaded.ContentID != dummyID {
		t.Errorf("expected ContentID %v, got %v", dummyID, loaded.ContentID)
	}
	if loaded.TargetPeer != peerID {
		t.Errorf("expected TargetPeer %v, got %v", peerID, loaded.TargetPeer)
	}
	if loaded.Status != StatusInProgress {
		t.Errorf("expected Status %v, got %v", StatusInProgress, loaded.Status)
	}

	// List
	list, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 session in List, got %d", len(list))
	}
	if list[0].ContentID != dummyID {
		t.Errorf("expected ContentID %v in list, got %v", dummyID, list[0].ContentID)
	}

	// Close
	if err := sm.Close(dummyID); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Delete
	if err := sm.Delete(dummyID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deletion
	loadedAfterDelete, err := sm.Open(dummyID)
	if err != nil {
		t.Fatalf("Open after delete failed: %v", err)
	}
	if loadedAfterDelete != nil {
		t.Errorf("expected nil session after delete, got %v", loadedAfterDelete)
	}
}
