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

func TestNewFileSessionManager_Permissions(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sessDir := filepath.Join(tempDir, "sessions")

	// 1. Test creation of new session directory with restricted permissions
	sm, err := NewFileSessionManager(sessDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	info, err := os.Stat(sessDir)
	if err != nil {
		t.Fatalf("Stat on session dir failed: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("Expected session dir permissions 0700, got %o", perm)
	}

	// 2. Test saving a new session produces 0600 file permissions
	var cid core.ContentID
	copy(cid[:], []byte("12345678901234567890123456789012"))
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}
	testPeer, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("Failed to get peer ID: %v", err)
	}

	session := &TransferSession{
		ContentID:   cid,
		TargetPeer:  testPeer,
		Completed:   []bool{true, false},
		TotalChunks: 2,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := sm.Save(session); err != nil {
		t.Fatalf("Save session failed: %v", err)
	}

	filePath := sm.getPath(cid)
	fInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Stat on session file failed: %v", err)
	}
	if perm := fInfo.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected session file permissions 0600, got %o", perm)
	}
}

func TestNewFileSessionManager_LegacyUpgrade(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_legacy_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sessDir := filepath.Join(tempDir, "sessions")

	// Pre-create directory with permissive 0755 permissions
	if err := os.MkdirAll(sessDir, 0755); err != nil {
		t.Fatalf("Failed to create legacy sessDir: %v", err)
	}
	if err := os.Chmod(sessDir, 0755); err != nil {
		t.Fatalf("Failed to chmod legacy sessDir: %v", err)
	}

	// Pre-create a legacy session file with 0644 permissions
	legacyFile := filepath.Join(sessDir, "legacy.json")
	if err := os.WriteFile(legacyFile, []byte("{}"), 0644); err != nil {
		t.Fatalf("Failed to write legacy file: %v", err)
	}
	if err := os.Chmod(legacyFile, 0644); err != nil {
		t.Fatalf("Failed to chmod legacy file: %v", err)
	}

	// Verify legacy permissions before manager startup
	infoBefore, _ := os.Stat(sessDir)
	if perm := infoBefore.Mode().Perm(); perm != 0755 {
		t.Fatalf("Setup check failed: expected 0755 for legacy dir, got %o", perm)
	}
	fileInfoBefore, _ := os.Stat(legacyFile)
	if perm := fileInfoBefore.Mode().Perm(); perm != 0644 {
		t.Fatalf("Setup check failed: expected 0644 for legacy file, got %o", perm)
	}

	// Initialize NewFileSessionManager which should fix permissions
	_, err = NewFileSessionManager(sessDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed on legacy dir: %v", err)
	}

	// Verify directory and file permissions were upgraded
	infoAfter, err := os.Stat(sessDir)
	if err != nil {
		t.Fatalf("Stat on session dir failed: %v", err)
	}
	if perm := infoAfter.Mode().Perm(); perm != 0700 {
		t.Errorf("Expected upgraded session dir permissions 0700, got %o", perm)
	}

	fileInfoAfter, err := os.Stat(legacyFile)
	if err != nil {
		t.Fatalf("Stat on legacy file failed: %v", err)
	}
	if perm := fileInfoAfter.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected upgraded legacy file permissions 0600, got %o", perm)
	}
}

func TestFileSessionManager_CRUD(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_crud_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	var cid core.ContentID
	copy(cid[:], []byte("abcdefghijklmnopqrstuvwxyz123456"))

	// 1. Open non-existent session
	s, err := sm.Open(cid)
	if err != nil {
		t.Fatalf("Open non-existent session returned error: %v", err)
	}
	if s != nil {
		t.Errorf("Expected nil for non-existent session, got %v", s)
	}

	// 2. Save session
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}
	testPeer, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("Failed to get peer ID: %v", err)
	}
	sess := &TransferSession{
		ContentID:   cid,
		TargetPeer:  testPeer,
		Completed:   []bool{true, true},
		TotalChunks: 2,
		StartedAt:   time.Now(),
		Status:      StatusCompleted,
	}
	if err := sm.Save(sess); err != nil {
		t.Fatalf("Save session failed: %v", err)
	}

	// 3. Open saved session
	loaded, err := sm.Open(cid)
	if err != nil {
		t.Fatalf("Open saved session failed: %v", err)
	}
	if loaded == nil {
		t.Fatalf("Expected session to be loaded, got nil")
	}
	if loaded.Status != StatusCompleted {
		t.Errorf("Expected status StatusCompleted, got %s", loaded.Status)
	}

	// 4. List sessions
	list, err := sm.List()
	if err != nil {
		t.Fatalf("List sessions failed: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("Expected 1 session in list, got %d", len(list))
	}

	// 5. Delete session
	if err := sm.Delete(cid); err != nil {
		t.Fatalf("Delete session failed: %v", err)
	}

	loadedAfterDelete, err := sm.Open(cid)
	if err != nil {
		t.Fatalf("Open after delete returned error: %v", err)
	}
	if loadedAfterDelete != nil {
		t.Errorf("Expected nil after delete, got %v", loadedAfterDelete)
	}
}
