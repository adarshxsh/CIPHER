package manager_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cipher/internal/content/core"
	"cipher/internal/transfer/manager"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestNewFileSessionManager_Permissions(t *testing.T) {
	tmpDir := t.TempDir()
	sessDir := filepath.Join(tmpDir, "sessions")

	// Test 1: Directory creation mode 0700
	sm, err := manager.NewFileSessionManager(sessDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}
	if sm == nil {
		t.Fatal("expected non-nil FileSessionManager")
	}

	info, err := os.Stat(sessDir)
	if err != nil {
		t.Fatalf("failed to stat session dir: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("expected dir permissions 0700, got %04o", perm)
	}

	// Test 2: Existing permissive directory remediated to 0700
	permissiveDir := filepath.Join(tmpDir, "permissive_sessions")
	if err := os.MkdirAll(permissiveDir, 0755); err != nil {
		t.Fatalf("failed to create permissive dir: %v", err)
	}
	if err := os.Chmod(permissiveDir, 0755); err != nil {
		t.Fatalf("failed to chmod permissive dir: %v", err)
	}

	_, err = manager.NewFileSessionManager(permissiveDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed on existing dir: %v", err)
	}

	info, err = os.Stat(permissiveDir)
	if err != nil {
		t.Fatalf("failed to stat permissive dir: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("expected remediated dir permissions 0700, got %04o", perm)
	}
}

func TestSave_Permissions(t *testing.T) {
	tmpDir := t.TempDir()
	sessDir := filepath.Join(tmpDir, "sessions")

	sm, err := manager.NewFileSessionManager(sessDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	var contentID core.ContentID
	copy(contentID[:], []byte("01234567890123456789012345678901"))
	dummyPeer, _ := peer.Decode("12D3KooWGzR29Sj9P28b6d8p1w5a8m2v8q4x7y1z3a4b5c6d7e8f")

	session := &manager.TransferSession{
		ContentID:   contentID,
		TargetPeer:  dummyPeer,
		Completed:   []bool{true, false, true},
		TotalChunks: 3,
		StartedAt:   time.Now(),
		Status:      manager.StatusInProgress,
	}

	if err := sm.Save(session); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	filePath := filepath.Join(sessDir, "3031323334353637383930313233343536373839303132333435363738393031.json")
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat saved session file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("expected session file permissions 0600, got %04o", perm)
	}
}

func TestOpen_RemediatesPermissions(t *testing.T) {
	tmpDir := t.TempDir()
	sessDir := filepath.Join(tmpDir, "sessions")

	sm, err := manager.NewFileSessionManager(sessDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	var contentID core.ContentID
	copy(contentID[:], []byte("abcdefghijklmnopqrstuvwxyz123456"))
	dummyPeer, _ := peer.Decode("12D3KooWGzR29Sj9P28b6d8p1w5a8m2v8q4x7y1z3a4b5c6d7e8f")

	originalSession := &manager.TransferSession{
		ContentID:   contentID,
		TargetPeer:  dummyPeer,
		Completed:   []bool{true, true},
		TotalChunks: 2,
		StartedAt:   time.Now(),
		Status:      manager.StatusCompleted,
	}

	data, err := json.MarshalIndent(originalSession, "", "  ")
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	filePath := filepath.Join(sessDir, "6162636465666768696a6b6c6d6e6f707172737475767778797a313233343536.json")
	// Write file with permissive 0644 mode
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		t.Fatalf("failed to write permissive session file: %v", err)
	}
	if err := os.Chmod(filePath, 0644); err != nil {
		t.Fatalf("failed to chmod permissive session file: %v", err)
	}

	// Verify file is currently 0644
	info, err := os.Stat(filePath)
	if err != nil || info.Mode().Perm() != 0644 {
		t.Fatalf("expected initial permission 0644, got %04o", info.Mode().Perm())
	}

	// Open should remediate to 0600
	openedSession, err := sm.Open(contentID)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if openedSession == nil {
		t.Fatal("expected non-nil session")
	}

	info, err = os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat session file after Open: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("expected remediated permissions 0600, got %04o", perm)
	}
}

func TestList_RemediatesPermissions(t *testing.T) {
	tmpDir := t.TempDir()
	sessDir := filepath.Join(tmpDir, "sessions")

	sm, err := manager.NewFileSessionManager(sessDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	var contentID core.ContentID
	copy(contentID[:], []byte("11112222333344445555666677778888"))
	dummyPeer, _ := peer.Decode("12D3KooWGzR29Sj9P28b6d8p1w5a8m2v8q4x7y1z3a4b5c6d7e8f")

	sess := &manager.TransferSession{
		ContentID:   contentID,
		TargetPeer:  dummyPeer,
		Completed:   []bool{false},
		TotalChunks: 1,
		StartedAt:   time.Now(),
		Status:      manager.StatusFailed,
	}

	data, err := json.MarshalIndent(sess, "", "  ")
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	filePath := filepath.Join(sessDir, "3131313132323232333333333434343435353535363636363737373738383838.json")
	if err := os.WriteFile(filePath, data, 0666); err != nil {
		t.Fatalf("failed to write permissive session file: %v", err)
	}
	if err := os.Chmod(filePath, 0666); err != nil {
		t.Fatalf("failed to chmod permissive session file: %v", err)
	}

	// List should find session and remediate permissions to 0600
	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(sessions))
	}

	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat session file after List: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("expected remediated permissions 0600, got %04o", perm)
	}
}

func TestDelete_And_Close(t *testing.T) {
	tmpDir := t.TempDir()
	sessDir := filepath.Join(tmpDir, "sessions")

	sm, err := manager.NewFileSessionManager(sessDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	var contentID core.ContentID
	copy(contentID[:], []byte("99998888777766665555444433332222"))

	sess := &manager.TransferSession{
		ContentID:   contentID,
		TotalChunks: 5,
		Status:      manager.StatusInProgress,
	}

	if err := sm.Save(sess); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	if err := sm.Close(contentID); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if err := sm.Delete(contentID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	loaded, err := sm.Open(contentID)
	if err != nil {
		t.Fatalf("Open after Delete failed: %v", err)
	}
	if loaded != nil {
		t.Errorf("expected nil session after Delete, got %v", loaded)
	}
}
