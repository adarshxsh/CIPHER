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
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sessionDir := filepath.Join(tempDir, "sessions")

	// Pre-create sessionDir with 0755 permissions to verify update to 0700
	if err := os.MkdirAll(sessionDir, 0755); err != nil {
		t.Fatalf("Failed to pre-create sessionDir: %v", err)
	}
	if err := os.Chmod(sessionDir, 0755); err != nil {
		t.Fatalf("Failed to chmod sessionDir: %v", err)
	}

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	// Verify directory permissions are 0700
	dirInfo, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("os.Stat sessionDir failed: %v", err)
	}
	if perm := dirInfo.Mode().Perm(); perm != 0700 {
		t.Errorf("Expected session dir permissions 0700, got %#o", perm)
	}

	// Create dummy session
	var cid core.ContentID
	copy(cid[:], []byte("01234567890123456789012345678901"))

	dummyPeer, _ := peer.Decode("12D3KooWSD5x26s8X91qXjUv5B3A6yvY8K3B8W3K8Y3K8Y3K8Y3K")

	sess := &TransferSession{
		ContentID:   cid,
		TargetPeer:  dummyPeer,
		Completed:   []bool{true, false, true},
		TotalChunks: 3,
		StartedAt:   time.Now().Truncate(time.Second),
		Status:      StatusInProgress,
	}

	// Save session
	if err := sm.Save(sess); err != nil {
		t.Fatalf("Save session failed: %v", err)
	}

	// Verify file permissions are 0600
	sessionFilePath := filepath.Join(sessionDir, sm.getPath(cid)[len(sessionDir)+1:])
	fileInfo, err := os.Stat(sessionFilePath)
	if err != nil {
		t.Fatalf("os.Stat session file failed: %v", err)
	}
	if perm := fileInfo.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected session file permissions 0600, got %#o", perm)
	}

	// Verify pre-existing 0644 file gets updated to 0600 on Save
	if err := os.Chmod(sessionFilePath, 0644); err != nil {
		t.Fatalf("Failed to chmod session file to 0644: %v", err)
	}

	sess.Status = StatusCompleted
	if err := sm.Save(sess); err != nil {
		t.Fatalf("Save session second time failed: %v", err)
	}

	fileInfo, err = os.Stat(sessionFilePath)
	if err != nil {
		t.Fatalf("os.Stat session file failed: %v", err)
	}
	if perm := fileInfo.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected session file permissions 0600 after update, got %#o", perm)
	}
}

func TestFileSessionManager_CRUD(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_crud_test_*")
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

	// Open non-existent session
	loaded, err := sm.Open(cid)
	if err != nil {
		t.Fatalf("Open non-existent session returned error: %v", err)
	}
	if loaded != nil {
		t.Errorf("Expected nil for non-existent session, got %+v", loaded)
	}

	dummyPeer, _ := peer.Decode("12D3KooWSD5x26s8X91qXjUv5B3A6yvY8K3B8W3K8Y3K8Y3K8Y3K")

	sess := &TransferSession{
		ContentID:   cid,
		TargetPeer:  dummyPeer,
		Completed:   []bool{true, true},
		TotalChunks: 2,
		StartedAt:   time.Now().Truncate(time.Second),
		Status:      StatusCompleted,
	}

	// Save
	if err := sm.Save(sess); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Open
	loaded, err = sm.Open(cid)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if loaded == nil {
		t.Fatalf("Expected session, got nil")
	}
	if loaded.ContentID != cid {
		t.Errorf("Expected ContentID %v, got %v", cid, loaded.ContentID)
	}
	if loaded.Status != StatusCompleted {
		t.Errorf("Expected status %s, got %s", StatusCompleted, loaded.Status)
	}
	if loaded.CompletedCount() != 2 {
		t.Errorf("Expected CompletedCount 2, got %d", loaded.CompletedCount())
	}

	// List
	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("Expected 1 session listed, got %d", len(sessions))
	}

	// Close
	if err := sm.Close(cid); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	// Delete
	if err := sm.Delete(cid); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	loaded, err = sm.Open(cid)
	if err != nil {
		t.Fatalf("Open after delete failed: %v", err)
	}
	if loaded != nil {
		t.Errorf("Expected nil after delete, got %+v", loaded)
	}
}
