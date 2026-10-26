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
	tempDir, err := os.MkdirTemp("", "session_perm_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sessionDir := filepath.Join(tempDir, "sessions")
	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create NewFileSessionManager: %v", err)
	}

	// Verify directory permissions are 0700
	dirInfo, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("failed to stat session dir: %v", err)
	}
	if perm := dirInfo.Mode().Perm(); perm != 0700 {
		t.Errorf("expected directory perm 0700, got %o", perm)
	}

	cid := core.ContentID{1, 2, 3, 4, 5, 6, 7, 8}
	peerID, err := peer.Decode("12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN")
	if err != nil {
		t.Fatalf("failed to decode peer id: %v", err)
	}
	session := &TransferSession{
		ContentID:   cid,
		TargetPeer:  peerID,
		TotalChunks: 10,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := sm.Save(session); err != nil {
		t.Fatalf("failed to save session: %v", err)
	}

	sessionPath := sm.getPath(cid)
	fileInfo, err := os.Stat(sessionPath)
	if err != nil {
		t.Fatalf("failed to stat session file: %v", err)
	}

	// Verify file permissions are 0600
	if perm := fileInfo.Mode().Perm(); perm != 0600 {
		t.Errorf("expected session file perm 0600, got %o", perm)
	}

	// Verify Open succeeds for 0600
	loaded, err := sm.Open(cid)
	if err != nil {
		t.Fatalf("failed to open session with 0600 perm: %v", err)
	}
	if loaded == nil || loaded.TotalChunks != 10 {
		t.Errorf("unexpected loaded session content: %+v", loaded)
	}

	// Verify List succeeds
	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("failed to list sessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Errorf("expected 1 session, got %d", len(sessions))
	}
}

func TestFileSessionManager_InsecurePermissions(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_insecure_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sessionDir := filepath.Join(tempDir, "sessions")
	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create NewFileSessionManager: %v", err)
	}

	cid := core.ContentID{10, 20, 30, 40}
	peerID, err := peer.Decode("12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN")
	if err != nil {
		t.Fatalf("failed to decode peer id: %v", err)
	}
	session := &TransferSession{
		ContentID:   cid,
		TargetPeer:  peerID,
		TotalChunks: 5,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := sm.Save(session); err != nil {
		t.Fatalf("failed to save session: %v", err)
	}

	sessionPath := sm.getPath(cid)

	// Change file permissions to 0644 (insecure)
	if err := os.Chmod(sessionPath, 0644); err != nil {
		t.Fatalf("failed to chmod file: %v", err)
	}

	// Verify Open fails due to insecure permissions
	_, err = sm.Open(cid)
	if err == nil {
		t.Errorf("expected Open to fail for file with 0644 permissions, but got no error")
	}

	// Verify List ignores files with insecure permissions
	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("failed to list sessions: %v", err)
	}
	if len(sessions) != 0 {
		t.Errorf("expected 0 sessions listed due to insecure permissions, got %d", len(sessions))
	}
}
