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
	t.Run("creates new directory with 0700 permissions", func(t *testing.T) {
		tempDir := t.TempDir()
		sessionDir := filepath.Join(tempDir, "sessions_new")

		mgr, err := NewFileSessionManager(sessionDir)
		if err != nil {
			t.Fatalf("NewFileSessionManager failed: %v", err)
		}
		if mgr == nil {
			t.Fatal("expected non-nil FileSessionManager")
		}

		info, err := os.Stat(sessionDir)
		if err != nil {
			t.Fatalf("os.Stat failed: %v", err)
		}

		expectedPerm := os.FileMode(0700)
		if perm := info.Mode().Perm(); perm != expectedPerm {
			t.Errorf("expected directory permissions %o, got %o", expectedPerm, perm)
		}
	})

	t.Run("tightens existing directory permissions from 0755 to 0700", func(t *testing.T) {
		tempDir := t.TempDir()
		sessionDir := filepath.Join(tempDir, "sessions_existing")

		if err := os.MkdirAll(sessionDir, 0755); err != nil {
			t.Fatalf("failed to create initial directory: %v", err)
		}
		if err := os.Chmod(sessionDir, 0755); err != nil {
			t.Fatalf("failed to chmod initial directory: %v", err)
		}

		// Verify initial permissions were 0755
		initialInfo, err := os.Stat(sessionDir)
		if err != nil {
			t.Fatalf("os.Stat failed: %v", err)
		}
		if perm := initialInfo.Mode().Perm(); perm != os.FileMode(0755) {
			t.Fatalf("expected initial permissions 0755, got %o", perm)
		}

		mgr, err := NewFileSessionManager(sessionDir)
		if err != nil {
			t.Fatalf("NewFileSessionManager failed: %v", err)
		}
		if mgr == nil {
			t.Fatal("expected non-nil FileSessionManager")
		}

		info, err := os.Stat(sessionDir)
		if err != nil {
			t.Fatalf("os.Stat failed: %v", err)
		}

		expectedPerm := os.FileMode(0700)
		if perm := info.Mode().Perm(); perm != expectedPerm {
			t.Errorf("expected updated directory permissions %o, got %o", expectedPerm, perm)
		}
	})
}

func TestFileSessionManager_Save_FilePermissions(t *testing.T) {
	tempDir := t.TempDir()
	sessionDir := filepath.Join(tempDir, "sessions")

	mgr, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	var contentID core.ContentID
	contentID[0] = 0xab
	contentID[1] = 0xcd

	_, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}
	testPeerID, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("failed to get peer ID from public key: %v", err)
	}

	session := &TransferSession{
		ContentID:   contentID,
		TargetPeer:  testPeerID,
		Completed:   []bool{true, false, true},
		TotalChunks: 3,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := mgr.Save(session); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	filePath := mgr.getPath(contentID)
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("os.Stat failed for saved session file: %v", err)
	}

	expectedPerm := os.FileMode(0600)
	if perm := info.Mode().Perm(); perm != expectedPerm {
		t.Errorf("expected session file permissions %o, got %o", expectedPerm, perm)
	}

	// Test overwriting/updating existing session
	session.Status = StatusCompleted
	if err := mgr.Save(session); err != nil {
		t.Fatalf("Save (update) failed: %v", err)
	}

	info, err = os.Stat(filePath)
	if err != nil {
		t.Fatalf("os.Stat failed for updated session file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != expectedPerm {
		t.Errorf("expected updated session file permissions %o, got %o", expectedPerm, perm)
	}
}

func TestFileSessionManager_Lifecycle(t *testing.T) {
	tempDir := t.TempDir()
	mgr, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	var contentID core.ContentID
	contentID[0] = 0x01

	// Open non-existent session
	s, err := mgr.Open(contentID)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if s != nil {
		t.Fatalf("expected nil for non-existent session, got %v", s)
	}

	_, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}
	testPeerID, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("failed to get peer ID from public key: %v", err)
	}

	// Create and save
	session := &TransferSession{
		ContentID:   contentID,
		TargetPeer:  testPeerID,
		Completed:   []bool{true},
		TotalChunks: 1,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := mgr.Save(session); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Open saved session
	loaded, err := mgr.Open(contentID)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if loaded == nil || loaded.ContentID != contentID || loaded.Status != StatusInProgress {
		t.Fatalf("unexpected loaded session: %+v", loaded)
	}

	// List sessions
	sessions, err := mgr.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session in list, got %d", len(sessions))
	}

	// Close session
	if err := mgr.Close(contentID); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Delete session
	if err := mgr.Delete(contentID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	sessions, err = mgr.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(sessions) != 0 {
		t.Fatalf("expected 0 sessions after delete, got %d", len(sessions))
	}
}
