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

func TestFileSessionManager_PermissionsAndRemediation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sessionsDir := filepath.Join(tempDir, "sessions")

	// 1. Verify NewFileSessionManager creates directory with 0700 permissions
	sm, err := NewFileSessionManager(sessionsDir)
	if err != nil {
		t.Fatalf("Failed to create FileSessionManager: %v", err)
	}

	info, err := os.Stat(sessionsDir)
	if err != nil {
		t.Fatalf("Failed to stat sessions directory: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("Expected directory permissions 0700, got %o", perm)
	}

	// 2. Save a new session and verify file permissions are 0600
	var cID core.ContentID
	copy(cID[:], []byte("01234567890123456789012345678901"))

	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}
	peerID, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("Failed to get peer ID from private key: %v", err)
	}

	sess := &TransferSession{
		ContentID:   cID,
		TargetPeer:  peerID,
		TotalChunks: 10,
		Completed:   make([]bool, 10),
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := sm.Save(sess); err != nil {
		t.Fatalf("Failed to save session: %v", err)
	}

	jsonPath := sm.getPath(cID)
	fInfo, err := os.Stat(jsonPath)
	if err != nil {
		t.Fatalf("Failed to stat saved session file: %v", err)
	}
	if perm := fInfo.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected session file permissions 0600, got %o", perm)
	}

	// 3. Test legacy directory permission remediation (0755 -> 0700)
	if err := os.Chmod(sessionsDir, 0755); err != nil {
		t.Fatalf("Failed to chmod sessions directory to 0755: %v", err)
	}

	// Re-initialize manager on legacy 0755 dir
	sm2, err := NewFileSessionManager(sessionsDir)
	if err != nil {
		t.Fatalf("Failed to re-initialize FileSessionManager: %v", err)
	}

	info, err = os.Stat(sessionsDir)
	if err != nil {
		t.Fatalf("Failed to stat sessions directory: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("Expected legacy directory to be retroactively updated to 0700, got %o", perm)
	}

	// 4. Test legacy file permission remediation (0644 -> 0600) on Open
	if err := os.Chmod(jsonPath, 0644); err != nil {
		t.Fatalf("Failed to set legacy permissions 0644 on session file: %v", err)
	}

	openSess, err := sm2.Open(cID)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	if openSess == nil {
		t.Fatalf("Expected open session, got nil")
	}

	fInfo, err = os.Stat(jsonPath)
	if err != nil {
		t.Fatalf("Failed to stat session file after Open: %v", err)
	}
	if perm := fInfo.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected session file permission to be retroactively updated to 0600 on Open, got %o", perm)
	}

	// 5. Test legacy file permission remediation (0644 -> 0600) on List
	if err := os.Chmod(jsonPath, 0644); err != nil {
		t.Fatalf("Failed to set legacy permissions 0644 on session file: %v", err)
	}

	// Also create a legacy .tmp file
	tmpPath := jsonPath + ".tmp"
	if err := os.WriteFile(tmpPath, []byte("{}"), 0644); err != nil {
		t.Fatalf("Failed to write legacy tmp file: %v", err)
	}
	if err := os.Chmod(tmpPath, 0644); err != nil {
		t.Fatalf("Failed to set legacy permissions 0644 on tmp file: %v", err)
	}

	sessions, err := sm2.List()
	if err != nil {
		t.Fatalf("Failed to list sessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Errorf("Expected 1 session in List, got %d", len(sessions))
	}

	fInfo, err = os.Stat(jsonPath)
	if err != nil {
		t.Fatalf("Failed to stat session file after List: %v", err)
	}
	if perm := fInfo.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected session file permission to be retroactively updated to 0600 on List, got %o", perm)
	}

	tmpInfo, err := os.Stat(tmpPath)
	if err != nil {
		t.Fatalf("Failed to stat tmp file after List: %v", err)
	}
	if perm := tmpInfo.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected tmp file permission to be retroactively updated to 0600 on List, got %o", perm)
	}

	// Clean up tmp file
	os.Remove(tmpPath)

	// 6. Test Delete
	if err := sm2.Delete(cID); err != nil {
		t.Fatalf("Failed to delete session: %v", err)
	}

	deletedSess, err := sm2.Open(cID)
	if err != nil {
		t.Fatalf("Unexpected error opening deleted session: %v", err)
	}
	if deletedSess != nil {
		t.Errorf("Expected nil for deleted session, got %v", deletedSess)
	}
}
