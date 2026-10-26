package manager

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func generatePeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("failed to get peer ID: %v", err)
	}
	return pid
}

func TestNewFileSessionManager_DirectoryPermissions(t *testing.T) {
	tempDir := t.TempDir()
	sessionDir := filepath.Join(tempDir, "sessions")

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}
	_ = sm

	info, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("os.Stat failed: %v", err)
	}

	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("expected session directory mode 0700, got %o", perm)
	}
}

func TestNewFileSessionManager_RemediateExistingDirectory(t *testing.T) {
	tempDir := t.TempDir()
	sessionDir := filepath.Join(tempDir, "sessions")

	if err := os.MkdirAll(sessionDir, 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}
	if err := os.Chmod(sessionDir, 0755); err != nil {
		t.Fatalf("failed to chmod dir: %v", err)
	}

	info, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("os.Stat failed: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0755 {
		t.Fatalf("setup failed: expected 0755, got %o", perm)
	}

	_, err = NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	info, err = os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("os.Stat failed: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("expected remediated directory mode 0700, got %o", perm)
	}
}

func TestSave_FilePermissions(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	contentID := core.ContentID{0x01, 0x02, 0x03}
	session := &TransferSession{
		ContentID:   contentID,
		TargetPeer:  generatePeerID(t),
		Completed:   []bool{true, false},
		TotalChunks: 2,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := sm.Save(session); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	sessionFilePath := sm.getPath(contentID)
	info, err := os.Stat(sessionFilePath)
	if err != nil {
		t.Fatalf("os.Stat failed: %v", err)
	}

	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("expected session file mode 0600, got %o", perm)
	}
}

func TestRemediateExistingStateFiles(t *testing.T) {
	tempDir := t.TempDir()

	// Pre-create directory with 0755
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}
	_ = os.Chmod(tempDir, 0755)

	// Pre-create legacy state files with 0644
	jsonPath := filepath.Join(tempDir, "010203.json")
	tmpPath := filepath.Join(tempDir, "010203.json.tmp")

	pid := generatePeerID(t)
	dummySession := &TransferSession{
		ContentID:   core.ContentID{0x01, 0x02, 0x03},
		TargetPeer:  pid,
		Status:      StatusPaused,
		TotalChunks: 5,
	}
	dummyJSON, err := json.Marshal(dummySession)
	if err != nil {
		t.Fatalf("failed to marshal dummy session: %v", err)
	}

	if err := os.WriteFile(jsonPath, dummyJSON, 0644); err != nil {
		t.Fatalf("failed to write legacy json: %v", err)
	}
	_ = os.Chmod(jsonPath, 0644)

	if err := os.WriteFile(tmpPath, dummyJSON, 0644); err != nil {
		t.Fatalf("failed to write legacy tmp: %v", err)
	}
	_ = os.Chmod(tmpPath, 0644)

	// Verify legacy setup
	if info, _ := os.Stat(jsonPath); info.Mode().Perm() != 0644 {
		t.Fatalf("setup failed for json: expected 0644, got %o", info.Mode().Perm())
	}
	if info, _ := os.Stat(tmpPath); info.Mode().Perm() != 0644 {
		t.Fatalf("setup failed for tmp: expected 0644, got %o", info.Mode().Perm())
	}

	// Initialize manager
	_, err = NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	// Verify remediation
	if info, err := os.Stat(jsonPath); err != nil || info.Mode().Perm() != 0600 {
		t.Errorf("expected json file mode 0600, got %o (err: %v)", info.Mode().Perm(), err)
	}
	if info, err := os.Stat(tmpPath); err != nil || info.Mode().Perm() != 0600 {
		t.Errorf("expected tmp file mode 0600, got %o (err: %v)", info.Mode().Perm(), err)
	}
}

func TestOpenAndList_RemediatePermissions(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	contentID := core.ContentID{0xaa, 0xbb}
	session := &TransferSession{
		ContentID:   contentID,
		TargetPeer:  generatePeerID(t),
		Completed:   []bool{true},
		TotalChunks: 1,
		StartedAt:   time.Now(),
		Status:      StatusCompleted,
	}

	if err := sm.Save(session); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	filePath := sm.getPath(contentID)
	// Change permission back to 0644 to simulate legacy file modification
	if err := os.Chmod(filePath, 0644); err != nil {
		t.Fatalf("Chmod failed: %v", err)
	}

	// Test Open remediates to 0600
	openedSession, err := sm.Open(contentID)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if openedSession == nil || openedSession.Status != StatusCompleted {
		t.Fatalf("Open returned invalid session")
	}

	if info, err := os.Stat(filePath); err != nil || info.Mode().Perm() != 0600 {
		t.Errorf("Open did not remediate file permissions to 0600, got %o", info.Mode().Perm())
	}

	// Change permission back to 0644 for List test
	if err := os.Chmod(filePath, 0644); err != nil {
		t.Fatalf("Chmod failed: %v", err)
	}

	// Test List remediates to 0600
	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session from List, got %d", len(sessions))
	}

	if info, err := os.Stat(filePath); err != nil || info.Mode().Perm() != 0600 {
		t.Errorf("List did not remediate file permissions to 0600, got %o", info.Mode().Perm())
	}
}

func TestSessionManager_CRUD(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	contentID := core.ContentID{0xde, 0xad, 0xbe, 0xef}

	// Open non-existent
	s, err := sm.Open(contentID)
	if err != nil {
		t.Fatalf("Open non-existent failed: %v", err)
	}
	if s != nil {
		t.Fatalf("expected nil for non-existent session, got %v", s)
	}

	// Save
	session := &TransferSession{
		ContentID:   contentID,
		TargetPeer:  generatePeerID(t),
		Completed:   []bool{false, true},
		TotalChunks: 2,
		StartedAt:   time.Now(),
		Status:      StatusPaused,
	}
	if err := sm.Save(session); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Open
	s, err = sm.Open(contentID)
	if err != nil || s == nil {
		t.Fatalf("Open after save failed: %v, session: %v", err, s)
	}
	if s.Status != StatusPaused || s.CompletedCount() != 1 {
		t.Errorf("session content mismatch: %+v", s)
	}

	// List
	list, err := sm.List()
	if err != nil || len(list) != 1 {
		t.Fatalf("List failed: err=%v, count=%d", err, len(list))
	}

	// Close (no-op)
	if err := sm.Close(contentID); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Delete
	if err := sm.Delete(contentID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Open after delete
	s, err = sm.Open(contentID)
	if err != nil || s != nil {
		t.Fatalf("expected nil after delete, got %v, err=%v", s, err)
	}
}
