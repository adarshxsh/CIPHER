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

func dummyContentID(b byte) core.ContentID {
	var id core.ContentID
	for i := range id {
		id[i] = b
	}
	return id
}

func dummyPeerID() peer.ID {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		panic(err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		panic(err)
	}
	return pid
}

func TestNewFileSessionManager_PermissionsAndRemediation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sessionDir := filepath.Join(tempDir, "sessions")

	// 1. Verify NewFileSessionManager creates directory with 0700 permissions
	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}
	if sm == nil {
		t.Fatal("expected non-nil SessionManager")
	}

	info, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("failed to stat session dir: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("expected directory mode 0700, got %o", perm)
	}

	// 2. Intentionally loosen directory mode to 0755 and re-init
	if err := os.Chmod(sessionDir, 0755); err != nil {
		t.Fatalf("failed to chmod session dir to 0755: %v", err)
	}
	info, _ = os.Stat(sessionDir)
	if perm := info.Mode().Perm(); perm != 0755 {
		t.Fatalf("setup failed: expected mode 0755, got %o", perm)
	}

	// Re-run NewFileSessionManager and check remediation
	_, err = NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to re-init session manager: %v", err)
	}

	info, err = os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("failed to stat session dir: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("expected remediated directory mode 0700, got %o", perm)
	}
}

func TestFileSessionManager_Save_Permissions(t *testing.T) {
	sessionDir, err := os.MkdirTemp("", "session_save_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(sessionDir)

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	cid := dummyContentID(1)
	session := &TransferSession{
		ContentID:   cid,
		TargetPeer:  dummyPeerID(),
		TotalChunks: 5,
		Completed:   []bool{true, false, true, false, false},
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := sm.Save(session); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	filePath := sm.getPath(cid)
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat saved session file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("expected session file mode 0600, got %o", perm)
	}
}

func TestFileSessionManager_Open_Remediation(t *testing.T) {
	sessionDir, err := os.MkdirTemp("", "session_open_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(sessionDir)

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	cid := dummyContentID(2)
	session := &TransferSession{
		ContentID:   cid,
		TargetPeer:  dummyPeerID(),
		TotalChunks: 3,
		Completed:   []bool{true, true, false},
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	// Save initially
	if err := sm.Save(session); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	filePath := sm.getPath(cid)
	// Manually set insecure mode 0644
	if err := os.Chmod(filePath, 0644); err != nil {
		t.Fatalf("failed to chmod session file: %v", err)
	}

	// Open should audit and remediate mode to 0600
	loaded, err := sm.Open(cid)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected non-nil loaded session")
	}
	if loaded.TotalChunks != 3 {
		t.Errorf("expected TotalChunks 3, got %d", loaded.TotalChunks)
	}

	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat session file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("expected remediated file mode 0600 on Open, got %o", perm)
	}
}

func TestFileSessionManager_List_Remediation(t *testing.T) {
	sessionDir, err := os.MkdirTemp("", "session_list_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(sessionDir)

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	cid1 := dummyContentID(3)
	cid2 := dummyContentID(4)

	session1 := &TransferSession{ContentID: cid1, TargetPeer: dummyPeerID(), TotalChunks: 2, Status: StatusInProgress}
	session2 := &TransferSession{ContentID: cid2, TargetPeer: dummyPeerID(), TotalChunks: 4, Status: StatusCompleted}

	if err := sm.Save(session1); err != nil {
		t.Fatalf("Save session1 failed: %v", err)
	}
	if err := sm.Save(session2); err != nil {
		t.Fatalf("Save session2 failed: %v", err)
	}

	// Set both session files to insecure mode 0644
	file1 := sm.getPath(cid1)
	file2 := sm.getPath(cid2)
	if err := os.Chmod(file1, 0644); err != nil {
		t.Fatalf("chmod file1 failed: %v", err)
	}
	if err := os.Chmod(file2, 0644); err != nil {
		t.Fatalf("chmod file2 failed: %v", err)
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(sessions) != 2 {
		t.Fatalf("expected 2 sessions from List, got %d", len(sessions))
	}

	// Verify both files were remediated to 0600
	for _, f := range []string{file1, file2} {
		info, err := os.Stat(f)
		if err != nil {
			t.Fatalf("failed to stat %s: %v", f, err)
		}
		if perm := info.Mode().Perm(); perm != 0600 {
			t.Errorf("file %s expected remediated mode 0600, got %o", f, perm)
		}
	}
}

func TestFileSessionManager_OrphanedTmpCleanup(t *testing.T) {
	sessionDir, err := os.MkdirTemp("", "session_tmp_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(sessionDir)

	// Create an orphaned .tmp file prior to initialization
	orphanedTmp := filepath.Join(sessionDir, "orphaned.json.tmp")
	if err := os.MkdirAll(sessionDir, 0700); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	if err := os.WriteFile(orphanedTmp, []byte("{}"), 0644); err != nil {
		t.Fatalf("failed to create orphaned tmp file: %v", err)
	}

	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	// Verify orphaned .tmp file was deleted during init
	if _, err := os.Stat(orphanedTmp); !os.IsNotExist(err) {
		t.Errorf("expected orphaned tmp file to be deleted during init, but it exists")
	}

	// Create another orphaned .tmp file and test cleanup during List()
	orphanedTmp2 := filepath.Join(sessionDir, "orphaned2.json.tmp")
	if err := os.WriteFile(orphanedTmp2, []byte("{}"), 0644); err != nil {
		t.Fatalf("failed to create second orphaned tmp file: %v", err)
	}

	if _, err := sm.List(); err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if _, err := os.Stat(orphanedTmp2); !os.IsNotExist(err) {
		t.Errorf("expected orphaned tmp file to be deleted during List, but it exists")
	}
}
