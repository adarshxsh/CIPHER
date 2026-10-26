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
	pid, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("failed to get peer ID from public key: %v", err)
	}
	return pid
}

func TestFileSessionManager_Permissions(t *testing.T) {
	tmpDir := t.TempDir()
	sessionDir := filepath.Join(tmpDir, "sessions")

	// 1. Verify NewFileSessionManager creates new directory with 0700 permissions
	sm, err := NewFileSessionManager(sessionDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	dirInfo, err := os.Stat(sessionDir)
	if err != nil {
		t.Fatalf("failed to stat session dir: %v", err)
	}
	if perm := dirInfo.Mode().Perm(); perm != 0700 {
		t.Errorf("expected dir mode 0700, got %o", perm)
	}

	// 2. Verify NewFileSessionManager updates existing 0755 directory to 0700
	existingDir := filepath.Join(tmpDir, "existing_sessions")
	if err := os.MkdirAll(existingDir, 0755); err != nil {
		t.Fatalf("failed to create existing dir: %v", err)
	}
	if err := os.Chmod(existingDir, 0755); err != nil {
		t.Fatalf("failed to chmod existing dir: %v", err)
	}

	sm2, err := NewFileSessionManager(existingDir)
	if err != nil {
		t.Fatalf("failed to initialize session manager on existing dir: %v", err)
	}

	dirInfo2, err := os.Stat(existingDir)
	if err != nil {
		t.Fatalf("failed to stat existing session dir: %v", err)
	}
	if perm := dirInfo2.Mode().Perm(); perm != 0700 {
		t.Errorf("expected existing dir mode updated to 0700, got %o", perm)
	}

	// 3. Verify Save creates files with 0600 permissions
	var cid core.ContentID
	cid[0] = 0xab
	cid[1] = 0xcd

	sess := &TransferSession{
		ContentID:   cid,
		TargetPeer:  generateTestPeerID(t),
		Completed:   []bool{true, false},
		TotalChunks: 2,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	if err := sm2.Save(sess); err != nil {
		t.Fatalf("failed to save session: %v", err)
	}

	filePath := sm2.getPath(cid)
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat session file: %v", err)
	}
	if perm := fileInfo.Mode().Perm(); perm != 0600 {
		t.Errorf("expected saved file mode 0600, got %o", perm)
	}

	// 4. Verify Open retroactively updates pre-existing 0644 file to 0600
	var cid2 core.ContentID
	cid2[0] = 0x12
	cid2[1] = 0x34

	sess2 := &TransferSession{
		ContentID:   cid2,
		TargetPeer:  generateTestPeerID(t),
		Completed:   []bool{true},
		TotalChunks: 1,
		StartedAt:   time.Now(),
		Status:      StatusCompleted,
	}

	if err := sm2.Save(sess2); err != nil {
		t.Fatalf("failed to save session2: %v", err)
	}

	filePath2 := sm2.getPath(cid2)
	// Force permission to 0644 to simulate old file
	if err := os.Chmod(filePath2, 0644); err != nil {
		t.Fatalf("failed to set 0644 mode on session file: %v", err)
	}

	// Calling Open should retroactively update file mode to 0600
	openedSess, err := sm2.Open(cid2)
	if err != nil {
		t.Fatalf("failed to open session: %v", err)
	}
	if openedSess == nil {
		t.Fatalf("expected non-nil session from Open")
	}

	fileInfo2, err := os.Stat(filePath2)
	if err != nil {
		t.Fatalf("failed to stat file after Open: %v", err)
	}
	if perm := fileInfo2.Mode().Perm(); perm != 0600 {
		t.Errorf("expected file mode updated to 0600 on Open, got %o", perm)
	}

	// 5. Verify List retroactively updates pre-existing 0644 file to 0600
	var cid3 core.ContentID
	cid3[0] = 0x56
	cid3[1] = 0x78

	sess3 := &TransferSession{
		ContentID:   cid3,
		TargetPeer:  generateTestPeerID(t),
		Completed:   []bool{false},
		TotalChunks: 1,
		StartedAt:   time.Now(),
		Status:      StatusPaused,
	}

	if err := sm2.Save(sess3); err != nil {
		t.Fatalf("failed to save session3: %v", err)
	}

	filePath3 := sm2.getPath(cid3)
	if err := os.Chmod(filePath3, 0644); err != nil {
		t.Fatalf("failed to set 0644 mode on session file: %v", err)
	}

	listSesses, err := sm2.List()
	if err != nil {
		t.Fatalf("failed to list sessions: %v", err)
	}
	if len(listSesses) < 3 {
		t.Errorf("expected at least 3 sessions listed, got %d", len(listSesses))
	}

	fileInfo3, err := os.Stat(filePath3)
	if err != nil {
		t.Fatalf("failed to stat file after List: %v", err)
	}
	if perm := fileInfo3.Mode().Perm(); perm != 0600 {
		t.Errorf("expected file mode updated to 0600 on List, got %o", perm)
	}

	_ = sm
}
