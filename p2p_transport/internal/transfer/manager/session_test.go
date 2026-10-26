package manager

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/peer"
)

var dummyPeerID peer.ID

func init() {
	pid, err := peer.Decode("12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN")
	if err != nil {
		panic(err)
	}
	dummyPeerID = pid
}

func TestNewFileSessionManager_DirectoryPermissions(t *testing.T) {
	tempDir := t.TempDir()
	sessDir := filepath.Join(tempDir, "new_sessions")

	// 1. Verify NewFileSessionManager creates new directory with 0700
	sm, err := NewFileSessionManager(sessDir)
	if err != nil {
		t.Fatalf("failed to create NewFileSessionManager: %v", err)
	}
	if sm == nil {
		t.Fatalf("expected non-nil FileSessionManager")
	}

	info, err := os.Stat(sessDir)
	if err != nil {
		t.Fatalf("failed to stat session dir: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0700 {
		t.Errorf("expected new dir permissions 0700, got %o", perm)
	}

	// 2. Verify NewFileSessionManager remediates pre-existing dir with 0755
	preExistingDir := filepath.Join(tempDir, "existing_sessions")
	if err := os.MkdirAll(preExistingDir, 0755); err != nil {
		t.Fatalf("failed to create pre-existing dir: %v", err)
	}
	if err := os.Chmod(preExistingDir, 0755); err != nil {
		t.Fatalf("failed to set pre-existing dir chmod: %v", err)
	}

	preInfo, err := os.Stat(preExistingDir)
	if err != nil || preInfo.Mode().Perm() != 0755 {
		t.Fatalf("expected pre-existing dir mode 0755, got %o", preInfo.Mode().Perm())
	}

	_, err = NewFileSessionManager(preExistingDir)
	if err != nil {
		t.Fatalf("failed to initialize on pre-existing dir: %v", err)
	}

	remediatedInfo, err := os.Stat(preExistingDir)
	if err != nil {
		t.Fatalf("failed to stat remediated dir: %v", err)
	}
	if perm := remediatedInfo.Mode().Perm(); perm != 0700 {
		t.Errorf("expected remediated dir permissions 0700, got %o", perm)
	}
}

func TestNewFileSessionManager_RemediatesExistingFiles(t *testing.T) {
	tempDir := t.TempDir()

	// Create files with open 0644 permissions before manager initialization
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	jsonPath := filepath.Join(tempDir, "session1.json")
	tmpPath := filepath.Join(tempDir, "session2.json.tmp")

	if err := os.WriteFile(jsonPath, []byte("{}"), 0644); err != nil {
		t.Fatalf("failed to write json file: %v", err)
	}
	if err := os.Chmod(jsonPath, 0644); err != nil {
		t.Fatalf("chmod json failed: %v", err)
	}

	if err := os.WriteFile(tmpPath, []byte("{}"), 0644); err != nil {
		t.Fatalf("failed to write tmp file: %v", err)
	}
	if err := os.Chmod(tmpPath, 0644); err != nil {
		t.Fatalf("chmod tmp failed: %v", err)
	}

	_, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to initialize manager: %v", err)
	}

	infoJSON, err := os.Stat(jsonPath)
	if err != nil {
		t.Fatalf("stat json error: %v", err)
	}
	if perm := infoJSON.Mode().Perm(); perm != 0600 {
		t.Errorf("expected json permissions 0600 after startup remediation, got %o", perm)
	}

	infoTMP, err := os.Stat(tmpPath)
	if err != nil {
		t.Fatalf("stat tmp error: %v", err)
	}
	if perm := infoTMP.Mode().Perm(); perm != 0600 {
		t.Errorf("expected tmp permissions 0600 after startup remediation, got %o", perm)
	}
}

func TestSave_FilePermissions(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager error: %v", err)
	}

	contentID := core.ContentID{0x01, 0x02, 0x03}
	sess := &TransferSession{
		ContentID:   contentID,
		TargetPeer:  dummyPeerID,
		Status:      StatusInProgress,
		StartedAt:   time.Now(),
		TotalChunks: 5,
		Completed:   []bool{true, false, false, false, false},
	}

	if err := sm.Save(sess); err != nil {
		t.Fatalf("Save error: %v", err)
	}

	path := sm.getPath(contentID)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat saved file error: %v", err)
	}

	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("expected saved file permissions 0600, got %o", perm)
	}
}

func TestOpen_RemediatesLegacyPermissions(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager error: %v", err)
	}

	contentID := core.ContentID{0x0a, 0x0b, 0x0c}
	sess := &TransferSession{
		ContentID:   contentID,
		TargetPeer:  dummyPeerID,
		Status:      StatusInProgress,
		StartedAt:   time.Now(),
		TotalChunks: 2,
	}

	if err := sm.Save(sess); err != nil {
		t.Fatalf("Save error: %v", err)
	}

	path := sm.getPath(contentID)
	// Manually change permissions to legacy 0644
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatalf("chmod error: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0644 {
		t.Fatalf("expected mode 0644 before Open, got %o", info.Mode().Perm())
	}

	// Call Open
	res, err := sm.Open(contentID)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	if res == nil {
		t.Fatalf("expected non-nil session from Open")
	}

	// Verify permission was remediated to 0600
	remediatedInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat error after Open: %v", err)
	}
	if perm := remediatedInfo.Mode().Perm(); perm != 0600 {
		t.Errorf("expected file permissions 0600 after Open, got %o", perm)
	}
}

func TestList_RemediatesLegacyPermissions(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager error: %v", err)
	}

	id1 := core.ContentID{0x10}
	id2 := core.ContentID{0x20}

	_ = sm.Save(&TransferSession{ContentID: id1, TargetPeer: dummyPeerID, Status: StatusInProgress})
	_ = sm.Save(&TransferSession{ContentID: id2, TargetPeer: dummyPeerID, Status: StatusCompleted})

	p1 := sm.getPath(id1)
	p2 := sm.getPath(id2)

	_ = os.Chmod(p1, 0644)
	_ = os.Chmod(p2, 0644)

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if len(sessions) != 2 {
		t.Errorf("expected 2 sessions, got %d", len(sessions))
	}

	for _, p := range []string{p1, p2} {
		info, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat error: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0600 {
			t.Errorf("expected file %s permissions 0600 after List, got %o", p, perm)
		}
	}
}

func TestDeleteAndClose(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager error: %v", err)
	}

	id := core.ContentID{0x30}
	_ = sm.Save(&TransferSession{ContentID: id, TargetPeer: dummyPeerID, Status: StatusInProgress})

	if err := sm.Close(id); err != nil {
		t.Errorf("Close returned unexpected error: %v", err)
	}

	if err := sm.Delete(id); err != nil {
		t.Errorf("Delete returned unexpected error: %v", err)
	}

	res, err := sm.Open(id)
	if err != nil {
		t.Errorf("Open after Delete returned error: %v", err)
	}
	if res != nil {
		t.Errorf("expected nil session after Delete")
	}
}
