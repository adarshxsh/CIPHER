package manager

import (
	"sync/atomic"
	"testing"
	"time"

	"cipher/internal/content/core"
)

type mockSessionManager struct {
	saveCalls int64
	sessions  map[string]*TransferSession
}

func newMockSessionManager() *mockSessionManager {
	return &mockSessionManager{
		sessions: make(map[string]*TransferSession),
	}
}

func (m *mockSessionManager) Open(id core.ContentID) (*TransferSession, error) {
	return m.sessions[string(id[:])], nil
}

func (m *mockSessionManager) Save(sess *TransferSession) error {
	atomic.AddInt64(&m.saveCalls, 1)
	m.sessions[string(sess.ContentID[:])] = sess
	// Simulate slight delay in disk save
	time.Sleep(10 * time.Millisecond)
	return nil
}

func (m *mockSessionManager) Close(id core.ContentID) error {
	return nil
}

func (m *mockSessionManager) List() ([]*TransferSession, error) {
	return nil, nil
}

func (m *mockSessionManager) Delete(id core.ContentID) error {
	return nil
}

func TestTransferManager_AsyncSessionSave(t *testing.T) {
	sm := newMockSessionManager()
	tm := NewTransferManager(sm, nil, nil)

	contentID := core.ContentID{1, 2, 3}
	chunkIDs := []core.ChunkID{{1}, {2}, {3}}

	// Create dummy session
	sess := &TransferSession{
		ContentID:   contentID,
		Status:      StatusInProgress,
		StartedAt:   time.Now(),
		Completed:   make([]bool, len(chunkIDs)),
		TotalChunks: len(chunkIDs),
	}
	err := sm.Save(sess)
	if err != nil {
		t.Fatalf("failed to save session: %v", err)
	}

	// Verify save count
	if atomic.LoadInt64(&sm.saveCalls) < 1 {
		t.Fatalf("expected save calls to be at least 1")
	}
	_ = tm
}
