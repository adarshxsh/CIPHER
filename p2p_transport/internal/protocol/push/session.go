package push

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
)

var (
	ErrGlobalCapacityExceeded = errors.New("global push session capacity exceeded")
	ErrPeerQuotaExceeded     = errors.New("per-peer push session quota exceeded")
)

type SessionConfig struct {
	MaxGlobalSessions int
	MaxPeerSessions   int
	SessionTTL        time.Duration
	PruneInterval     time.Duration
}

func DefaultSessionConfig() SessionConfig {
	return SessionConfig{
		MaxGlobalSessions: 100,
		MaxPeerSessions:   10,
		SessionTTL:        5 * time.Minute,
		PruneInterval:     30 * time.Second,
	}
}

type SessionManager struct {
	mu           sync.RWMutex
	sessions     map[core.ContentID]*PendingSession
	peerSessions map[peer.ID]map[core.ContentID]struct{}
	config       SessionConfig
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewSessionManager(config SessionConfig) *SessionManager {
	ctx, cancel := context.WithCancel(context.Background())
	sm := &SessionManager{
		sessions:     make(map[core.ContentID]*PendingSession),
		peerSessions: make(map[peer.ID]map[core.ContentID]struct{}),
		config:       config,
		ctx:          ctx,
		cancel:       cancel,
	}
	sm.startPruner()
	return sm
}

func (sm *SessionManager) startPruner() {
	if sm.config.PruneInterval <= 0 || sm.config.SessionTTL <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(sm.config.PruneInterval)
		defer ticker.Stop()
		for {
			select {
			case <-sm.ctx.Done():
				return
			case <-ticker.C:
				sm.PruneStaleSessions()
			}
		}
	}()
}

func (sm *SessionManager) CreateSession(
	peerID peer.ID,
	contentID core.ContentID,
	m *manifest.Manifest,
	manifestBytes []byte,
	assignedChunkIDs []core.ChunkID,
) (*PendingSession, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// If session already exists for this ContentID
	if existing, exists := sm.sessions[contentID]; exists {
		// If same peer, update/replace existing session without increasing quota count
		if existing.PeerID == peerID {
			expectedMap := make(map[core.ChunkID]struct{}, len(assignedChunkIDs))
			for _, cid := range assignedChunkIDs {
				expectedMap[cid] = struct{}{}
			}
			now := time.Now()
			session := &PendingSession{
				ContentID:       contentID,
				PeerID:          peerID,
				Manifest:        m,
				ManifestBytes:   manifestBytes,
				ExpectedChunks:  expectedMap,
				CommittedChunks: make(map[core.ChunkID]bool),
				StartedAt:       now,
				UpdatedAt:       now,
			}
			sm.sessions[contentID] = session
			return session, nil
		}
		// If a different peer attempts to create session for same content ID, check capacity
	}

	// Check global capacity
	if sm.config.MaxGlobalSessions > 0 && len(sm.sessions) >= sm.config.MaxGlobalSessions {
		return nil, ErrGlobalCapacityExceeded
	}

	// Check per-peer quota
	peerSet := sm.peerSessions[peerID]
	if sm.config.MaxPeerSessions > 0 && peerSet != nil && len(peerSet) >= sm.config.MaxPeerSessions {
		return nil, ErrPeerQuotaExceeded
	}

	expectedMap := make(map[core.ChunkID]struct{}, len(assignedChunkIDs))
	for _, cid := range assignedChunkIDs {
		expectedMap[cid] = struct{}{}
	}

	now := time.Now()
	session := &PendingSession{
		ContentID:       contentID,
		PeerID:          peerID,
		Manifest:        m,
		ManifestBytes:   manifestBytes,
		ExpectedChunks:  expectedMap,
		CommittedChunks: make(map[core.ChunkID]bool),
		StartedAt:       now,
		UpdatedAt:       now,
	}

	sm.sessions[contentID] = session
	if sm.peerSessions[peerID] == nil {
		sm.peerSessions[peerID] = make(map[core.ContentID]struct{})
	}
	sm.peerSessions[peerID][contentID] = struct{}{}

	return session, nil
}

func (sm *SessionManager) GetSession(contentID core.ContentID) (*PendingSession, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	session, exists := sm.sessions[contentID]
	return session, exists
}

func (sm *SessionManager) RecordChunkCommit(contentID core.ContentID, chunkID core.ChunkID) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	session, exists := sm.sessions[contentID]
	if !exists {
		return false
	}

	session.CommittedChunks[chunkID] = true
	session.UpdatedAt = time.Now()
	return true
}

func (sm *SessionManager) RemoveSession(contentID core.ContentID) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	session, exists := sm.sessions[contentID]
	if !exists {
		return false
	}

	delete(sm.sessions, contentID)
	if peerSet, ok := sm.peerSessions[session.PeerID]; ok {
		delete(peerSet, contentID)
		if len(peerSet) == 0 {
			delete(sm.peerSessions, session.PeerID)
		}
	}
	return true
}

func (sm *SessionManager) PruneStaleSessions() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.config.SessionTTL <= 0 {
		return 0
	}

	now := time.Now()
	pruned := 0
	for contentID, session := range sm.sessions {
		if now.Sub(session.UpdatedAt) > sm.config.SessionTTL {
			delete(sm.sessions, contentID)
			if peerSet, ok := sm.peerSessions[session.PeerID]; ok {
				delete(peerSet, contentID)
				if len(peerSet) == 0 {
					delete(sm.peerSessions, session.PeerID)
				}
			}
			log.Printf("[Push Session Manager] Pruned stale push session ContentID %x for peer %s (inactive for %s)",
				contentID, session.PeerID, now.Sub(session.UpdatedAt))
			pruned++
		}
	}
	return pruned
}

func (sm *SessionManager) ActiveSessions() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.sessions)
}

func (sm *SessionManager) PeerSessionCount(peerID peer.ID) int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if peerSet, ok := sm.peerSessions[peerID]; ok {
		return len(peerSet)
	}
	return 0
}

func (sm *SessionManager) Config() SessionConfig {
	return sm.config
}

func (sm *SessionManager) Close() {
	if sm.cancel != nil {
		sm.cancel()
	}
}
