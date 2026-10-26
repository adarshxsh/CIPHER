package push

import (
	"context"
	"io"
	"log"
	"sync"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/verifier"
	"cipher/internal/discovery"
	"cipher/internal/protocol"
)

type AuthPolicy string

const (
	AuthPolicyOpen      AuthPolicy = "open"
	AuthPolicyAllowlist AuthPolicy = "allowlist"

	MaxGlobalSessions = 100
	MaxPeerSessions   = 5
	SessionTTL        = 15 * time.Minute
	ReaperInterval    = 1 * time.Minute
)

type Option func(*StreamHandler)

func WithMaxGlobalSessions(n int) Option {
	return func(h *StreamHandler) {
		if n > 0 {
			h.maxGlobalSessions = n
		}
	}
}

func WithMaxPeerSessions(n int) Option {
	return func(h *StreamHandler) {
		if n > 0 {
			h.maxPeerSessions = n
		}
	}
}

func WithSessionTTL(d time.Duration) Option {
	return func(h *StreamHandler) {
		if d > 0 {
			h.sessionTTL = d
		}
	}
}

func WithReaperInterval(d time.Duration) Option {
	return func(h *StreamHandler) {
		if d >= 0 {
			h.reaperInterval = d
		}
	}
}

type PendingSession struct {
	ContentID       core.ContentID
	PeerID          peer.ID
	Manifest        *manifest.Manifest
	ManifestBytes   []byte
	ExpectedChunks  map[core.ChunkID]struct{}
	CommittedChunks map[core.ChunkID]bool
	StartedAt       time.Time
	UpdatedAt       time.Time
}

type StreamHandler struct {
	host              host.Host
	engine            *engine.ContentEngine
	kdht              *dht.IpfsDHT
	digest            core.Digest
	allowPush         bool
	authPolicy        AuthPolicy
	allowedPublishers map[peer.ID]struct{}

	maxGlobalSessions int
	maxPeerSessions   int
	sessionTTL        time.Duration
	reaperInterval    time.Duration

	sessionsMu   sync.RWMutex
	sessions     map[core.ContentID]*PendingSession
	peerSessions map[peer.ID]int

	closeOnce      sync.Once
	reaperStopChan chan struct{}
	reaperDoneChan chan struct{}
}

func NewStreamHandler(
	h host.Host,
	eng *engine.ContentEngine,
	kdht *dht.IpfsDHT,
	allowPush bool,
	authPolicy AuthPolicy,
	allowedPublishers []peer.ID,
	opts ...Option,
) *StreamHandler {
	allowedMap := make(map[peer.ID]struct{})
	for _, pid := range allowedPublishers {
		allowedMap[pid] = struct{}{}
	}

	handler := &StreamHandler{
		host:              h,
		engine:            eng,
		kdht:              kdht,
		digest:            verifier.NewSHA256Digest(),
		allowPush:         allowPush,
		authPolicy:        authPolicy,
		allowedPublishers: allowedMap,
		maxGlobalSessions: MaxGlobalSessions,
		maxPeerSessions:   MaxPeerSessions,
		sessionTTL:        SessionTTL,
		reaperInterval:    ReaperInterval,
		sessions:          make(map[core.ContentID]*PendingSession),
		peerSessions:      make(map[peer.ID]int),
		reaperStopChan:    make(chan struct{}),
		reaperDoneChan:    make(chan struct{}),
	}

	for _, opt := range opts {
		opt(handler)
	}

	if handler.reaperInterval > 0 {
		go handler.runReaper()
	} else {
		close(handler.reaperDoneChan)
	}

	if h != nil {
		h.SetStreamHandler(protocol.PushTransportProtocolID, handler.handleStream)
	}
	return handler
}

func (h *StreamHandler) Close() error {
	h.closeOnce.Do(func() {
		if h.reaperInterval > 0 {
			close(h.reaperStopChan)
			<-h.reaperDoneChan
		}
	})
	return nil
}

func (h *StreamHandler) runReaper() {
	defer close(h.reaperDoneChan)
	ticker := time.NewTicker(h.reaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.reaperStopChan:
			return
		case now := <-ticker.C:
			h.reapExpiredSessions(now)
		}
	}
}

func (h *StreamHandler) reapExpiredSessions(now time.Time) {
	h.sessionsMu.Lock()
	defer h.sessionsMu.Unlock()

	for contentID, session := range h.sessions {
		if now.Sub(session.UpdatedAt) >= h.sessionTTL {
			log.Printf("[Push Protocol] Evicting expired push session for content %x (peer %s, inactive for %v)",
				contentID, session.PeerID, now.Sub(session.UpdatedAt))
			delete(h.sessions, contentID)
			h.decrementPeerSessionLocked(session.PeerID)
		}
	}
}

func (h *StreamHandler) decrementPeerSessionLocked(pid peer.ID) {
	if count, ok := h.peerSessions[pid]; ok {
		if count <= 1 {
			delete(h.peerSessions, pid)
		} else {
			h.peerSessions[pid] = count - 1
		}
	}
}

func (h *StreamHandler) removeSessionLocked(contentID core.ContentID) *PendingSession {
	session, exists := h.sessions[contentID]
	if !exists {
		return nil
	}
	delete(h.sessions, contentID)
	h.decrementPeerSessionLocked(session.PeerID)
	return session
}

func (h *StreamHandler) ActiveSessionsCount() int {
	h.sessionsMu.RLock()
	defer h.sessionsMu.RUnlock()
	return len(h.sessions)
}

func (h *StreamHandler) PeerSessionsCount(pid peer.ID) int {
	h.sessionsMu.RLock()
	defer h.sessionsMu.RUnlock()
	return h.peerSessions[pid]
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	remotePeer := s.Conn().RemotePeer()

	// 1. Authorization check
	if !h.allowPush {
		log.Printf("[Push Protocol] Ingestion rejected from %s: push disabled (-allow-push=false)", remotePeer)
		_ = WritePushMessage(s, BuildPushError(PushStatusUnauthorized, "provider push disabled"))
		return
	}

	if h.authPolicy == AuthPolicyAllowlist {
		if _, ok := h.allowedPublishers[remotePeer]; !ok {
			log.Printf("[Push Protocol] Ingestion rejected from unauthorized publisher: %s", remotePeer)
			_ = WritePushMessage(s, BuildPushError(PushStatusUnauthorized, "publisher not in allowlist"))
			return
		}
	}

	log.Printf("[Push Protocol] Accepted push stream from %s", remotePeer)

	for {
		_ = s.SetReadDeadline(time.Now().Add(ReadTimeout))
		msg, err := ReadPushMessage(s)
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Push Protocol] Push stream closed by %s", remotePeer)
				return
			}
			log.Printf("[Push Protocol] Error reading message: %v", err)
			return
		}
		_ = s.SetReadDeadline(time.Time{})

		if msg.Version != CurrentPushVersion {
			log.Printf("[Push Protocol] Unsupported message version: %d", msg.Version)
			_ = WritePushMessage(s, BuildPushError(PushStatusMalformed, "unsupported version"))
			return
		}

		switch msg.Type {
		case MsgPushManifest:
			h.handlePushManifest(s, msg)
		case MsgPushChunk:
			h.handlePushChunk(s, msg)
		case MsgPushBatchComplete:
			h.handlePushBatchComplete(s, msg)
		default:
			log.Printf("[Push Protocol] Unsupported message type: %d", msg.Type)
			_ = WritePushMessage(s, BuildPushError(PushStatusMalformed, "unknown message type"))
			return
		}
	}
}

func (h *StreamHandler) handlePushManifest(s network.Stream, msg *PushMessage) {
	remotePeer := s.Conn().RemotePeer()

	contentID, assignedChunkIDs, manifestData, err := ParsePushManifest(msg.Payload)
	if err != nil {
		log.Printf("[Push Protocol] Failed to parse PUSH_MANIFEST: %v", err)
		_ = WritePushMessage(s, BuildPushError(PushStatusMalformed, "malformed manifest payload"))
		return
	}

	m, err := manifest.Deserialize(manifestData)
	if err != nil {
		log.Printf("[Push Protocol] Failed to deserialize manifest JSON: %v", err)
		_ = WritePushMessage(s, BuildPushError(PushStatusMalformed, "invalid manifest JSON"))
		return
	}

	if m.Descriptor.ID != contentID {
		log.Printf("[Push Protocol] Manifest contentID mismatch: %x vs %x", m.Descriptor.ID, contentID)
		_ = WritePushMessage(s, BuildPushError(PushStatusMalformed, "contentID mismatch"))
		return
	}

	expectedMap := make(map[core.ChunkID]struct{})
	for _, cid := range assignedChunkIDs {
		expectedMap[cid] = struct{}{}
	}

	h.sessionsMu.Lock()
	existingSession, exists := h.sessions[contentID]
	if exists {
		if existingSession.PeerID != remotePeer && h.peerSessions[remotePeer] >= h.maxPeerSessions {
			h.sessionsMu.Unlock()
			log.Printf("[Push Protocol] Ingestion rejected from %s: per-peer session limit (%d) reached", remotePeer, h.maxPeerSessions)
			_ = WritePushMessage(s, BuildPushError(PushStatusQuotaExceeded, "per-peer session limit reached"))
			return
		}
		h.removeSessionLocked(contentID)
	} else {
		if len(h.sessions) >= h.maxGlobalSessions {
			h.sessionsMu.Unlock()
			log.Printf("[Push Protocol] Ingestion rejected from %s: global session limit (%d) reached", remotePeer, h.maxGlobalSessions)
			_ = WritePushMessage(s, BuildPushError(PushStatusQuotaExceeded, "global session limit reached"))
			return
		}
		if h.peerSessions[remotePeer] >= h.maxPeerSessions {
			h.sessionsMu.Unlock()
			log.Printf("[Push Protocol] Ingestion rejected from %s: per-peer session limit (%d) reached", remotePeer, h.maxPeerSessions)
			_ = WritePushMessage(s, BuildPushError(PushStatusQuotaExceeded, "per-peer session limit reached"))
			return
		}
	}

	h.sessions[contentID] = &PendingSession{
		ContentID:       contentID,
		PeerID:          remotePeer,
		Manifest:        m,
		ManifestBytes:   manifestData,
		ExpectedChunks:  expectedMap,
		CommittedChunks: make(map[core.ChunkID]bool),
		StartedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}
	h.peerSessions[remotePeer]++
	h.sessionsMu.Unlock()

	log.Printf("[Push Protocol] Initialized push session for ContentID %x (expecting %d chunks)", contentID, len(expectedMap))

	ack := BuildPushManifestAck(contentID, PushStatusOK)
	if err := WritePushMessage(s, ack); err != nil {
		log.Printf("[Push Protocol] Failed to write PUSH_MANIFEST_ACK: %v", err)
	}
}

func (h *StreamHandler) handlePushChunk(s network.Stream, msg *PushMessage) {
	contentID, chunk, err := ParsePushChunk(msg.Payload)
	if err != nil {
		log.Printf("[Push Protocol] Failed to parse PUSH_CHUNK: %v", err)
		_ = WritePushMessage(s, BuildPushError(PushStatusMalformed, "malformed chunk payload"))
		return
	}

	chunkID := chunk.Header.ID

	h.sessionsMu.RLock()
	session, exists := h.sessions[contentID]
	h.sessionsMu.RUnlock()

	if !exists {
		log.Printf("[Push Protocol] Chunk %x received for unknown/non-pending content %x", chunkID, contentID)
		_ = WritePushMessage(s, BuildPushChunkAck(chunkID, PushStatusNotInAssignedSet))
		return
	}

	// 1. Validate chunk is in assigned set
	if _, ok := session.ExpectedChunks[chunkID]; !ok {
		log.Printf("[Push Protocol] Chunk %x not in assigned set for ContentID %x", chunkID, contentID)
		_ = WritePushMessage(s, BuildPushChunkAck(chunkID, PushStatusNotInAssignedSet))
		return
	}

	// 2. Validate chunk belongs to manifest
	foundInManifest := false
	for _, cid := range session.Manifest.ChunkIDs {
		if cid == chunkID {
			foundInManifest = true
			break
		}
	}
	if !foundInManifest {
		log.Printf("[Push Protocol] Chunk %x not listed in manifest for ContentID %x", chunkID, contentID)
		_ = WritePushMessage(s, BuildPushChunkAck(chunkID, PushStatusMalformed))
		return
	}

	// 3. Verify SHA-256(ciphertext) matches ChunkID
	computedHash := h.digest.Sum(chunk.Data)
	if computedHash != core.Hash(chunkID) {
		log.Printf("[Push Protocol] Checksum mismatch for chunk %x", chunkID)
		_ = WritePushMessage(s, BuildPushChunkAck(chunkID, PushStatusHashMismatch))
		return
	}

	// 4. Idempotent commit: check if already exists in CAS
	ctx := context.Background()
	has, _ := h.engine.HasChunk(ctx, chunkID)
	if !has {
		if err := h.engine.PutChunk(ctx, chunk); err != nil {
			log.Printf("[Push Protocol] Failed to store chunk %x: %v", chunkID, err)
			_ = WritePushMessage(s, BuildPushChunkAck(chunkID, PushStatusIOError))
			return
		}
	}

	h.sessionsMu.Lock()
	session.CommittedChunks[chunkID] = true
	session.UpdatedAt = time.Now()
	h.sessionsMu.Unlock()

	ack := BuildPushChunkAck(chunkID, PushStatusOK)
	if err := WritePushMessage(s, ack); err != nil {
		log.Printf("[Push Protocol] Failed to write PUSH_CHUNK_ACK: %v", err)
	}
}

func (h *StreamHandler) handlePushBatchComplete(s network.Stream, msg *PushMessage) {
	contentID, err := ParsePushBatchComplete(msg.Payload)
	if err != nil {
		log.Printf("[Push Protocol] Failed to parse PUSH_BATCH_COMPLETE: %v", err)
		_ = WritePushMessage(s, BuildPushError(PushStatusMalformed, "malformed batch complete payload"))
		return
	}

	h.sessionsMu.Lock()
	session, exists := h.sessions[contentID]
	if !exists {
		h.sessionsMu.Unlock()
		log.Printf("[Push Protocol] BatchComplete requested for non-pending content %x", contentID)
		_ = WritePushMessage(s, BuildPushBatchCompleteAck(contentID, PushStatusIncomplete))
		return
	}

	// Invariant check: Did we commit ALL assigned chunks?
	allCommitted := true
	for cid := range session.ExpectedChunks {
		if !session.CommittedChunks[cid] {
			allCommitted = false
			break
		}
	}

	if !allCommitted || len(session.CommittedChunks) < len(session.ExpectedChunks) {
		h.sessionsMu.Unlock()
		log.Printf("[Push Protocol] BatchComplete rejected for %x: committed %d/%d assigned chunks",
			contentID, len(session.CommittedChunks), len(session.ExpectedChunks))
		_ = WritePushMessage(s, BuildPushBatchCompleteAck(contentID, PushStatusIncomplete))
		return
	}

	// Commit manifest to local CAS
	ctx := context.Background()
	if err := h.engine.PutManifestBytes(ctx, contentID, session.ManifestBytes); err != nil {
		h.sessionsMu.Unlock()
		log.Printf("[Push Protocol] Failed to store manifest for %x: %v", contentID, err)
		_ = WritePushMessage(s, BuildPushBatchCompleteAck(contentID, PushStatusIOError))
		return
	}

	// Remove session from pending
	h.removeSessionLocked(contentID)
	h.sessionsMu.Unlock()

	log.Printf("[Push Protocol] Content %x successfully committed to CAS (all %d assigned chunks verified)",
		contentID, len(session.ExpectedChunks))

	// Announce to DHT
	if h.kdht != nil {
		go func() {
			dhtCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := discovery.Provide(dhtCtx, h.kdht, contentID); err != nil {
				log.Printf("[Push Protocol] Warning: Failed to announce %x on DHT: %v", contentID, err)
			} else {
				log.Printf("[Push Protocol] [✓] Successfully announced ContentID %x on DHT", contentID)
			}
		}()
	}

	ack := BuildPushBatchCompleteAck(contentID, PushStatusOK)
	if err := WritePushMessage(s, ack); err != nil {
		log.Printf("[Push Protocol] Failed to write PUSH_BATCH_COMPLETE_ACK: %v", err)
	}
}
