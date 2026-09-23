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
)

type PendingSession struct {
	ContentID       core.ContentID
	PublisherPeer   peer.ID
	Manifest        *manifest.Manifest
	ManifestBytes   []byte
	ExpectedChunks  map[core.ChunkID]struct{}
	CommittedChunks map[core.ChunkID]bool
	StartedAt       time.Time
	UpdatedAt       time.Time
}

type StreamHandlerConfig struct {
	MaxPendingSessions int
	MaxSessionsPerPeer int
	SessionTTL         time.Duration
	SessionIdleTimeout time.Duration
	GCTickerInterval   time.Duration
}

var DefaultConfig = StreamHandlerConfig{
	MaxPendingSessions: 100,
	MaxSessionsPerPeer: 10,
	SessionTTL:         15 * time.Minute,
	SessionIdleTimeout: 5 * time.Minute,
	GCTickerInterval:   1 * time.Minute,
}

type StreamHandler struct {
	host              host.Host
	engine            *engine.ContentEngine
	kdht              *dht.IpfsDHT
	digest            core.Digest
	allowPush         bool
	authPolicy        AuthPolicy
	allowedPublishers map[peer.ID]struct{}

	MaxPendingSessions int
	MaxSessionsPerPeer int
	SessionTTL         time.Duration
	SessionIdleTimeout time.Duration
	GCTickerInterval   time.Duration

	sessionsMu   sync.RWMutex
	sessions     map[core.ContentID]*PendingSession
	peerSessions map[peer.ID]int

	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func NewStreamHandler(
	h host.Host,
	eng *engine.ContentEngine,
	kdht *dht.IpfsDHT,
	allowPush bool,
	authPolicy AuthPolicy,
	allowedPublishers []peer.ID,
) *StreamHandler {
	return NewStreamHandlerWithConfig(h, eng, kdht, allowPush, authPolicy, allowedPublishers, DefaultConfig)
}

func NewStreamHandlerWithConfig(
	h host.Host,
	eng *engine.ContentEngine,
	kdht *dht.IpfsDHT,
	allowPush bool,
	authPolicy AuthPolicy,
	allowedPublishers []peer.ID,
	cfg StreamHandlerConfig,
) *StreamHandler {
	allowedMap := make(map[peer.ID]struct{})
	for _, pid := range allowedPublishers {
		allowedMap[pid] = struct{}{}
	}

	ctx, cancel := context.WithCancel(context.Background())

	handler := &StreamHandler{
		host:              h,
		engine:            eng,
		kdht:              kdht,
		digest:            verifier.NewSHA256Digest(),
		allowPush:         allowPush,
		authPolicy:        authPolicy,
		allowedPublishers: allowedMap,
		MaxPendingSessions: cfg.MaxPendingSessions,
		MaxSessionsPerPeer: cfg.MaxSessionsPerPeer,
		SessionTTL:         cfg.SessionTTL,
		SessionIdleTimeout: cfg.SessionIdleTimeout,
		GCTickerInterval:   cfg.GCTickerInterval,
		sessions:          make(map[core.ContentID]*PendingSession),
		peerSessions:      make(map[peer.ID]int),
		ctx:               ctx,
		cancel:            cancel,
	}

	handler.startGC()

	if h != nil {
		h.SetStreamHandler(protocol.PushTransportProtocolID, handler.handleStream)
	}
	return handler
}

func (h *StreamHandler) startGC() {
	if h.ctx == nil {
		return
	}
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		interval := h.GCTickerInterval
		if interval <= 0 {
			interval = DefaultConfig.GCTickerInterval
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-h.ctx.Done():
				return
			case <-ticker.C:
				h.sessionsMu.Lock()
				h.evictStaleSessionsLocked(time.Now())
				h.sessionsMu.Unlock()
			}
		}
	}()
}

func (h *StreamHandler) evictStaleSessionsLocked(now time.Time) int {
	evicted := 0
	for cid, session := range h.sessions {
		isTTLExpired := h.SessionTTL > 0 && now.Sub(session.StartedAt) > h.SessionTTL
		isIdleExpired := h.SessionIdleTimeout > 0 && now.Sub(session.UpdatedAt) > h.SessionIdleTimeout

		if isTTLExpired || isIdleExpired {
			delete(h.sessions, cid)
			h.peerSessions[session.PublisherPeer]--
			if h.peerSessions[session.PublisherPeer] <= 0 {
				delete(h.peerSessions, session.PublisherPeer)
			}
			evicted++
			log.Printf("[Push Protocol] GC evicted stale push session for ContentID %x from peer %s (TTL expired: %v, Idle expired: %v)",
				cid, session.PublisherPeer, isTTLExpired, isIdleExpired)
		}
	}
	return evicted
}

func (h *StreamHandler) Close() error {
	h.stopOnce.Do(func() {
		if h.cancel != nil {
			h.cancel()
		}
		h.wg.Wait()
	})
	return nil
}

func (h *StreamHandler) Stop() {
	_ = h.Close()
}

func (h *StreamHandler) PendingSessionCount() int {
	h.sessionsMu.RLock()
	defer h.sessionsMu.RUnlock()
	return len(h.sessions)
}

func (h *StreamHandler) PeerSessionCount(pid peer.ID) int {
	h.sessionsMu.RLock()
	defer h.sessionsMu.RUnlock()
	return h.peerSessions[pid]
}

func (h *StreamHandler) PurgeStaleSessions() int {
	h.sessionsMu.Lock()
	defer h.sessionsMu.Unlock()
	return h.evictStaleSessionsLocked(time.Now())
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

	now := time.Now()

	h.sessionsMu.Lock()

	existingSession, isUpdate := h.sessions[contentID]

	// Check per-peer limit
	currentPeerSessions := h.peerSessions[remotePeer]
	if isUpdate && existingSession.PublisherPeer == remotePeer {
		// Peer session count doesn't increase for same-peer update
	} else if h.MaxSessionsPerPeer > 0 && currentPeerSessions >= h.MaxSessionsPerPeer {
		h.sessionsMu.Unlock()
		log.Printf("[Push Protocol] Rejecting push manifest from %s: peer session count %d reaches limit %d",
			remotePeer, currentPeerSessions, h.MaxSessionsPerPeer)
		_ = WritePushMessage(s, BuildPushManifestAck(contentID, PushStatusResourceExhausted))
		return
	}

	// Check total session limit
	if !isUpdate && h.MaxPendingSessions > 0 && len(h.sessions) >= h.MaxPendingSessions {
		// Trigger immediate stale session eviction before rejecting new sessions
		h.evictStaleSessionsLocked(now)

		if len(h.sessions) >= h.MaxPendingSessions {
			h.sessionsMu.Unlock()
			log.Printf("[Push Protocol] Rejecting push manifest from %s: total sessions %d reaches max %d",
				remotePeer, len(h.sessions), h.MaxPendingSessions)
			_ = WritePushMessage(s, BuildPushManifestAck(contentID, PushStatusResourceExhausted))
			return
		}
	}

	// Clean up existing session tracking if replacing
	if isUpdate {
		h.peerSessions[existingSession.PublisherPeer]--
		if h.peerSessions[existingSession.PublisherPeer] <= 0 {
			delete(h.peerSessions, existingSession.PublisherPeer)
		}
	}

	h.sessions[contentID] = &PendingSession{
		ContentID:       contentID,
		PublisherPeer:   remotePeer,
		Manifest:        m,
		ManifestBytes:   manifestData,
		ExpectedChunks:  expectedMap,
		CommittedChunks: make(map[core.ChunkID]bool),
		StartedAt:       now,
		UpdatedAt:       now,
	}
	h.peerSessions[remotePeer]++
	h.sessionsMu.Unlock()

	log.Printf("[Push Protocol] Initialized push session for ContentID %x from %s (expecting %d chunks)", contentID, remotePeer, len(expectedMap))

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
	delete(h.sessions, contentID)
	h.peerSessions[session.PublisherPeer]--
	if h.peerSessions[session.PublisherPeer] <= 0 {
		delete(h.peerSessions, session.PublisherPeer)
	}
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
