package push

import (
	"context"
	"errors"
	"io"
	"log"
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

	sessionConfig SessionConfig
	sessionMgr    *SessionManager
}

type Option func(*StreamHandler)

func WithSessionConfig(cfg SessionConfig) Option {
	return func(h *StreamHandler) {
		h.sessionConfig = cfg
	}
}

func WithMaxGlobalSessions(n int) Option {
	return func(h *StreamHandler) {
		h.sessionConfig.MaxGlobalSessions = n
	}
}

func WithMaxPeerSessions(n int) Option {
	return func(h *StreamHandler) {
		h.sessionConfig.MaxPeerSessions = n
	}
}

func WithSessionTTL(ttl time.Duration) Option {
	return func(h *StreamHandler) {
		h.sessionConfig.SessionTTL = ttl
	}
}

func WithPruneInterval(interval time.Duration) Option {
	return func(h *StreamHandler) {
		h.sessionConfig.PruneInterval = interval
	}
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
		sessionConfig:     DefaultSessionConfig(),
	}

	for _, opt := range opts {
		opt(handler)
	}

	handler.sessionMgr = NewSessionManager(handler.sessionConfig)

	if h != nil {
		h.SetStreamHandler(protocol.PushTransportProtocolID, handler.handleStream)
	}
	return handler
}

func (h *StreamHandler) SessionManager() *SessionManager {
	return h.sessionMgr
}

func (h *StreamHandler) Close() error {
	if h.sessionMgr != nil {
		h.sessionMgr.Close()
	}
	return nil
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

	session, err := h.sessionMgr.CreateSession(remotePeer, contentID, m, manifestData, assignedChunkIDs)
	if err != nil {
		if errors.Is(err, ErrPeerQuotaExceeded) {
			log.Printf("[Push Protocol] Ingestion rejected from %s: per-peer session quota exceeded", remotePeer)
			_ = WritePushMessage(s, BuildPushError(PushStatusQuotaExceeded, "per-peer push session quota exceeded"))
			return
		}
		if errors.Is(err, ErrGlobalCapacityExceeded) {
			log.Printf("[Push Protocol] Ingestion rejected from %s: global session capacity limit reached", remotePeer)
			_ = WritePushMessage(s, BuildPushError(PushStatusCapacityExceeded, "global push session capacity limit reached"))
			return
		}
		log.Printf("[Push Protocol] Ingestion rejected from %s: %v", remotePeer, err)
		_ = WritePushMessage(s, BuildPushError(PushStatusCapacityExceeded, err.Error()))
		return
	}

	log.Printf("[Push Protocol] Initialized push session for ContentID %x from peer %s (expecting %d chunks)", contentID, remotePeer, len(session.ExpectedChunks))

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

	session, exists := h.sessionMgr.GetSession(contentID)
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

	h.sessionMgr.RecordChunkCommit(contentID, chunkID)

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

	session, exists := h.sessionMgr.GetSession(contentID)
	if !exists {
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
		log.Printf("[Push Protocol] BatchComplete rejected for %x: committed %d/%d assigned chunks",
			contentID, len(session.CommittedChunks), len(session.ExpectedChunks))
		_ = WritePushMessage(s, BuildPushBatchCompleteAck(contentID, PushStatusIncomplete))
		return
	}

	// Commit manifest to local CAS
	ctx := context.Background()
	if err := h.engine.PutManifestBytes(ctx, contentID, session.ManifestBytes); err != nil {
		log.Printf("[Push Protocol] Failed to store manifest for %x: %v", contentID, err)
		_ = WritePushMessage(s, BuildPushBatchCompleteAck(contentID, PushStatusIOError))
		return
	}

	// Remove session from pending
	h.sessionMgr.RemoveSession(contentID)

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
