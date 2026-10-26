package chunk

import (
	"context"
	"io"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"

	"cipher/internal/content/engine"
	"cipher/internal/protocol"
)

var TestCorruptProb float64

type peerLimiterEntry struct {
	limiter         *rate.Limiter
	lastSeen        time.Time
	suppressedCount uint64
}

type PeerErrorRateLimiter struct {
	mu       sync.Mutex
	limiters map[peer.ID]*peerLimiterEntry
	rate     rate.Limit
	burst    int
	ttl      time.Duration
}

func NewPeerErrorRateLimiter(r rate.Limit, burst int, ttl time.Duration) *PeerErrorRateLimiter {
	return &PeerErrorRateLimiter{
		limiters: make(map[peer.ID]*peerLimiterEntry),
		rate:     r,
		burst:    burst,
		ttl:      ttl,
	}
}

func (p *PeerErrorRateLimiter) Allow(peerID peer.ID) (bool, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()

	for id, entry := range p.limiters {
		if now.Sub(entry.lastSeen) > p.ttl {
			delete(p.limiters, id)
		}
	}

	entry, ok := p.limiters[peerID]
	if !ok {
		entry = &peerLimiterEntry{
			limiter:  rate.NewLimiter(p.rate, p.burst),
			lastSeen: now,
		}
		p.limiters[peerID] = entry
	}
	entry.lastSeen = now

	if entry.limiter.AllowN(now, 1) {
		prevSuppressed := entry.suppressedCount
		entry.suppressedCount = 0
		return true, prevSuppressed
	}

	entry.suppressedCount++
	return false, 0
}

type StreamHandler struct {
	host             host.Host
	engine           *engine.ContentEngine
	errorRateLimiter *PeerErrorRateLimiter
}

type StreamHandlerOption func(*StreamHandler)

func WithErrorRateLimit(r rate.Limit, burst int) StreamHandlerOption {
	return func(sh *StreamHandler) {
		sh.errorRateLimiter = NewPeerErrorRateLimiter(r, burst, sh.errorRateLimiter.ttl)
	}
}

func WithErrorRateLimiterTTL(ttl time.Duration) StreamHandlerOption {
	return func(sh *StreamHandler) {
		sh.errorRateLimiter.ttl = ttl
	}
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine, opts ...StreamHandlerOption) *StreamHandler {
	handler := &StreamHandler{
		host:             h,
		engine:           eng,
		errorRateLimiter: NewPeerErrorRateLimiter(rate.Limit(10), 10, 5*time.Minute),
	}
	for _, opt := range opts {
		opt(handler)
	}
	if h != nil {
		h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	}
	return handler
}

func (h *StreamHandler) AllowErrorLog(peerID peer.ID) bool {
	if h.errorRateLimiter == nil {
		return true
	}
	allowed, suppressed := h.errorRateLimiter.Allow(peerID)
	if allowed && suppressed > 0 {
		log.Printf("[Chunk Protocol] Suppressed %d error log messages from peer %s", suppressed, peerID)
	}
	return allowed
}

func (h *StreamHandler) logPeerError(peerID peer.ID, format string, args ...interface{}) {
	if h.AllowErrorLog(peerID) {
		log.Printf(format, args...)
	}
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	remotePeer := s.Conn().RemotePeer()
	log.Printf("[Chunk Protocol] New stream from %s", remotePeer)

	for {
		msg, err := ReadMessage(s)
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", remotePeer)
				return
			}
			h.logPeerError(remotePeer, "[Chunk Protocol] Error reading message from %s: %v", remotePeer, err)
			return
		}

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			h.logPeerError(remotePeer, "[Chunk Protocol] Unsupported version %d from %s", msg.Version, remotePeer)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg)
		default:
			h.logPeerError(remotePeer, "[Chunk Protocol] Unsupported message type %d from %s", msg.Type, remotePeer)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) {
	remotePeer := s.Conn().RemotePeer()
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		return
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		WriteMessage(s, BuildError(ErrContentNotFound, "manifest not found"))
		return
	}

	resp := BuildManifest(contentID, manifestData)
	if err := WriteMessage(s, resp); err != nil {
		h.logPeerError(remotePeer, "[Chunk Protocol] Error writing MANIFEST response to %s: %v", remotePeer, err)
	}
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message) {
	remotePeer := s.Conn().RemotePeer()
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		return
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		WriteMessage(s, BuildError(ErrChunkNotFound, "chunk not found"))
		return
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		WriteMessage(s, BuildError(ErrInternal, "failed to build chunk message"))
		return
	}

	if err := WriteMessage(s, resp); err != nil {
		h.logPeerError(remotePeer, "[Chunk Protocol] Error writing CHUNK response to %s: %v", remotePeer, err)
		return
	}

	// 5. Wait for ACK synchronously (sequential protocol requirement)
	ackMsg, err := ReadMessage(s)
	if err != nil {
		h.logPeerError(remotePeer, "[Chunk Protocol] Error reading ACK from %s: %v", remotePeer, err)
		return
	}
	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		h.logPeerError(remotePeer, "[Chunk Protocol] Client %s reported error on chunk %x: [%d] %s", remotePeer, chunkID, code, msgStr)
		return
	}
	if ackMsg.Type != MsgAck {
		h.logPeerError(remotePeer, "[Chunk Protocol] Expected ACK from %s, got type %d", remotePeer, ackMsg.Type)
		return
	}
}
