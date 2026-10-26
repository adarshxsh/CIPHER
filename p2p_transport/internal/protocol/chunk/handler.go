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

type peerLimiter struct {
	limiter  *rate.Limiter
	dropped  uint64
	lastSeen time.Time
}

type StreamHandler struct {
	host     host.Host
	engine   *engine.ContentEngine
	mu       sync.Mutex
	limiters map[peer.ID]*peerLimiter
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine) *StreamHandler {
	handler := &StreamHandler{
		host:     h,
		engine:   eng,
		limiters: make(map[peer.ID]*peerLimiter),
	}
	if h != nil {
		h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	}
	return handler
}

func (h *StreamHandler) logError(p peer.ID, format string, v ...any) {
	h.mu.Lock()
	if h.limiters == nil {
		h.limiters = make(map[peer.ID]*peerLimiter)
	}

	// Prune map if it grows too large to prevent memory leaks
	now := time.Now()
	if len(h.limiters) > 1000 {
		for id, pl := range h.limiters {
			if now.Sub(pl.lastSeen) > time.Minute {
				delete(h.limiters, id)
			}
		}
		if len(h.limiters) > 1000 {
			h.limiters = make(map[peer.ID]*peerLimiter)
		}
	}

	pl, exists := h.limiters[p]
	if !exists {
		pl = &peerLimiter{
			limiter:  rate.NewLimiter(rate.Limit(5), 5),
			lastSeen: now,
		}
		h.limiters[p] = pl
	}
	pl.lastSeen = now

	allowed := pl.limiter.Allow()
	var dropped uint64
	if !allowed {
		pl.dropped++
		h.mu.Unlock()
		return
	}
	dropped = pl.dropped
	pl.dropped = 0
	h.mu.Unlock()

	if dropped > 0 {
		log.Printf("[Chunk Protocol] Peer %s: (suppressed %d log messages)", p, dropped)
	}
	log.Printf(format, v...)
}

func (h *StreamHandler) flushAndCleanup(p peer.ID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.limiters == nil {
		return
	}
	pl, exists := h.limiters[p]
	if !exists {
		return
	}
	if pl.dropped > 0 {
		log.Printf("[Chunk Protocol] Peer %s: (suppressed %d log messages)", p, pl.dropped)
		pl.dropped = 0
	}
}

func (h *StreamHandler) handleStream(s network.Stream) {
	p := s.Conn().RemotePeer()
	defer func() {
		s.Close()
		h.flushAndCleanup(p)
	}()
	log.Printf("[Chunk Protocol] New stream from %s", p)

	for {
		msg, err := ReadMessage(s)
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", p)
				return
			}
			h.logError(p, "[Chunk Protocol] Error reading message: %v", err)
			return
		}

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			h.logError(p, "[Chunk Protocol] Unsupported version %d", msg.Version)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg)
		default:
			h.logError(p, "[Chunk Protocol] Unsupported message type: %d", msg.Type)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) {
	p := s.Conn().RemotePeer()
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
		h.logError(p, "[Chunk Protocol] Error writing MANIFEST response: %v", err)
	}
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message) {
	p := s.Conn().RemotePeer()
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
		h.logError(p, "[Chunk Protocol] Error writing CHUNK response: %v", err)
		return
	}

	// 5. Wait for ACK synchronously (sequential protocol requirement)
	ackMsg, err := ReadMessage(s)
	if err != nil {
		h.logError(p, "[Chunk Protocol] Error reading ACK: %v", err)
		return
	}
	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		h.logError(p, "[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, msgStr)
		return
	}
	if ackMsg.Type != MsgAck {
		h.logError(p, "[Chunk Protocol] Expected ACK, got type %d", ackMsg.Type)
		return
	}
}
