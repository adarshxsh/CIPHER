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
	limiter  *rate.Limiter
	lastSeen time.Time
}

type StreamHandler struct {
	host         host.Host
	engine       *engine.ContentEngine
	peerLimiters map[peer.ID]*peerLimiterEntry
	limitersMu   sync.Mutex
	rateLimit    rate.Limit
	burst        int
	lastCleanup  time.Time
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine) *StreamHandler {
	return NewStreamHandlerWithRateLimit(h, eng, rate.Limit(5), 5)
}

func NewStreamHandlerWithRateLimit(h host.Host, eng *engine.ContentEngine, r rate.Limit, b int) *StreamHandler {
	handler := &StreamHandler{
		host:         h,
		engine:       eng,
		peerLimiters: make(map[peer.ID]*peerLimiterEntry),
		rateLimit:    r,
		burst:        b,
		lastCleanup:  time.Now(),
	}
	h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	return handler
}

func (h *StreamHandler) SetRateLimit(r rate.Limit, b int) {
	h.limitersMu.Lock()
	defer h.limitersMu.Unlock()
	h.rateLimit = r
	h.burst = b
	for _, entry := range h.peerLimiters {
		entry.limiter = rate.NewLimiter(r, b)
	}
}

func (h *StreamHandler) AllowErrorLog(p peer.ID) bool {
	h.limitersMu.Lock()
	defer h.limitersMu.Unlock()

	now := time.Now()
	// Automatically clear inactive peer records (> 5 minutes inactive)
	if now.Sub(h.lastCleanup) > 1*time.Minute {
		for id, entry := range h.peerLimiters {
			if now.Sub(entry.lastSeen) > 5*time.Minute {
				delete(h.peerLimiters, id)
			}
		}
		h.lastCleanup = now
	}

	entry, exists := h.peerLimiters[p]
	if !exists {
		entry = &peerLimiterEntry{
			limiter:  rate.NewLimiter(h.rateLimit, h.burst),
			lastSeen: now,
		}
		h.peerLimiters[p] = entry
	} else {
		entry.lastSeen = now
	}

	return entry.limiter.Allow()
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
			if h.AllowErrorLog(remotePeer) {
				log.Printf("[Chunk Protocol] Error reading message from peer %s: %v", remotePeer, err)
			}
			return
		}

		if msg.Version != CurrentMessageVersion {
			if h.AllowErrorLog(remotePeer) {
				log.Printf("[Chunk Protocol] Unsupported version %d from peer %s", msg.Version, remotePeer)
			}
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg)
		case MsgError:
			h.handleErrorMsg(s, msg)
		default:
			if h.AllowErrorLog(remotePeer) {
				log.Printf("[Chunk Protocol] Unsupported message type %d from peer %s", msg.Type, remotePeer)
			}
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
		}
	}
}

func (h *StreamHandler) handleErrorMsg(s network.Stream, msg *Message) {
	remotePeer := s.Conn().RemotePeer()
	code, msgStr, err := ParseError(msg.Payload)
	if err != nil {
		if h.AllowErrorLog(remotePeer) {
			log.Printf("[Chunk Protocol] Peer %s sent invalid error message: %v", remotePeer, err)
		}
		return
	}
	if h.AllowErrorLog(remotePeer) {
		log.Printf("[Chunk Protocol] Peer %s reported error: [%d] %s", remotePeer, code, msgStr)
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) {
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
		log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
	}
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message) {
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
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return
	}

	// 5. Wait for ACK synchronously (sequential protocol requirement)
	ackMsg, err := ReadMessage(s)
	if err != nil {
		remotePeer := s.Conn().RemotePeer()
		if h.AllowErrorLog(remotePeer) {
			log.Printf("[Chunk Protocol] Peer %s error reading ACK: %v", remotePeer, err)
		}
		return
	}
	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		remotePeer := s.Conn().RemotePeer()
		if h.AllowErrorLog(remotePeer) {
			log.Printf("[Chunk Protocol] Client %s reported error on chunk %x: [%d] %s", remotePeer, chunkID, code, msgStr)
		}
		return
	}
	if ackMsg.Type != MsgAck {
		remotePeer := s.Conn().RemotePeer()
		if h.AllowErrorLog(remotePeer) {
			log.Printf("[Chunk Protocol] Peer %s sent expected ACK, got type %d", remotePeer, ackMsg.Type)
		}
		return
	}
}
