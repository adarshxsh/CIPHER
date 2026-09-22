package chunk

import (
	"context"
	"io"
	"log"
	"math/rand"
	"sync"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"

	"cipher/internal/content/engine"
	"cipher/internal/protocol"
)

var TestCorruptProb float64

type peerLimiterState struct {
	limiter  *rate.Limiter
	refCount int
}

type StreamHandler struct {
	host       host.Host
	engine     *engine.ContentEngine
	rateLimit  rate.Limit
	burstLimit int

	mu       sync.Mutex
	limiters map[peer.ID]*peerLimiterState
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine) *StreamHandler {
	handler := &StreamHandler{
		host:       h,
		engine:     eng,
		rateLimit:  rate.Limit(5),
		burstLimit: 5,
		limiters:   make(map[peer.ID]*peerLimiterState),
	}
	if h != nil {
		h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	}
	return handler
}

func (h *StreamHandler) SetRateLimit(r rate.Limit, burst int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rateLimit = r
	h.burstLimit = burst
}

func (h *StreamHandler) getLimiter(p peer.ID) *rate.Limiter {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.limiters == nil {
		h.limiters = make(map[peer.ID]*peerLimiterState)
	}

	r := h.rateLimit
	if r <= 0 {
		r = rate.Limit(5)
	}
	b := h.burstLimit
	if b <= 0 {
		b = 5
	}

	state, ok := h.limiters[p]
	if !ok {
		state = &peerLimiterState{
			limiter:  rate.NewLimiter(r, b),
			refCount: 0,
		}
		h.limiters[p] = state
	}
	state.refCount++
	return state.limiter
}

func (h *StreamHandler) putLimiter(p peer.ID) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.limiters == nil {
		return
	}

	state, ok := h.limiters[p]
	if ok {
		state.refCount--
		if state.refCount <= 0 {
			delete(h.limiters, p)
		}
	}
}

func (h *StreamHandler) logError(limiter *rate.Limiter, format string, v ...interface{}) {
	if limiter.Allow() {
		log.Printf(format, v...)
	}
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	remotePeer := s.Conn().RemotePeer()
	limiter := h.getLimiter(remotePeer)
	defer h.putLimiter(remotePeer)

	log.Printf("[Chunk Protocol] New stream from %s", remotePeer)

	for {
		msg, err := ReadMessage(s)
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", remotePeer)
				return
			}
			h.logError(limiter, "[Chunk Protocol] Error reading message: %v", err)
			return
		}

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			h.logError(limiter, "[Chunk Protocol] Unsupported version %d", msg.Version)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg, limiter)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg, limiter)
		default:
			h.logError(limiter, "[Chunk Protocol] Unsupported message type: %d", msg.Type)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message, limiter *rate.Limiter) {
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
		h.logError(limiter, "[Chunk Protocol] Error writing MANIFEST response: %v", err)
	}
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message, limiter *rate.Limiter) {
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
		h.logError(limiter, "[Chunk Protocol] Error writing CHUNK response: %v", err)
		return
	}

	// 5. Wait for ACK synchronously (sequential protocol requirement)
	ackMsg, err := ReadMessage(s)
	if err != nil {
		h.logError(limiter, "[Chunk Protocol] Error reading ACK: %v", err)
		return
	}
	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		h.logError(limiter, "[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, msgStr)
		return
	}
	if ackMsg.Type != MsgAck {
		h.logError(limiter, "[Chunk Protocol] Expected ACK, got type %d", ackMsg.Type)
		return
	}
}
