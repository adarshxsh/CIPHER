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

// streamLogger applies a per-stream token bucket rate limiter to error logs.
type streamLogger struct {
	remotePeer peer.ID
	limiter    *rate.Limiter
	suppressed int
	mu         sync.Mutex
}

func newStreamLogger(peerID peer.ID) *streamLogger {
	return &streamLogger{
		remotePeer: peerID,
		limiter:    rate.NewLimiter(rate.Limit(5), 5), // 5 logs per second, burst 5
	}
}

func (sl *streamLogger) logError(format string, args ...interface{}) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	if sl.limiter.Allow() {
		if sl.suppressed > 0 {
			log.Printf("[Chunk Protocol] Rate limit reset for peer %s: suppressed %d log messages", sl.remotePeer, sl.suppressed)
			sl.suppressed = 0
		}
		log.Printf(format, args...)
	} else {
		sl.suppressed++
	}
}

func (sl *streamLogger) flush() {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	if sl.suppressed > 0 {
		log.Printf("[Chunk Protocol] Rate limit reset for peer %s: suppressed %d log messages", sl.remotePeer, sl.suppressed)
		sl.suppressed = 0
	}
}

type StreamHandler struct {
	host   host.Host
	engine *engine.ContentEngine
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine) *StreamHandler {
	handler := &StreamHandler{
		host:   h,
		engine: eng,
	}
	h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	return handler
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	remotePeer := s.Conn().RemotePeer()
	sl := newStreamLogger(remotePeer)
	defer sl.flush()

	log.Printf("[Chunk Protocol] New stream from %s", remotePeer)

	for {
		msg, err := ReadMessage(s)
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", remotePeer)
				return
			}
			sl.logError("[Chunk Protocol] Error reading message: %v", err)
			return
		}

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			sl.logError("[Chunk Protocol] Unsupported version %d", msg.Version)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg, sl)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg, sl)
		default:
			sl.logError("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message, sl *streamLogger) {
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
		sl.logError("[Chunk Protocol] Error writing MANIFEST response: %v", err)
	}
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message, sl *streamLogger) {
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
		sl.logError("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return
	}

	// 5. Wait for ACK synchronously (sequential protocol requirement)
	ackMsg, err := ReadMessage(s)
	if err != nil {
		sl.logError("[Chunk Protocol] Error reading ACK: %v", err)
		return
	}
	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		sl.logError("[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, msgStr)
		return
	}
	if ackMsg.Type != MsgAck {
		sl.logError("[Chunk Protocol] Expected ACK, got type %d", ackMsg.Type)
		return
	}
}
