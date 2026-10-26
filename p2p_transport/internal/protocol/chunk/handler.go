package chunk

import (
	"context"
	"io"
	"log"
	"math/rand"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"golang.org/x/time/rate"

	"cipher/internal/content/engine"
	"cipher/internal/protocol"
)

var TestCorruptProb float64

type StreamHandler struct {
	host          host.Host
	engine        *engine.ContentEngine
	errorLogRate  rate.Limit
	errorLogBurst int
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine) *StreamHandler {
	handler := &StreamHandler{
		host:          h,
		engine:        eng,
		errorLogRate:  rate.Limit(DefaultErrorLogRate),
		errorLogBurst: DefaultErrorLogBurst,
	}
	h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	return handler
}

func (h *StreamHandler) SetErrorLogRate(r rate.Limit, b int) {
	h.errorLogRate = r
	h.errorLogBurst = b
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	log.Printf("[Chunk Protocol] New stream from %s", s.Conn().RemotePeer())

	logLimiter := rate.NewLimiter(h.errorLogRate, h.errorLogBurst)

	for {
		msg, err := ReadMessage(s)
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", s.Conn().RemotePeer())
				return
			}
			log.Printf("[Chunk Protocol] Error reading message: %v", err)
			return
		}

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			log.Printf("[Chunk Protocol] Unsupported version %d", msg.Version)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg, logLimiter)
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) {
	if err := ValidateMessagePayload(msg.Type, msg.Payload); err != nil {
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		return
	}

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

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message, logLimiter *rate.Limiter) {
	if err := ValidateMessagePayload(msg.Type, msg.Payload); err != nil {
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		return
	}

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
		log.Printf("[Chunk Protocol] Error reading ACK: %v", err)
		return
	}

	if err := ValidateMessagePayload(ackMsg.Type, ackMsg.Payload); err != nil {
		if logLimiter == nil || logLimiter.Allow() {
			log.Printf("[Chunk Protocol] Invalid payload from client on chunk %x: %v", chunkID, err)
		}
		return
	}

	if ackMsg.Type == MsgError {
		code, msgStr, err := ParseError(ackMsg.Payload)
		if err != nil {
			if logLimiter == nil || logLimiter.Allow() {
				log.Printf("[Chunk Protocol] Client sent malformed error payload on chunk %x: %v", chunkID, err)
			}
			return
		}
		if logLimiter == nil || logLimiter.Allow() {
			log.Printf("[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, msgStr)
		}
		return
	}
	if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK, got type %d", ackMsg.Type)
		return
	}
}
