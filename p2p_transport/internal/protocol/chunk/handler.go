package chunk

import (
	"context"
	"io"
	"log"
	"math/rand"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/content/engine"
	"cipher/internal/protocol"
)

var TestCorruptProb float64

type HandlerOption func(*StreamHandler)

func WithReadTimeout(d time.Duration) HandlerOption {
	return func(h *StreamHandler) {
		h.readTimeout = d
	}
}

func WithWriteTimeout(d time.Duration) HandlerOption {
	return func(h *StreamHandler) {
		h.writeTimeout = d
	}
}

type StreamHandler struct {
	host         host.Host
	engine       *engine.ContentEngine
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine, opts ...HandlerOption) *StreamHandler {
	handler := &StreamHandler{
		host:         h,
		engine:       eng,
		readTimeout:  30 * time.Second,
		writeTimeout: 15 * time.Second,
	}
	for _, opt := range opts {
		opt(handler)
	}
	h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	return handler
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	log.Printf("[Chunk Protocol] New stream from %s", s.Conn().RemotePeer())

	for {
		if err := s.SetReadDeadline(time.Now().Add(h.readTimeout)); err != nil {
			log.Printf("[Chunk Protocol] Error setting read deadline: %v", err)
			return
		}

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
			_ = s.SetWriteDeadline(time.Now().Add(h.writeTimeout))
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg)
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			_ = s.SetWriteDeadline(time.Now().Add(h.writeTimeout))
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(h.writeTimeout))
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		return
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(h.writeTimeout))
		WriteMessage(s, BuildError(ErrContentNotFound, "manifest not found"))
		return
	}

	resp := BuildManifest(contentID, manifestData)
	if err := s.SetWriteDeadline(time.Now().Add(h.writeTimeout)); err != nil {
		log.Printf("[Chunk Protocol] Error setting write deadline: %v", err)
		return
	}
	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
	}
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message) {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(h.writeTimeout))
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		return
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(h.writeTimeout))
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
		_ = s.SetWriteDeadline(time.Now().Add(h.writeTimeout))
		WriteMessage(s, BuildError(ErrInternal, "failed to build chunk message"))
		return
	}

	if err := s.SetWriteDeadline(time.Now().Add(h.writeTimeout)); err != nil {
		log.Printf("[Chunk Protocol] Error setting write deadline: %v", err)
		return
	}
	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return
	}

	// 5. Wait for ACK synchronously (sequential protocol requirement)
	if err := s.SetReadDeadline(time.Now().Add(h.readTimeout)); err != nil {
		log.Printf("[Chunk Protocol] Error setting read deadline for ACK: %v", err)
		return
	}
	ackMsg, err := ReadMessage(s)
	if err != nil {
		log.Printf("[Chunk Protocol] Error reading ACK: %v", err)
		return
	}
	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		log.Printf("[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, msgStr)
		return
	}
	if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK, got type %d", ackMsg.Type)
		return
	}
}
