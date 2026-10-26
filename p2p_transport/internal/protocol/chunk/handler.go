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

type StreamHandler struct {
	host           host.Host
	engine         *engine.ContentEngine
	ControlTimeout time.Duration
	ChunkTimeout   time.Duration
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine) *StreamHandler {
	handler := &StreamHandler{
		host:           h,
		engine:         eng,
		ControlTimeout: 15 * time.Second,
		ChunkTimeout:   30 * time.Second,
	}
	h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	return handler
}

func (h *StreamHandler) writeError(s network.Stream, code ErrorCode, msg string) {
	_ = s.SetWriteDeadline(time.Now().Add(h.ControlTimeout))
	_ = WriteMessage(s, BuildError(code, msg))
	_ = s.SetWriteDeadline(time.Time{})
}

func (h *StreamHandler) handleStream(s network.Stream) {
	log.Printf("[Chunk Protocol] New stream from %s", s.Conn().RemotePeer())

	for {
		_ = s.SetReadDeadline(time.Now().Add(h.ControlTimeout))
		msg, err := ReadMessage(s)
		_ = s.SetReadDeadline(time.Time{})
		if err != nil {
			if err == io.EOF {
				log.Printf("[Chunk Protocol] Stream closed gracefully by %s", s.Conn().RemotePeer())
				s.Close()
				return
			}
			log.Printf("[Chunk Protocol] Error reading message: %v", err)
			s.Reset()
			return
		}

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			log.Printf("[Chunk Protocol] Unsupported version %d", msg.Version)
			h.writeError(s, ErrUnsupportedMessage, "unsupported message version")
			s.Reset()
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			if !h.handleRequestManifest(s, msg) {
				s.Reset()
				return
			}
		case MsgRequestChunk:
			if !h.handleRequestChunk(s, msg) {
				s.Reset()
				return
			}
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			h.writeError(s, ErrUnsupportedMessage, "unsupported message type")
			s.Reset()
			return
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) bool {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		h.writeError(s, ErrBadRequest, "invalid payload for REQUEST_MANIFEST")
		return false
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		h.writeError(s, ErrContentNotFound, "manifest not found")
		return false
	}

	resp := BuildManifest(contentID, manifestData)
	_ = s.SetWriteDeadline(time.Now().Add(h.ControlTimeout))
	err = WriteMessage(s, resp)
	_ = s.SetWriteDeadline(time.Time{})
	if err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
		return false
	}
	return true
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message) bool {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		h.writeError(s, ErrBadRequest, "invalid payload for REQUEST_CHUNK")
		return false
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		h.writeError(s, ErrChunkNotFound, "chunk not found")
		return false
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		h.writeError(s, ErrInternal, "failed to build chunk message")
		return false
	}

	_ = s.SetWriteDeadline(time.Now().Add(h.ChunkTimeout))
	err = WriteMessage(s, resp)
	_ = s.SetWriteDeadline(time.Time{})
	if err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return false
	}

	// 5. Wait for ACK synchronously (sequential protocol requirement)
	_ = s.SetReadDeadline(time.Now().Add(h.ControlTimeout))
	ackMsg, err := ReadMessage(s)
	_ = s.SetReadDeadline(time.Time{})
	if err != nil {
		log.Printf("[Chunk Protocol] Error reading ACK: %v", err)
		return false
	}
	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		log.Printf("[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, msgStr)
		return false
	}
	if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK, got type %d", ackMsg.Type)
		return false
	}
	return true
}
