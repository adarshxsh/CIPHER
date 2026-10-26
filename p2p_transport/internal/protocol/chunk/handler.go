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
	log.Printf("[Chunk Protocol] New stream from %s", s.Conn().RemotePeer())

	for {
		_ = s.SetReadDeadline(time.Now().Add(StreamReadTimeout))
		msg, err := ReadMessage(s)
		_ = s.SetReadDeadline(time.Time{})
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
			_ = s.SetWriteDeadline(time.Now().Add(StreamWriteTimeout))
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			_ = s.SetWriteDeadline(time.Time{})
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg)
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			_ = s.SetWriteDeadline(time.Now().Add(StreamWriteTimeout))
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
			_ = s.SetWriteDeadline(time.Time{})
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(StreamWriteTimeout))
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		_ = s.SetWriteDeadline(time.Time{})
		return
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(StreamWriteTimeout))
		WriteMessage(s, BuildError(ErrContentNotFound, "manifest not found"))
		_ = s.SetWriteDeadline(time.Time{})
		return
	}

	resp := BuildManifest(contentID, manifestData)
	_ = s.SetWriteDeadline(time.Now().Add(StreamWriteTimeout))
	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
	}
	_ = s.SetWriteDeadline(time.Time{})
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message) {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(StreamWriteTimeout))
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		_ = s.SetWriteDeadline(time.Time{})
		return
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(StreamWriteTimeout))
		WriteMessage(s, BuildError(ErrChunkNotFound, "chunk not found"))
		_ = s.SetWriteDeadline(time.Time{})
		return
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(StreamWriteTimeout))
		WriteMessage(s, BuildError(ErrInternal, "failed to build chunk message"))
		_ = s.SetWriteDeadline(time.Time{})
		return
	}

	_ = s.SetWriteDeadline(time.Now().Add(StreamWriteTimeout))
	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		_ = s.SetWriteDeadline(time.Time{})
		return
	}
	_ = s.SetWriteDeadline(time.Time{})

	// 5. Wait for ACK synchronously (sequential protocol requirement)
	_ = s.SetReadDeadline(time.Now().Add(StreamReadTimeout))
	ackMsg, err := ReadMessage(s)
	_ = s.SetReadDeadline(time.Time{})
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
