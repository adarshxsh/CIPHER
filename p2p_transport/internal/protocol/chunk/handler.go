package chunk

import (
	"context"
	"errors"
	"io"
	"log"
	"math/rand"

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
		frame, err := DecodeFrame(s)
		if err != nil {
			if errors.Is(err, io.EOF) || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", s.Conn().RemotePeer())
				return
			}
			if errors.Is(err, ErrInvalidProtocolVersion) {
				log.Printf("[Chunk Protocol] Unsupported version %v", err)
				WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
				return
			}
			if errors.Is(err, ErrUnknownMessageType) {
				log.Printf("[Chunk Protocol] Unsupported message type %v", err)
				WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
				return
			}
			log.Printf("[Chunk Protocol] Error reading message: %v", err)
			return
		}

		switch frame.MessageType {
		case MsgRequestManifest:
			h.handleRequestManifest(s, frame)
		case MsgRequestChunk:
			h.handleRequestChunk(s, frame)
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", frame.MessageType)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, frame *Frame) {
	contentID, err := ParseRequestManifest(frame.Payload)
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

func (h *StreamHandler) handleRequestChunk(s network.Stream, frame *Frame) {
	chunkID, err := ParseRequestChunk(frame.Payload)
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
	ackFrame, err := DecodeFrame(s)
	if err != nil {
		log.Printf("[Chunk Protocol] Error reading ACK: %v", err)
		return
	}
	if ackFrame.MessageType == MsgError {
		code, msgStr, _ := ParseError(ackFrame.Payload)
		log.Printf("[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, msgStr)
		return
	}
	if ackFrame.MessageType != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK, got type %d", ackFrame.MessageType)
		return
	}
}
