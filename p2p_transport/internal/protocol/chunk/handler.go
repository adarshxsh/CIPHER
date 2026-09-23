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
	remotePeer := s.Conn().RemotePeer()
	log.Printf("[Chunk Protocol] New stream from %s", remotePeer)

	messageCount := 0

	for {
		msg, err := ReadMessage(s)
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", remotePeer)
				return
			}
			log.Printf("[Chunk Protocol] Error reading message: %v", err)
			return
		}

		messageCount++
		if messageCount > MaxMessagesPerStream {
			log.Printf("[Chunk Protocol] Rate limit error: stream from %s exceeded max messages per stream (%d)", remotePeer, MaxMessagesPerStream)
			WriteMessage(s, BuildError(ErrBadRequest, "rate limit exceeded: max messages per stream exceeded"))
			return
		}

		if err := ValidateMessage(msg); err != nil {
			log.Printf("[Chunk Protocol] Invalid message from %s: %v", remotePeer, err)
			if errors.Is(err, ErrInvalidMessageVersion) || errors.Is(err, ErrInvalidMessageType) {
				WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version or type"))
			} else {
				WriteMessage(s, BuildError(ErrBadRequest, "invalid message envelope or payload"))
			}
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg, &messageCount)
		case MsgError:
			code, msgStr, _ := ParseError(msg.Payload)
			log.Printf("[Chunk Protocol] Remote peer %s sent error: [%d] %s", remotePeer, code, msgStr)
			return
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
			return
		}
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

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message, messageCount *int) {
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

	*messageCount++
	if *messageCount > MaxMessagesPerStream {
		log.Printf("[Chunk Protocol] Rate limit error: stream from %s exceeded max messages per stream (%d)", s.Conn().RemotePeer(), MaxMessagesPerStream)
		WriteMessage(s, BuildError(ErrBadRequest, "rate limit exceeded: max messages per stream exceeded"))
		return
	}

	if err := ValidateMessage(ackMsg); err != nil {
		log.Printf("[Chunk Protocol] Invalid ACK/message from %s: %v", s.Conn().RemotePeer(), err)
		WriteMessage(s, BuildError(ErrBadRequest, "invalid ACK message"))
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
