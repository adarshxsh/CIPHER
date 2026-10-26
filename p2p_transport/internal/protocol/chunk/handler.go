package chunk

import (
	"context"
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

	msgCount := 0
	txCount := 0

	for {
		if txCount >= MaxTransactionsPerStream {
			log.Printf("[Chunk Protocol] Max transactions reached for stream %s", s.Conn().RemotePeer())
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

		msgCount++
		if msgCount > MaxMessagesPerStream {
			log.Printf("[Chunk Protocol] Message limit exceeded (%d > %d)", msgCount, MaxMessagesPerStream)
			WriteMessage(s, BuildError(ErrBadRequest, "message limit exceeded"))
			return
		}

		if msg.Version != CurrentMessageVersion {
			log.Printf("[Chunk Protocol] Unsupported version %d", msg.Version)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		if txCount >= MaxTransactionsPerStream {
			log.Printf("[Chunk Protocol] Transaction limit exceeded for stream %s", s.Conn().RemotePeer())
			WriteMessage(s, BuildError(ErrBadRequest, "transaction limit exceeded"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			ok := h.handleRequestManifest(s, msg)
			if !ok {
				return
			}
			txCount++

		case MsgRequestChunk:
			ok := h.handleRequestChunk(s, msg, &msgCount)
			if !ok {
				return
			}
			txCount++

		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
			return
		}

		if txCount >= MaxTransactionsPerStream {
			log.Printf("[Chunk Protocol] Transaction completed for stream %s, closing stream cleanly", s.Conn().RemotePeer())
			return
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) bool {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		return true
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		WriteMessage(s, BuildError(ErrContentNotFound, "manifest not found"))
		return true
	}

	resp := BuildManifest(contentID, manifestData)
	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
		return false
	}
	return true
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message, msgCount *int) bool {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		return true
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		WriteMessage(s, BuildError(ErrChunkNotFound, "chunk not found"))
		return true
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		WriteMessage(s, BuildError(ErrInternal, "failed to build chunk message"))
		return true
	}

	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return false
	}

	// 5. Wait for ACK synchronously (sequential protocol requirement)
	ackMsg, err := ReadMessage(s)
	if err != nil {
		log.Printf("[Chunk Protocol] Error reading ACK: %v", err)
		return false
	}

	*msgCount++
	if *msgCount > MaxMessagesPerStream {
		log.Printf("[Chunk Protocol] Message limit exceeded reading ACK (%d > %d)", *msgCount, MaxMessagesPerStream)
		WriteMessage(s, BuildError(ErrBadRequest, "message limit exceeded"))
		return false
	}

	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		log.Printf("[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, msgStr)
		return true
	}

	if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK, got type %d", ackMsg.Type)
		WriteMessage(s, BuildError(ErrBadRequest, "expected ACK message"))
		return false
	}

	return true
}
