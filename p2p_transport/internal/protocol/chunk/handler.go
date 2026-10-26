package chunk

import (
	"context"
	"errors"
	"io"
	"log"
	"math/rand"
	"net"
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
	defer func() {
		_ = s.SetReadDeadline(time.Time{})
		_ = s.Close()
	}()
	log.Printf("[Chunk Protocol] New stream from %s", s.Conn().RemotePeer())

	txCount := 0
	for {
		if err := s.SetReadDeadline(time.Now().Add(StreamTimeout)); err != nil {
			log.Printf("[Chunk Protocol] Error setting read deadline: %v", err)
			_ = s.Reset()
			return
		}

		msg, err := ReadMessage(s)
		if err != nil {
			if errors.Is(err, io.EOF) || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", s.Conn().RemotePeer())
				return
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				log.Printf("[Chunk Protocol] Read deadline expired for stream from %s: %v", s.Conn().RemotePeer(), err)
				_ = s.Reset()
				return
			}
			log.Printf("[Chunk Protocol] Error reading message from %s: %v", s.Conn().RemotePeer(), err)
			_ = s.Reset()
			return
		}

		if txCount >= MaxTransactionsPerStream {
			log.Printf("[Chunk Protocol] Stream transaction limit exceeded (%d/%d) from %s", txCount, MaxTransactionsPerStream, s.Conn().RemotePeer())
			_ = WriteMessage(s, BuildError(ErrBadRequest, "transaction limit exceeded"))
			_ = s.Reset()
			return
		}

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			log.Printf("[Chunk Protocol] Unsupported version %d", msg.Version)
			_ = WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			_ = s.Reset()
			return
		}

		var txCompleted bool
		switch msg.Type {
		case MsgRequestManifest:
			txCompleted = h.handleRequestManifest(s, msg)
		case MsgRequestChunk:
			txCompleted = h.handleRequestChunk(s, msg)
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			_ = WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
			_ = s.Reset()
			return
		}

		if txCompleted {
			txCount++
		}

		if txCount >= MaxTransactionsPerStream {
			log.Printf("[Chunk Protocol] Stream transaction limit reached (%d/%d) for peer %s, closing stream", txCount, MaxTransactionsPerStream, s.Conn().RemotePeer())
			return
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) bool {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		return false
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		WriteMessage(s, BuildError(ErrContentNotFound, "manifest not found"))
		return false
	}

	resp := BuildManifest(contentID, manifestData)
	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
		return false
	}
	return true
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message) bool {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		return false
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		WriteMessage(s, BuildError(ErrChunkNotFound, "chunk not found"))
		return false
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		WriteMessage(s, BuildError(ErrInternal, "failed to build chunk message"))
		return false
	}

	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return false
	}

	// Wait for ACK synchronously (sequential protocol requirement)
	if err := s.SetReadDeadline(time.Now().Add(StreamTimeout)); err != nil {
		log.Printf("[Chunk Protocol] Error setting read deadline for ACK: %v", err)
		return false
	}

	ackMsg, err := ReadMessage(s)
	if err != nil {
		log.Printf("[Chunk Protocol] Error reading ACK: %v", err)
		return false
	}
	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		log.Printf("[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, msgStr)
		return true
	}
	if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK, got type %d", ackMsg.Type)
		return false
	}
	return true
}
