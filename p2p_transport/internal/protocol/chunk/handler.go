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
	remotePeer := s.Conn().RemotePeer()
	log.Printf("[Chunk Protocol] New stream from %s", remotePeer)

	var messageCount int
	var transactionCount int

	for {
		_ = s.SetReadDeadline(time.Now().Add(StreamReadTimeout))
		readTimer := time.AfterFunc(StreamReadTimeout, func() {
			_ = s.Reset()
		})
		msg, err := ReadMessage(s)
		readTimer.Stop()
		_ = s.SetReadDeadline(time.Time{})
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
			log.Printf("[Chunk Protocol] Exceeded maximum messages per stream (%d > %d) from %s", messageCount, MaxMessagesPerStream, remotePeer)
			_ = h.sendError(s, ErrBadRequest, "exceeded maximum messages per stream")
			return
		}

		if transactionCount >= MaxTransactionsPerStream {
			log.Printf("[Chunk Protocol] Exceeded maximum transactions per stream (%d >= %d) from %s", transactionCount, MaxTransactionsPerStream, remotePeer)
			_ = h.sendError(s, ErrBadRequest, "exceeded maximum transactions per stream")
			return
		}

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			log.Printf("[Chunk Protocol] Unsupported version %d", msg.Version)
			_ = h.sendError(s, ErrUnsupportedMessage, "unsupported message version")
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg)
			transactionCount++
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg, &messageCount)
			transactionCount++
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			_ = h.sendError(s, ErrUnsupportedMessage, "unsupported message type")
			return
		}
	}
}

func (h *StreamHandler) sendError(s network.Stream, code ErrorCode, msg string) error {
	_ = s.SetWriteDeadline(time.Now().Add(StreamWriteTimeout))
	defer s.SetWriteDeadline(time.Time{})
	return WriteMessage(s, BuildError(code, msg))
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		_ = h.sendError(s, ErrBadRequest, "invalid payload for REQUEST_MANIFEST")
		return
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		_ = h.sendError(s, ErrContentNotFound, "manifest not found")
		return
	}

	resp := BuildManifest(contentID, manifestData)
	_ = s.SetWriteDeadline(time.Now().Add(StreamWriteTimeout))
	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
	}
	_ = s.SetWriteDeadline(time.Time{})
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message, messageCount *int) {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		_ = h.sendError(s, ErrBadRequest, "invalid payload for REQUEST_CHUNK")
		return
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		_ = h.sendError(s, ErrChunkNotFound, "chunk not found")
		return
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		_ = h.sendError(s, ErrInternal, "failed to build chunk message")
		return
	}

	_ = s.SetWriteDeadline(time.Now().Add(StreamWriteTimeout))
	if err := WriteMessage(s, resp); err != nil {
		_ = s.SetWriteDeadline(time.Time{})
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return
	}
	_ = s.SetWriteDeadline(time.Time{})

	// 5. Wait for ACK synchronously (sequential protocol requirement)
	_ = s.SetReadDeadline(time.Now().Add(StreamReadTimeout))
	ackTimer := time.AfterFunc(StreamReadTimeout, func() {
		_ = s.Reset()
	})
	ackMsg, err := ReadMessage(s)
	ackTimer.Stop()
	_ = s.SetReadDeadline(time.Time{})
	if err != nil {
		log.Printf("[Chunk Protocol] Error reading ACK: %v", err)
		return
	}
	*messageCount++

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
