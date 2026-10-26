package chunk

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/content/engine"
	"cipher/internal/protocol"
)

var TestCorruptProb float64

type streamTracker struct {
	incomingCount int
	outgoingCount int
	txCount       int
}

func (st *streamTracker) totalMessages() int {
	return st.incomingCount + st.outgoingCount
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

func (h *StreamHandler) writeMessage(s network.Stream, tracker *streamTracker, msg *Message) error {
	if tracker.totalMessages() >= MaxMessagesPerStream {
		return fmt.Errorf("message limit reached")
	}
	if err := WriteMessage(s, msg); err != nil {
		return err
	}
	tracker.outgoingCount++
	return nil
}

func (h *StreamHandler) writeErrorAndClose(s network.Stream, tracker *streamTracker, code ErrorCode, msg string) {
	if tracker.totalMessages() < MaxMessagesPerStream {
		_ = WriteMessage(s, BuildError(code, msg))
		tracker.outgoingCount++
	}
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	log.Printf("[Chunk Protocol] New stream from %s", s.Conn().RemotePeer())

	tracker := &streamTracker{}

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

		tracker.incomingCount++
		if tracker.totalMessages() > MaxMessagesPerStream {
			log.Printf("[Chunk Protocol] Max messages per stream exceeded (%d > %d)", tracker.totalMessages(), MaxMessagesPerStream)
			h.writeErrorAndClose(s, tracker, ErrBadRequest, "message limit exceeded")
			return
		}

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			log.Printf("[Chunk Protocol] Unsupported version %d", msg.Version)
			h.writeErrorAndClose(s, tracker, ErrUnsupportedMessage, "unsupported message version")
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			if tracker.txCount >= MaxTransactionsPerStream {
				log.Printf("[Chunk Protocol] Max transactions per stream exceeded (%d >= %d)", tracker.txCount, MaxTransactionsPerStream)
				h.writeErrorAndClose(s, tracker, ErrBadRequest, "transaction limit exceeded")
				return
			}
			tracker.txCount++
			if err := h.handleRequestManifest(s, tracker, msg); err != nil {
				return
			}
		case MsgRequestChunk:
			if tracker.txCount >= MaxTransactionsPerStream {
				log.Printf("[Chunk Protocol] Max transactions per stream exceeded (%d >= %d)", tracker.txCount, MaxTransactionsPerStream)
				h.writeErrorAndClose(s, tracker, ErrBadRequest, "transaction limit exceeded")
				return
			}
			tracker.txCount++
			if err := h.handleRequestChunk(s, tracker, msg); err != nil {
				return
			}
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			h.writeErrorAndClose(s, tracker, ErrUnsupportedMessage, "unsupported message type")
			return
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, tracker *streamTracker, msg *Message) error {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		h.writeErrorAndClose(s, tracker, ErrBadRequest, "invalid payload for REQUEST_MANIFEST")
		return err
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		h.writeErrorAndClose(s, tracker, ErrContentNotFound, "manifest not found")
		return err
	}

	resp := BuildManifest(contentID, manifestData)
	if err := h.writeMessage(s, tracker, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
		return err
	}
	return nil
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, tracker *streamTracker, msg *Message) error {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		h.writeErrorAndClose(s, tracker, ErrBadRequest, "invalid payload for REQUEST_CHUNK")
		return err
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		h.writeErrorAndClose(s, tracker, ErrChunkNotFound, "chunk not found")
		return err
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		h.writeErrorAndClose(s, tracker, ErrInternal, "failed to build chunk message")
		return err
	}

	if err := h.writeMessage(s, tracker, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return err
	}

	// Wait for ACK synchronously (sequential protocol requirement)
	ackMsg, err := ReadMessage(s)
	if err != nil {
		log.Printf("[Chunk Protocol] Error reading ACK: %v", err)
		return err
	}

	tracker.incomingCount++
	if tracker.totalMessages() > MaxMessagesPerStream {
		log.Printf("[Chunk Protocol] Max messages per stream exceeded after ACK (%d > %d)", tracker.totalMessages(), MaxMessagesPerStream)
		h.writeErrorAndClose(s, tracker, ErrBadRequest, "message limit exceeded")
		return fmt.Errorf("message limit exceeded")
	}

	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		log.Printf("[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, msgStr)
		return fmt.Errorf("client reported error: %s", msgStr)
	}
	if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK, got type %d", ackMsg.Type)
		h.writeErrorAndClose(s, tracker, ErrBadRequest, "expected ACK message")
		return fmt.Errorf("unexpected message type %d", ackMsg.Type)
	}

	return nil
}
