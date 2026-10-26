package chunk

import (
	"context"
	"fmt"
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

type StreamHandlerOption func(*StreamHandler)

func WithStreamTimeout(timeout time.Duration) StreamHandlerOption {
	return func(h *StreamHandler) {
		h.timeout = timeout
	}
}

type StreamHandler struct {
	host    host.Host
	engine  *engine.ContentEngine
	timeout time.Duration
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine, opts ...StreamHandlerOption) *StreamHandler {
	handler := &StreamHandler{
		host:    h,
		engine:  eng,
		timeout: DefaultStreamTimeout,
	}
	for _, opt := range opts {
		opt(handler)
	}
	h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	return handler
}

func (h *StreamHandler) SetTimeout(d time.Duration) {
	h.timeout = d
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	log.Printf("[Chunk Protocol] New stream from %s", s.Conn().RemotePeer())

	var transactionCount int
	var messageCount int

	timeout := h.timeout
	if timeout == 0 {
		timeout = DefaultStreamTimeout
	}

	for {
		if transactionCount >= MaxTransactionsPerStream {
			log.Printf("[Chunk Protocol] Stream from %s reached max transactions (%d)", s.Conn().RemotePeer(), MaxTransactionsPerStream)
			return
		}
		if messageCount >= MaxMessagesPerStream {
			log.Printf("[Chunk Protocol] Stream from %s reached max messages (%d)", s.Conn().RemotePeer(), MaxMessagesPerStream)
			_ = s.Reset()
			return
		}

		if err := s.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			log.Printf("[Chunk Protocol] Failed to set read deadline for %s: %v", s.Conn().RemotePeer(), err)
			_ = s.Reset()
			return
		}

		msg, err := ReadMessage(s)
		_ = s.SetReadDeadline(time.Time{})

		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", s.Conn().RemotePeer())
				return
			}
			log.Printf("[Chunk Protocol] Read error on stream from %s: %v", s.Conn().RemotePeer(), err)
			_ = s.Reset()
			return
		}

		messageCount++
		if messageCount > MaxMessagesPerStream {
			log.Printf("[Chunk Protocol] Stream from %s exceeded max messages (%d)", s.Conn().RemotePeer(), MaxMessagesPerStream)
			_ = s.Reset()
			return
		}

		if msg.Version != CurrentMessageVersion {
			log.Printf("[Chunk Protocol] Unsupported version %d from %s", msg.Version, s.Conn().RemotePeer())
			_ = s.SetWriteDeadline(time.Now().Add(timeout))
			_ = WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			_ = s.SetWriteDeadline(time.Time{})
			_ = s.Reset()
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			if err := h.handleRequestManifest(s, msg, timeout); err != nil {
				log.Printf("[Chunk Protocol] Error handling manifest request: %v", err)
				_ = s.Reset()
				return
			}
			transactionCount++

		case MsgRequestChunk:
			if err := h.handleRequestChunk(s, msg, &messageCount, timeout); err != nil {
				log.Printf("[Chunk Protocol] Error handling chunk request: %v", err)
				_ = s.Reset()
				return
			}
			transactionCount++

		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d from %s", msg.Type, s.Conn().RemotePeer())
			_ = s.SetWriteDeadline(time.Now().Add(timeout))
			_ = WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
			_ = s.SetWriteDeadline(time.Time{})
			_ = s.Reset()
			return
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message, timeout time.Duration) error {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(timeout))
		_ = WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		_ = s.SetWriteDeadline(time.Time{})
		return nil
	}

	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(timeout))
		_ = WriteMessage(s, BuildError(ErrContentNotFound, "manifest not found"))
		_ = s.SetWriteDeadline(time.Time{})
		return nil
	}

	resp := BuildManifest(contentID, manifestData)
	_ = s.SetWriteDeadline(time.Now().Add(timeout))
	err = WriteMessage(s, resp)
	_ = s.SetWriteDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("error writing MANIFEST response: %w", err)
	}
	return nil
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message, messageCount *int, timeout time.Duration) error {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(timeout))
		_ = WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		_ = s.SetWriteDeadline(time.Time{})
		return nil
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(timeout))
		_ = WriteMessage(s, BuildError(ErrChunkNotFound, "chunk not found"))
		_ = s.SetWriteDeadline(time.Time{})
		return nil
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(timeout))
		_ = WriteMessage(s, BuildError(ErrInternal, "failed to build chunk message"))
		_ = s.SetWriteDeadline(time.Time{})
		return nil
	}

	_ = s.SetWriteDeadline(time.Now().Add(timeout))
	err = WriteMessage(s, resp)
	_ = s.SetWriteDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("error writing CHUNK response: %w", err)
	}

	// Wait for ACK synchronously (sequential protocol requirement)
	if *messageCount >= MaxMessagesPerStream {
		return fmt.Errorf("max messages per stream reached before reading ACK")
	}

	if err := s.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("failed to set read deadline for ACK: %w", err)
	}

	ackMsg, err := ReadMessage(s)
	_ = s.SetReadDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("error reading ACK: %w", err)
	}

	*messageCount++

	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		log.Printf("[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, msgStr)
		return nil
	}
	if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK, got type %d", ackMsg.Type)
		_ = s.SetWriteDeadline(time.Now().Add(timeout))
		_ = WriteMessage(s, BuildError(ErrUnsupportedMessage, "expected ACK"))
		_ = s.SetWriteDeadline(time.Time{})
		return fmt.Errorf("expected ACK message type, got %d", ackMsg.Type)
	}

	return nil
}
