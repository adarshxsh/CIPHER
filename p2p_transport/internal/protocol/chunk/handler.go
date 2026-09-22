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

// StreamHandler handles incoming chunk protocol streams.
type StreamHandler struct {
	host   host.Host
	engine *engine.ContentEngine

	ReadTimeout              time.Duration
	WriteTimeout             time.Duration
	MaxTransactionsPerStream int
	MaxMessagesPerStream     int
}

// Option configures StreamHandler parameters.
type Option func(*StreamHandler)

// WithReadTimeout sets the per-operation read deadline duration.
func WithReadTimeout(d time.Duration) Option {
	return func(h *StreamHandler) {
		h.ReadTimeout = d
	}
}

// WithWriteTimeout sets the per-operation write deadline duration.
func WithWriteTimeout(d time.Duration) Option {
	return func(h *StreamHandler) {
		h.WriteTimeout = d
	}
}

// WithMaxTransactionsPerStream sets the maximum allowed completed transactions per stream.
func WithMaxTransactionsPerStream(n int) Option {
	return func(h *StreamHandler) {
		h.MaxTransactionsPerStream = n
	}
}

// WithMaxMessagesPerStream sets the maximum allowed total incoming messages read per stream.
func WithMaxMessagesPerStream(n int) Option {
	return func(h *StreamHandler) {
		h.MaxMessagesPerStream = n
	}
}

// NewStreamHandler creates and registers a new StreamHandler with configurable limits and timeouts.
func NewStreamHandler(h host.Host, eng *engine.ContentEngine, opts ...Option) *StreamHandler {
	handler := &StreamHandler{
		host:                     h,
		engine:                   eng,
		ReadTimeout:              DefaultReadTimeout,
		WriteTimeout:             DefaultWriteTimeout,
		MaxTransactionsPerStream: MaxTransactionsPerStream,
		MaxMessagesPerStream:     MaxMessagesPerStream,
	}
	for _, opt := range opts {
		opt(handler)
	}
	if h != nil {
		h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	}
	return handler
}

func (h *StreamHandler) readMessage(s network.Stream) (*Message, error) {
	if h.ReadTimeout > 0 {
		if err := s.SetReadDeadline(time.Now().Add(h.ReadTimeout)); err != nil {
			return nil, err
		}
	}
	return ReadMessage(s)
}

func (h *StreamHandler) writeMessage(s network.Stream, msg *Message) error {
	if h.WriteTimeout > 0 {
		if err := s.SetWriteDeadline(time.Now().Add(h.WriteTimeout)); err != nil {
			return err
		}
	}
	return WriteMessage(s, msg)
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer func() {
		_ = s.SetReadDeadline(time.Time{})
		_ = s.Close()
	}()

	log.Printf("[Chunk Protocol] New stream from %s", s.Conn().RemotePeer())

	msgCount := 0
	txCount := 0

	for {
		if h.MaxTransactionsPerStream > 0 && txCount >= h.MaxTransactionsPerStream {
			log.Printf("[Chunk Protocol] Stream from %s reached maximum transactions limit (%d/%d)",
				s.Conn().RemotePeer(), txCount, h.MaxTransactionsPerStream)
			return
		}

		msg, err := h.readMessage(s)
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", s.Conn().RemotePeer())
				return
			}
			log.Printf("[Chunk Protocol] Error reading message from %s: %v", s.Conn().RemotePeer(), err)
			_ = s.Reset()
			return
		}

		msgCount++
		if h.MaxMessagesPerStream > 0 && msgCount > h.MaxMessagesPerStream {
			log.Printf("[Chunk Protocol] Stream from %s exceeded message limit (%d/%d)",
				s.Conn().RemotePeer(), msgCount, h.MaxMessagesPerStream)
			_ = h.writeMessage(s, BuildError(ErrPermissionDenied, "message limit exceeded"))
			_ = s.Reset()
			return
		}

		if msg.Version != CurrentMessageVersion {
			log.Printf("[Chunk Protocol] Unsupported version %d from %s", msg.Version, s.Conn().RemotePeer())
			_ = h.writeMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		var txDone bool
		switch msg.Type {
		case MsgRequestManifest:
			txDone = h.handleRequestManifest(s, msg)
		case MsgRequestChunk:
			txDone = h.handleRequestChunk(s, msg, &msgCount)
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d from %s", msg.Type, s.Conn().RemotePeer())
			_ = h.writeMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
			return
		}

		if !txDone {
			return
		}

		txCount++
		if h.MaxTransactionsPerStream > 0 && txCount >= h.MaxTransactionsPerStream {
			log.Printf("[Chunk Protocol] Stream from %s completed maximum transactions (%d/%d)",
				s.Conn().RemotePeer(), txCount, h.MaxTransactionsPerStream)
			return
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) bool {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		_ = h.writeMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		return false
	}

	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		_ = h.writeMessage(s, BuildError(ErrContentNotFound, "manifest not found"))
		return false
	}

	resp := BuildManifest(contentID, manifestData)
	if err := h.writeMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
		return false
	}
	return true
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message, msgCount *int) bool {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		_ = h.writeMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		return false
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		_ = h.writeMessage(s, BuildError(ErrChunkNotFound, "chunk not found"))
		return false
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		_ = h.writeMessage(s, BuildError(ErrInternal, "failed to build chunk message"))
		return false
	}

	if err := h.writeMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return false
	}

	// Wait for ACK synchronously
	ackMsg, err := h.readMessage(s)
	if err != nil {
		log.Printf("[Chunk Protocol] Error reading ACK from %s: %v", s.Conn().RemotePeer(), err)
		_ = s.Reset()
		return false
	}

	(*msgCount)++
	if h.MaxMessagesPerStream > 0 && *msgCount > h.MaxMessagesPerStream {
		log.Printf("[Chunk Protocol] Stream from %s exceeded message limit on ACK (%d/%d)",
			s.Conn().RemotePeer(), *msgCount, h.MaxMessagesPerStream)
		_ = h.writeMessage(s, BuildError(ErrPermissionDenied, "message limit exceeded"))
		_ = s.Reset()
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
