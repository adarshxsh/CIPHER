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
	host         host.Host
	engine       *engine.ContentEngine
	readTimeout  time.Duration
	writeTimeout time.Duration
	maxTx        int
	maxMsgs      int
}

type Option func(*StreamHandler)

func WithReadTimeout(d time.Duration) Option {
	return func(h *StreamHandler) {
		h.readTimeout = d
	}
}

func WithWriteTimeout(d time.Duration) Option {
	return func(h *StreamHandler) {
		h.writeTimeout = d
	}
}

func WithMaxTransactions(n int) Option {
	return func(h *StreamHandler) {
		h.maxTx = n
	}
}

func WithMaxMessages(n int) Option {
	return func(h *StreamHandler) {
		h.maxMsgs = n
	}
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine, opts ...Option) *StreamHandler {
	handler := &StreamHandler{
		host:         h,
		engine:       eng,
		readTimeout:  DefaultReadTimeout,
		writeTimeout: DefaultWriteTimeout,
		maxTx:        MaxTransactionsPerStream,
		maxMsgs:      MaxMessagesPerStream,
	}
	for _, opt := range opts {
		opt(handler)
	}
	if handler.readTimeout <= 0 {
		handler.readTimeout = DefaultReadTimeout
	}
	if handler.writeTimeout <= 0 {
		handler.writeTimeout = DefaultWriteTimeout
	}
	if handler.maxTx <= 0 {
		handler.maxTx = MaxTransactionsPerStream
	}
	if handler.maxMsgs <= 0 {
		handler.maxMsgs = MaxMessagesPerStream
	}

	h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	return handler
}

func (h *StreamHandler) writeMessage(s network.Stream, msg *Message) error {
	_ = s.SetWriteDeadline(time.Now().Add(h.writeTimeout))
	writeTimer := time.AfterFunc(h.writeTimeout, func() {
		_ = s.Close()
	})
	defer writeTimer.Stop()
	return WriteMessage(s, msg)
}

func (h *StreamHandler) writeError(s network.Stream, code ErrorCode, msgStr string) {
	_ = h.writeMessage(s, BuildError(code, msgStr))
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	log.Printf("[Chunk Protocol] New stream from %s", s.Conn().RemotePeer())

	var readMessageCount int
	var transactionCount int

	for {
		if h.maxTx > 0 && transactionCount >= h.maxTx {
			log.Printf("[Chunk Protocol] Stream reached transaction cap (%d)", transactionCount)
			return
		}
		if h.maxMsgs > 0 && readMessageCount >= h.maxMsgs {
			log.Printf("[Chunk Protocol] Stream reached message cap (%d)", readMessageCount)
			return
		}

		_ = s.SetReadDeadline(time.Now().Add(h.readTimeout))
		readTimer := time.AfterFunc(h.readTimeout, func() {
			_ = s.Close()
		})
		msg, err := ReadMessage(s)
		readTimer.Stop()

		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", s.Conn().RemotePeer())
				return
			}
			log.Printf("[Chunk Protocol] Error reading message: %v", err)
			return
		}
		readMessageCount++

		if h.maxMsgs > 0 && readMessageCount > h.maxMsgs {
			log.Printf("[Chunk Protocol] Stream exceeded message cap (%d)", readMessageCount)
			h.writeError(s, ErrPermissionDenied, "max messages per stream exceeded")
			return
		}

		if h.maxTx > 0 && transactionCount >= h.maxTx {
			log.Printf("[Chunk Protocol] Stream exceeded transaction cap (%d)", transactionCount)
			h.writeError(s, ErrPermissionDenied, "max transactions per stream exceeded")
			return
		}

		if msg.Version != CurrentMessageVersion {
			log.Printf("[Chunk Protocol] Unsupported version %d", msg.Version)
			h.writeError(s, ErrUnsupportedMessage, "unsupported message version")
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg)
			transactionCount++
		case MsgRequestChunk:
			completed := h.handleRequestChunk(s, msg, &readMessageCount)
			if completed {
				transactionCount++
			}
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			h.writeError(s, ErrUnsupportedMessage, "unsupported message type")
			return
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		h.writeError(s, ErrBadRequest, "invalid payload for REQUEST_MANIFEST")
		return
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		h.writeError(s, ErrContentNotFound, "manifest not found")
		return
	}

	resp := BuildManifest(contentID, manifestData)
	if err := h.writeMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
	}
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message, readMessageCount *int) bool {
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

	if err := h.writeMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return false
	}

	// Wait for ACK synchronously (sequential protocol requirement)
	_ = s.SetReadDeadline(time.Now().Add(h.readTimeout))
	ackTimer := time.AfterFunc(h.readTimeout, func() {
		_ = s.Close()
	})
	ackMsg, err := ReadMessage(s)
	ackTimer.Stop()
	if err != nil {
		log.Printf("[Chunk Protocol] Error reading ACK: %v", err)
		return false
	}
	*readMessageCount++

	if h.maxMsgs > 0 && *readMessageCount > h.maxMsgs {
		log.Printf("[Chunk Protocol] Stream exceeded message cap during ACK (%d)", *readMessageCount)
		h.writeError(s, ErrPermissionDenied, "max messages per stream exceeded")
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
