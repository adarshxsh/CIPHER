package chunk

import (
	"bytes"
	"context"
	"io"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/protocol"
)

// FaultHook is an optional function that can transform/corrupt a chunk payload for testing.
type FaultHook func(chunk *core.Chunk) *core.Chunk

// HandlerOption configures a StreamHandler instance.
type HandlerOption func(*StreamHandler)

// WithCorruptProbability sets the probability (0.0 to 1.0) of corrupting chunk responses for testing.
func WithCorruptProbability(prob float64) HandlerOption {
	return func(h *StreamHandler) {
		h.SetCorruptProbability(prob)
	}
}

// WithFaultHook sets a custom fault injection hook for testing.
func WithFaultHook(hook FaultHook) HandlerOption {
	return func(h *StreamHandler) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.faultHook = hook
	}
}

type StreamHandler struct {
	host   host.Host
	engine *engine.ContentEngine

	corruptProbBits uint64

	mu        sync.RWMutex
	faultHook FaultHook
}

// SetCorruptProbability sets the corruption probability in a thread-safe manner.
func (h *StreamHandler) SetCorruptProbability(prob float64) {
	if prob < 0 {
		prob = 0
	} else if prob > 1 {
		prob = 1
	}
	atomic.StoreUint64(&h.corruptProbBits, math.Float64bits(prob))
}

// CorruptProbability returns the current corruption probability in a thread-safe manner.
func (h *StreamHandler) CorruptProbability() float64 {
	bits := atomic.LoadUint64(&h.corruptProbBits)
	return math.Float64frombits(bits)
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine, opts ...HandlerOption) *StreamHandler {
	handler := &StreamHandler{
		host:   h,
		engine: eng,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(handler)
		}
	}
	h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	return handler
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	log.Printf("[Chunk Protocol] New stream from %s", s.Conn().RemotePeer())

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

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			log.Printf("[Chunk Protocol] Unsupported version %d", msg.Version)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg)
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
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

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message) {
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

	sendChunk := chunkData

	prob := h.CorruptProbability()

	h.mu.RLock()
	hook := h.faultHook
	h.mu.RUnlock()

	if hook != nil {
		sendChunk = hook(chunkData)
	} else if prob > 0 && rand.Float64() < prob && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing while cloning the byte buffer to prevent process memory corruption
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		clonedData := bytes.Clone(chunkData.Data)
		clonedData[0] ^= 0xFF
		sendChunk = &core.Chunk{
			Header: chunkData.Header,
			Data:   clonedData,
		}
	}

	resp, err := BuildChunk(sendChunk)
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
