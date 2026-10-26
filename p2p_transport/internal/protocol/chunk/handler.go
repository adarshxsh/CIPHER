package chunk

import (
	"context"
	"io"
	"log"
	"math"
	"math/rand"
	"sync/atomic"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/content/engine"
	"cipher/internal/protocol"
)

// HandlerConfig defines configuration options for StreamHandler.
type HandlerConfig struct {
	CorruptProb float64
}

// Option defines a functional option for initializing a StreamHandler.
type Option func(*HandlerConfig)

// WithCorruptProb sets the corruption probability for fault injection testing.
func WithCorruptProb(prob float64) Option {
	return func(c *HandlerConfig) {
		if prob < 0 {
			prob = 0
		} else if prob > 1 {
			prob = 1
		}
		c.CorruptProb = prob
	}
}

// WithConfig sets the HandlerConfig for a StreamHandler.
func WithConfig(cfg HandlerConfig) Option {
	return func(c *HandlerConfig) {
		*c = cfg
	}
}

type StreamHandler struct {
	host            host.Host
	engine          *engine.ContentEngine
	corruptProbBits atomic.Uint64
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine, opts ...Option) *StreamHandler {
	var cfg HandlerConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	handler := &StreamHandler{
		host:   h,
		engine: eng,
	}
	handler.SetCorruptProb(cfg.CorruptProb)

	h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	return handler
}

func (h *StreamHandler) SetCorruptProb(prob float64) {
	if prob < 0 {
		prob = 0
	} else if prob > 1 {
		prob = 1
	}
	bits := math.Float64bits(prob)
	h.corruptProbBits.Store(bits)
}

func (h *StreamHandler) CorruptProb() float64 {
	bits := h.corruptProbBits.Load()
	return math.Float64frombits(bits)
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

	corruptProb := h.CorruptProb()
	if corruptProb > 0 && rand.Float64() < corruptProb && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing on a cloned byte slice
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkCopy := *chunkData
		chunkCopy.Data = make([]byte, len(chunkData.Data))
		copy(chunkCopy.Data, chunkData.Data)
		chunkCopy.Data[0] ^= 0xFF
		chunkData = &chunkCopy
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
