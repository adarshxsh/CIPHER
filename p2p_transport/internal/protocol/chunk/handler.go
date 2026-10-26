package chunk

import (
	"context"
	"io"
	"log"
	"math/rand"
	"sync"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/content/engine"
	"cipher/internal/protocol"
)

type StreamHandlerOptions struct {
	CorruptProb float64
}

type StreamHandlerOption func(*StreamHandler)

func WithCorruptProb(prob float64) StreamHandlerOption {
	return func(h *StreamHandler) {
		h.corruptProb = prob
	}
}

func WithTestCorruptProb(prob float64) StreamHandlerOption {
	return WithCorruptProb(prob)
}

type StreamHandler struct {
	host        host.Host
	engine      *engine.ContentEngine
	mu          sync.RWMutex
	corruptProb float64
}

func (h *StreamHandler) SetCorruptProb(prob float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.corruptProb = prob
}

func (h *StreamHandler) CorruptProb() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.corruptProb
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine, opts ...StreamHandlerOption) *StreamHandler {
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

	corruptProb := h.CorruptProb()
	if corruptProb > 0 && rand.Float64() < corruptProb && len(chunkData.Data) > 0 {
		// Corrupt a copy of the chunk for testing to avoid mutating engine memory
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		clonedChunk := *chunkData
		clonedData := make([]byte, len(chunkData.Data))
		copy(clonedData, chunkData.Data)
		clonedData[0] ^= 0xFF
		clonedChunk.Data = clonedData
		chunkData = &clonedChunk
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
