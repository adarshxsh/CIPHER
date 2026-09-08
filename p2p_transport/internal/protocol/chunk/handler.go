package chunk

import (
	"context"
	"io"
	"log"
	"math/rand"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/engine"
	"cipher/internal/protocol"
	"cipher/internal/ratelimit"
)

var TestCorruptProb float64

type StreamHandler struct {
	host    host.Host
	engine  *engine.ContentEngine
	limiter *ratelimit.PeerRateLimiter
}

type StreamHandlerOption func(*StreamHandler)

func WithRateLimiter(limiter *ratelimit.PeerRateLimiter) StreamHandlerOption {
	return func(sh *StreamHandler) {
		sh.limiter = limiter
	}
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine, opts ...StreamHandlerOption) *StreamHandler {
	handler := &StreamHandler{
		host:    h,
		engine:  eng,
		limiter: ratelimit.DefaultPeerRateLimiter(),
	}
	for _, opt := range opts {
		opt(handler)
	}
	if h != nil {
		h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	}
	return handler
}

func (h *StreamHandler) allowLog(p peer.ID) bool {
	if h.limiter == nil {
		return true
	}
	return h.limiter.Allow(p)
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	var remotePeer peer.ID
	if s != nil && s.Conn() != nil {
		remotePeer = s.Conn().RemotePeer()
	}

	if h.allowLog(remotePeer) {
		log.Printf("[Chunk Protocol] New stream from %s", remotePeer)
	}

	for {
		msg, err := ReadMessage(s)
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				if h.allowLog(remotePeer) {
					log.Printf("[Chunk Protocol] Stream closed by %s", remotePeer)
				}
				return
			}
			if h.allowLog(remotePeer) {
				log.Printf("[Chunk Protocol] Error reading message: %v", err)
			}
			return
		}

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			if h.allowLog(remotePeer) {
				log.Printf("[Chunk Protocol] Unsupported version %d", msg.Version)
			}
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg)
		default:
			if h.allowLog(remotePeer) {
				log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			}
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) {
	var remotePeer peer.ID
	if s != nil && s.Conn() != nil {
		remotePeer = s.Conn().RemotePeer()
	}

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
		if h.allowLog(remotePeer) {
			log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
		}
	}
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message) {
	var remotePeer peer.ID
	if s != nil && s.Conn() != nil {
		remotePeer = s.Conn().RemotePeer()
	}

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

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		WriteMessage(s, BuildError(ErrInternal, "failed to build chunk message"))
		return
	}

	if err := WriteMessage(s, resp); err != nil {
		if h.allowLog(remotePeer) {
			log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		}
		return
	}

	// 5. Wait for ACK synchronously (sequential protocol requirement)
	ackMsg, err := ReadMessage(s)
	if err != nil {
		if h.allowLog(remotePeer) {
			log.Printf("[Chunk Protocol] Error reading ACK: %v", err)
		}
		return
	}
	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		if h.allowLog(remotePeer) {
			log.Printf("[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, msgStr)
		}
		return
	}
	if ackMsg.Type != MsgAck {
		if h.allowLog(remotePeer) {
			log.Printf("[Chunk Protocol] Expected ACK, got type %d", ackMsg.Type)
		}
		return
	}
}
