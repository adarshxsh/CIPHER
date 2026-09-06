package chunk

import (
	"context"
	"io"
	"log"
	"math/rand"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"

	"cipher/internal/content/engine"
	"cipher/internal/protocol"
	"cipher/internal/ratelimit"
	"cipher/internal/sanitizer"
)

var TestCorruptProb float64

type StreamHandler struct {
	host       host.Host
	engine     *engine.ContentEngine
	logLimiter *ratelimit.PeerRateLimiter
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine) *StreamHandler {
	handler := &StreamHandler{
		host:       h,
		engine:     eng,
		logLimiter: ratelimit.DefaultPeerRateLimiter(),
	}
	h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	return handler
}

func (h *StreamHandler) SetLogRateLimit(r rate.Limit, b int) {
	if h.logLimiter != nil {
		h.logLimiter.SetLimit(r, b)
	}
}

func (h *StreamHandler) allowErrorLog(peerID peer.ID) bool {
	if h == nil || h.logLimiter == nil {
		return true
	}
	return h.logLimiter.Allow(peerID)
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	remotePeer := s.Conn().RemotePeer()
	log.Printf("[Chunk Protocol] New stream from %s", remotePeer)

	for {
		msg, err := ReadMessage(s)
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", remotePeer)
				return
			}
			if h.allowErrorLog(remotePeer) {
				log.Printf("[Chunk Protocol] Error reading message from %s: %v", remotePeer, err)
			}
			return
		}

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			if h.allowErrorLog(remotePeer) {
				log.Printf("[Chunk Protocol] Unsupported version %d from %s", msg.Version, remotePeer)
			}
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg)
		case MsgError:
			h.handleErrorMessage(s, msg)
		default:
			if h.allowErrorLog(remotePeer) {
				log.Printf("[Chunk Protocol] Unsupported message type: %d from %s", msg.Type, remotePeer)
			}
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
		}
	}
}

func (h *StreamHandler) handleErrorMessage(s network.Stream, msg *Message) {
	remotePeer := s.Conn().RemotePeer()
	code, msgStr, err := ParseError(msg.Payload)
	if err != nil {
		if h.allowErrorLog(remotePeer) {
			log.Printf("[Chunk Protocol] Error parsing ERROR payload from %s: %v", remotePeer, err)
		}
		return
	}
	sanitizedMsg := sanitizer.Sanitize(msgStr)
	if h.allowErrorLog(remotePeer) {
		log.Printf("[Chunk Protocol] Remote peer %s reported error: [%d] %s", remotePeer, code, sanitizedMsg)
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
		remotePeer := s.Conn().RemotePeer()
		if h.allowErrorLog(remotePeer) {
			log.Printf("[Chunk Protocol] Error writing MANIFEST response to %s: %v", remotePeer, err)
		}
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
		remotePeer := s.Conn().RemotePeer()
		if h.allowErrorLog(remotePeer) {
			log.Printf("[Chunk Protocol] Error writing CHUNK response to %s: %v", remotePeer, err)
		}
		return
	}

	// 5. Wait for ACK synchronously (sequential protocol requirement)
	ackMsg, err := ReadMessage(s)
	remotePeer := s.Conn().RemotePeer()
	if err != nil {
		if h.allowErrorLog(remotePeer) {
			log.Printf("[Chunk Protocol] Error reading ACK from %s: %v", remotePeer, err)
		}
		return
	}
	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		sanitizedMsg := sanitizer.Sanitize(msgStr)
		if h.allowErrorLog(remotePeer) {
			log.Printf("[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, sanitizedMsg)
		}
		return
	}
	if ackMsg.Type != MsgAck {
		if h.allowErrorLog(remotePeer) {
			log.Printf("[Chunk Protocol] Expected ACK from %s, got type %d", remotePeer, ackMsg.Type)
		}
		return
	}
}
