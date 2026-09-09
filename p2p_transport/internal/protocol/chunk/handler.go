package chunk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/content/engine"
	"cipher/internal/protocol"
)

var TestCorruptProb float64

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
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
	if h != nil {
		h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	}
	return handler
}

// HandleStream processes an incoming stream.
func (h *StreamHandler) HandleStream(s network.Stream) {
	h.handleStream(s)
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	peerID := s.Conn().RemotePeer()
	log.Printf("[Chunk Protocol] New stream from %s", peerID)

	for {
		if err := s.SetReadDeadline(time.Now().Add(DefaultReadDeadline)); err != nil {
			log.Printf("[Chunk Protocol] Failed to set read deadline for peer %s: %v", peerID, err)
			return
		}

		msg, err := ReadMessage(s)
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", peerID)
				return
			}
			if isTimeout(err) {
				log.Printf("[Chunk Protocol] Read deadline expired for peer %s: %v", peerID, err)
			} else {
				log.Printf("[Chunk Protocol] Error reading message: %v", err)
			}
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
			if err := h.handleRequestManifest(s, msg); err != nil {
				log.Printf("[Chunk Protocol] Stream terminating after manifest error from peer %s: %v", peerID, err)
				return
			}
		case MsgRequestChunk:
			if err := h.handleRequestChunk(s, msg); err != nil {
				log.Printf("[Chunk Protocol] Stream terminating after chunk error from peer %s: %v", peerID, err)
				return
			}
		default:
			log.Printf("[Chunk Protocol] Unsupported message type %d from peer %s", msg.Type, peerID)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
			return
		}

		// Clear deadlines upon successful transaction completion
		_ = s.SetDeadline(time.Time{})
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) error {
	peerID := s.Conn().RemotePeer()
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		return fmt.Errorf("invalid payload: %w", err)
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		WriteMessage(s, BuildError(ErrContentNotFound, "manifest not found"))
		return fmt.Errorf("manifest not found: %w", err)
	}

	resp := BuildManifest(contentID, manifestData)
	if err := WriteMessage(s, resp); err != nil {
		if isTimeout(err) {
			log.Printf("[Chunk Protocol] Write deadline expired for MANIFEST response to peer %s: %v", peerID, err)
		} else {
			log.Printf("[Chunk Protocol] Error writing MANIFEST response to peer %s: %v", peerID, err)
		}
		return fmt.Errorf("write MANIFEST response failed: %w", err)
	}
	return nil
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message) error {
	peerID := s.Conn().RemotePeer()
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		return fmt.Errorf("invalid payload: %w", err)
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		WriteMessage(s, BuildError(ErrChunkNotFound, "chunk not found"))
		return fmt.Errorf("chunk not found: %w", err)
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		WriteMessage(s, BuildError(ErrInternal, "failed to build chunk message"))
		return fmt.Errorf("build chunk message failed: %w", err)
	}

	if err := WriteMessage(s, resp); err != nil {
		if isTimeout(err) {
			log.Printf("[Chunk Protocol] Write deadline expired for CHUNK response to peer %s: %v", peerID, err)
		} else {
			log.Printf("[Chunk Protocol] Error writing CHUNK response to peer %s: %v", peerID, err)
		}
		return fmt.Errorf("write CHUNK response failed: %w", err)
	}

	// 5. Wait for ACK synchronously with dedicated ACK deadline
	if err := s.SetReadDeadline(time.Now().Add(DefaultACKDeadline)); err != nil {
		log.Printf("[Chunk Protocol] Failed to set ACK deadline for peer %s: %v", peerID, err)
		return fmt.Errorf("set ACK deadline failed: %w", err)
	}

	ackMsg, err := ReadMessage(s)
	if err != nil {
		if isTimeout(err) {
			log.Printf("[Chunk Protocol] ACK deadline expired waiting for peer %s on chunk %x: %v", peerID, chunkID, err)
		} else {
			log.Printf("[Chunk Protocol] Error reading ACK from peer %s: %v", peerID, err)
		}
		return fmt.Errorf("read ACK failed: %w", err)
	}
	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		log.Printf("[Chunk Protocol] Client %s reported error on chunk %x: [%d] %s", peerID, chunkID, code, msgStr)
		return fmt.Errorf("client reported error: [%d] %s", code, msgStr)
	}
	if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK from peer %s, got type %d", peerID, ackMsg.Type)
		return fmt.Errorf("expected ACK, got type %d", ackMsg.Type)
	}

	return nil
}
