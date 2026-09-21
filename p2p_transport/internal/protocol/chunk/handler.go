package chunk

import (
	"context"
	"errors"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/content/engine"
	"cipher/internal/protocol"
)

var TestCorruptProb float64

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

func isTimeoutErr(err error) bool {
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
	errMsg := err.Error()
	return strings.Contains(errMsg, "i/o timeout") || strings.Contains(errMsg, "deadline exceeded")
}

func readMessageWithDeadline(s network.Stream) (*Message, error) {
	if err := s.SetReadDeadline(time.Now().Add(DefaultReadTimeout)); err != nil {
		return nil, err
	}
	return ReadMessage(s)
}

func writeMessageWithDeadline(s network.Stream, msg *Message) error {
	if err := s.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout)); err != nil {
		return err
	}
	return WriteMessage(s, msg)
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	log.Printf("[Chunk Protocol] New stream from %s", s.Conn().RemotePeer())

	for {
		msg, err := readMessageWithDeadline(s)
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" || isTimeoutErr(err) {
				log.Printf("[Chunk Protocol] Stream closed by %s (reason: %v)", s.Conn().RemotePeer(), err)
				return
			}
			log.Printf("[Chunk Protocol] Error reading message: %v", err)
			return
		}

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			log.Printf("[Chunk Protocol] Unsupported version %d", msg.Version)
			_ = writeMessageWithDeadline(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg)
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			_ = writeMessageWithDeadline(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		_ = writeMessageWithDeadline(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		return
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		_ = writeMessageWithDeadline(s, BuildError(ErrContentNotFound, "manifest not found"))
		return
	}

	resp := BuildManifest(contentID, manifestData)
	if err := writeMessageWithDeadline(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
	}
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message) {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		_ = writeMessageWithDeadline(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		return
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		_ = writeMessageWithDeadline(s, BuildError(ErrChunkNotFound, "chunk not found"))
		return
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		_ = writeMessageWithDeadline(s, BuildError(ErrInternal, "failed to build chunk message"))
		return
	}

	if err := writeMessageWithDeadline(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return
	}

	// 5. Wait for ACK synchronously (sequential protocol requirement)
	ackMsg, err := readMessageWithDeadline(s)
	if err != nil {
		if isTimeoutErr(err) || err == io.EOF || err.Error() == "stream reset" {
			log.Printf("[Chunk Protocol] Stream closed waiting for ACK: %v", err)
		} else {
			log.Printf("[Chunk Protocol] Error reading ACK: %v", err)
		}
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
