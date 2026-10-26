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

func writeMessageWithDeadline(s network.Stream, msg *Message) error {
	_ = s.SetWriteDeadline(time.Now().Add(StreamWriteTimeout))
	err := WriteMessage(s, msg)
	_ = s.SetWriteDeadline(time.Time{})
	return err
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	remotePeer := s.Conn().RemotePeer()
	log.Printf("[Chunk Protocol] New stream from %s", remotePeer)

	for {
		_ = s.SetReadDeadline(time.Now().Add(StreamReadTimeout))
		msg, err := ReadMessage(s)
		_ = s.SetReadDeadline(time.Time{})
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", remotePeer)
				return
			}
			log.Printf("[Chunk Protocol] Error reading message from %s: %v", remotePeer, err)
			return
		}

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			log.Printf("[Chunk Protocol] Unsupported version %d from %s", msg.Version, remotePeer)
			writeMessageWithDeadline(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg)
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d from %s", msg.Type, remotePeer)
			writeMessageWithDeadline(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		writeMessageWithDeadline(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		return
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		writeMessageWithDeadline(s, BuildError(ErrContentNotFound, "manifest not found"))
		return
	}

	resp := BuildManifest(contentID, manifestData)
	if err := writeMessageWithDeadline(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response to %s: %v", s.Conn().RemotePeer(), err)
	}
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message) {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		writeMessageWithDeadline(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		return
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		writeMessageWithDeadline(s, BuildError(ErrChunkNotFound, "chunk not found"))
		return
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		writeMessageWithDeadline(s, BuildError(ErrInternal, "failed to build chunk message"))
		return
	}

	if err := writeMessageWithDeadline(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response to %s: %v", s.Conn().RemotePeer(), err)
		return
	}

	// 5. Wait for ACK synchronously (sequential protocol requirement)
	_ = s.SetReadDeadline(time.Now().Add(AckTimeout))
	ackMsg, err := ReadMessage(s)
	_ = s.SetReadDeadline(time.Time{})
	if err != nil {
		log.Printf("[Chunk Protocol] Error reading ACK from %s: %v", s.Conn().RemotePeer(), err)
		return
	}
	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		log.Printf("[Chunk Protocol] Client %s reported error on chunk %x: [%d] %s", s.Conn().RemotePeer(), chunkID, code, msgStr)
		return
	}
	if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK from %s, got type %d", s.Conn().RemotePeer(), ackMsg.Type)
		return
	}
}
