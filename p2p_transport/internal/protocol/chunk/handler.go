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

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	log.Printf("[Chunk Protocol] New stream from %s", s.Conn().RemotePeer())

	_ = s.SetDeadline(time.Now().Add(DefaultStreamDeadline))

	msgCount := 0
	txCount := 0

	for {
		_ = s.SetDeadline(time.Now().Add(DefaultStreamDeadline))
		msg, err := ReadMessage(s)
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", s.Conn().RemotePeer())
				return
			}
			log.Printf("[Chunk Protocol] Error reading message: %v", err)
			return
		}

		msgCount++
		if msgCount > MaxMessagesPerStream {
			log.Printf("[Chunk Protocol] Stream message limit exceeded (%d > %d) from %s", msgCount, MaxMessagesPerStream, s.Conn().RemotePeer())
			s.Reset()
			return
		}

		if txCount >= MaxTransactionsPerStream {
			log.Printf("[Chunk Protocol] Stream transaction limit exceeded (%d >= %d) from %s", txCount, MaxTransactionsPerStream, s.Conn().RemotePeer())
			s.Reset()
			return
		}

		txCount++

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			log.Printf("[Chunk Protocol] Unsupported version %d", msg.Version)
			msgCount++
			if msgCount <= MaxMessagesPerStream {
				_ = s.SetDeadline(time.Now().Add(DefaultStreamDeadline))
				_ = WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			}
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg, &msgCount)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg, &msgCount)
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			msgCount++
			if msgCount <= MaxMessagesPerStream {
				_ = s.SetDeadline(time.Now().Add(DefaultStreamDeadline))
				_ = WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
			}
		}

		if txCount >= MaxTransactionsPerStream {
			return
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message, msgCount *int) {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		*msgCount++
		if *msgCount <= MaxMessagesPerStream {
			_ = s.SetDeadline(time.Now().Add(DefaultStreamDeadline))
			_ = WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		}
		return
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		*msgCount++
		if *msgCount <= MaxMessagesPerStream {
			_ = s.SetDeadline(time.Now().Add(DefaultStreamDeadline))
			_ = WriteMessage(s, BuildError(ErrContentNotFound, "manifest not found"))
		}
		return
	}

	resp := BuildManifest(contentID, manifestData)
	*msgCount++
	if *msgCount <= MaxMessagesPerStream {
		_ = s.SetDeadline(time.Now().Add(DefaultStreamDeadline))
		if err := WriteMessage(s, resp); err != nil {
			log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
		}
	}
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message, msgCount *int) {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		*msgCount++
		if *msgCount <= MaxMessagesPerStream {
			_ = s.SetDeadline(time.Now().Add(DefaultStreamDeadline))
			_ = WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		}
		return
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		*msgCount++
		if *msgCount <= MaxMessagesPerStream {
			_ = s.SetDeadline(time.Now().Add(DefaultStreamDeadline))
			_ = WriteMessage(s, BuildError(ErrChunkNotFound, "chunk not found"))
		}
		return
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		*msgCount++
		if *msgCount <= MaxMessagesPerStream {
			_ = s.SetDeadline(time.Now().Add(DefaultStreamDeadline))
			_ = WriteMessage(s, BuildError(ErrInternal, "failed to build chunk message"))
		}
		return
	}

	*msgCount++
	if *msgCount > MaxMessagesPerStream {
		log.Printf("[Chunk Protocol] Stream message limit exceeded before writing CHUNK")
		s.Reset()
		return
	}

	_ = s.SetDeadline(time.Now().Add(DefaultStreamDeadline))
	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return
	}

	// 5. Wait for ACK synchronously (sequential protocol requirement)
	*msgCount++
	if *msgCount > MaxMessagesPerStream {
		log.Printf("[Chunk Protocol] Stream message limit exceeded before reading ACK (%d > %d) from %s", *msgCount, MaxMessagesPerStream, s.Conn().RemotePeer())
		s.Reset()
		return
	}

	_ = s.SetDeadline(time.Now().Add(DefaultStreamDeadline))
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
