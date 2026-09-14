package chunk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"

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

	var msgCount int
	var txCount int

	for {
		if msgCount >= MaxMessagesPerStream {
			log.Printf("[Chunk Protocol] Stream from %s reached message limit (%d)", s.Conn().RemotePeer(), msgCount)
			return
		}

		msg, err := ReadMessage(s)
		if err != nil {
			if err == io.EOF || errors.Is(err, io.EOF) || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", s.Conn().RemotePeer())
				return
			}
			log.Printf("[Chunk Protocol] Error reading message: %v", err)
			return
		}

		msgCount++
		if msgCount > MaxMessagesPerStream {
			log.Printf("[Chunk Protocol] Stream from %s exceeded message limit (%d)", s.Conn().RemotePeer(), msgCount)
			WriteMessage(s, BuildError(ErrBadRequest, "message limit per stream exceeded"))
			return
		}

		if txCount >= MaxTransactionsPerStream {
			log.Printf("[Chunk Protocol] Stream from %s attempted additional transaction (limit %d)", s.Conn().RemotePeer(), MaxTransactionsPerStream)
			msgCount++
			WriteMessage(s, BuildError(ErrBadRequest, "transaction limit per stream exceeded"))
			return
		}

		if msg.Version != CurrentMessageVersion {
			log.Printf("[Chunk Protocol] Unsupported version %d", msg.Version)
			msgCount++
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			if err := h.handleRequestManifest(s, msg, &msgCount, &txCount); err != nil {
				return
			}
		case MsgRequestChunk:
			if err := h.handleRequestChunk(s, msg, &msgCount, &txCount); err != nil {
				return
			}
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			msgCount++
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
			return
		}

		if txCount >= MaxTransactionsPerStream {
			log.Printf("[Chunk Protocol] Stream from %s completed max transactions (%d)", s.Conn().RemotePeer(), txCount)
			return
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message, msgCount *int, txCount *int) error {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		*msgCount++
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		*txCount++
		return fmt.Errorf("invalid payload: %w", err)
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		*msgCount++
		WriteMessage(s, BuildError(ErrContentNotFound, "manifest not found"))
		*txCount++
		return nil
	}

	resp := BuildManifest(contentID, manifestData)
	*msgCount++
	if *msgCount > MaxMessagesPerStream {
		WriteMessage(s, BuildError(ErrBadRequest, "message limit per stream exceeded"))
		return fmt.Errorf("message limit exceeded")
	}

	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
		return err
	}

	*txCount++
	return nil
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message, msgCount *int, txCount *int) error {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		*msgCount++
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		*txCount++
		return fmt.Errorf("invalid payload: %w", err)
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		*msgCount++
		WriteMessage(s, BuildError(ErrChunkNotFound, "chunk not found"))
		*txCount++
		return nil
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		*msgCount++
		WriteMessage(s, BuildError(ErrInternal, "failed to build chunk message"))
		*txCount++
		return fmt.Errorf("build chunk failed: %w", err)
	}

	*msgCount++
	if *msgCount > MaxMessagesPerStream {
		WriteMessage(s, BuildError(ErrBadRequest, "message limit per stream exceeded"))
		return fmt.Errorf("message limit exceeded")
	}

	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return err
	}

	// Wait for ACK synchronously (sequential protocol requirement)
	ackMsg, err := ReadMessage(s)
	if err != nil {
		log.Printf("[Chunk Protocol] Error reading ACK: %v", err)
		return err
	}

	*msgCount++
	if *msgCount > MaxMessagesPerStream {
		WriteMessage(s, BuildError(ErrBadRequest, "message limit per stream exceeded"))
		return fmt.Errorf("message limit exceeded")
	}

	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		log.Printf("[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, msgStr)
		*txCount++
		return nil
	}

	if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK, got type %d", ackMsg.Type)
		*txCount++
		return fmt.Errorf("expected ACK, got type %d", ackMsg.Type)
	}

	*txCount++
	return nil
}

