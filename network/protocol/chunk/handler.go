package chunk

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"

	"cipher/network/content/engine"
	"cipher/network/payments"
	"cipher/network/protocol"
)

var TestCorruptProb float64

// TicketHandlerFunc validates and stores a signed ticket received from a consumer.
type TicketHandlerFunc func(ticket *payments.SignedTicket) error

type StreamHandler struct {
	host          host.Host
	engine        *engine.ContentEngine
	ticketHandler TicketHandlerFunc
	rejectedPeers sync.Map
}

func (h *StreamHandler) SetTicketHandler(fn TicketHandlerFunc) {
	h.ticketHandler = fn
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
	remotePeer := s.Conn().RemotePeer()
	log.Printf("[Chunk Protocol] New stream from %s", remotePeer)

	if _, rejected := h.rejectedPeers.Load(remotePeer); rejected {
		log.Printf("[Chunk Protocol] Rejecting stream from rejected peer %s", remotePeer)
		_ = s.SetWriteDeadline(time.Now().Add(WriteTimeout))
		_ = WriteMessage(s, BuildError(ErrPermissionDenied, "peer rejected due to invalid ticket"))
		_ = s.SetWriteDeadline(time.Time{})
		return
	}

	var msgCount int
	var txCount int

	for {
		if txCount >= MaxTransactionsPerStream {
			log.Printf("[Chunk Protocol] Max transactions (%d) reached for stream from %s", MaxTransactionsPerStream, remotePeer)
			return
		}
		if msgCount >= MaxMessagesPerStream {
			log.Printf("[Chunk Protocol] Max messages (%d) reached for stream from %s", MaxMessagesPerStream, remotePeer)
			return
		}

		_ = s.SetReadDeadline(time.Now().Add(ReadTimeout))
		msg, err := ReadMessage(s)
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", remotePeer)
				return
			}
			log.Printf("[Chunk Protocol] Error reading message from %s: %v", remotePeer, err)
			return
		}
		_ = s.SetReadDeadline(time.Time{})

		msgCount++
		if msgCount > MaxMessagesPerStream {
			log.Printf("[Chunk Protocol] Max messages (%d) exceeded from %s", MaxMessagesPerStream, remotePeer)
			_ = s.SetWriteDeadline(time.Now().Add(WriteTimeout))
			_ = WriteMessage(s, BuildError(ErrBadRequest, "message limit per stream exceeded"))
			_ = s.SetWriteDeadline(time.Time{})
			return
		}

		if err := ValidateMessage(msg); err != nil {
			log.Printf("[Chunk Protocol] Invalid message structure from %s: %v", remotePeer, err)
			_ = s.SetWriteDeadline(time.Now().Add(WriteTimeout))
			_ = WriteMessage(s, BuildError(ErrBadRequest, fmt.Sprintf("invalid message structure: %v", err)))
			_ = s.SetWriteDeadline(time.Time{})
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			if !h.handleRequestManifest(s, msg) {
				return
			}
			txCount++
		case MsgRequestChunk:
			if !h.handleRequestChunk(s, msg, &msgCount) {
				return
			}
			txCount++
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d from %s", msg.Type, remotePeer)
			_ = s.SetWriteDeadline(time.Now().Add(WriteTimeout))
			_ = WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
			_ = s.SetWriteDeadline(time.Time{})
			return
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) bool {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(WriteTimeout))
		_ = WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		_ = s.SetWriteDeadline(time.Time{})
		return true
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(WriteTimeout))
		_ = WriteMessage(s, BuildError(ErrContentNotFound, "manifest not found"))
		_ = s.SetWriteDeadline(time.Time{})
		return true
	}

	resp := BuildManifest(contentID, manifestData)
	_ = s.SetWriteDeadline(time.Now().Add(WriteTimeout))
	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
		_ = s.SetWriteDeadline(time.Time{})
		return false
	}
	_ = s.SetWriteDeadline(time.Time{})
	return true
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message, msgCount *int) bool {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(WriteTimeout))
		_ = WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		_ = s.SetWriteDeadline(time.Time{})
		return true
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(WriteTimeout))
		_ = WriteMessage(s, BuildError(ErrChunkNotFound, "chunk not found"))
		_ = s.SetWriteDeadline(time.Time{})
		return true
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		_ = s.SetWriteDeadline(time.Now().Add(WriteTimeout))
		_ = WriteMessage(s, BuildError(ErrInternal, "failed to build chunk message"))
		_ = s.SetWriteDeadline(time.Time{})
		return true
	}

	_ = s.SetWriteDeadline(time.Now().Add(WriteTimeout))
	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		_ = s.SetWriteDeadline(time.Time{})
		return false
	}
	_ = s.SetWriteDeadline(time.Time{})

	// Wait for ACK or TICKET synchronously (sequential protocol requirement)
	_ = s.SetReadDeadline(time.Now().Add(ReadTimeout))
	ackMsg, err := ReadMessage(s)
	if err != nil {
		log.Printf("[Chunk Protocol] Error reading ACK/Ticket: %v", err)
		return false
	}
	_ = s.SetReadDeadline(time.Time{})

	(*msgCount)++
	if *msgCount > MaxMessagesPerStream {
		log.Printf("[Chunk Protocol] Max messages (%d) exceeded from %s", MaxMessagesPerStream, s.Conn().RemotePeer())
		_ = s.SetWriteDeadline(time.Now().Add(WriteTimeout))
		_ = WriteMessage(s, BuildError(ErrBadRequest, "message limit per stream exceeded"))
		_ = s.SetWriteDeadline(time.Time{})
		return false
	}

	if err := ValidateMessage(ackMsg); err != nil {
		log.Printf("[Chunk Protocol] Invalid ACK message structure from %s: %v", s.Conn().RemotePeer(), err)
		_ = s.SetWriteDeadline(time.Now().Add(WriteTimeout))
		_ = WriteMessage(s, BuildError(ErrBadRequest, fmt.Sprintf("invalid ack message: %v", err)))
		_ = s.SetWriteDeadline(time.Time{})
		return false
	}

	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		log.Printf("[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, msgStr)
		return true
	}

	if ackMsg.Type == MsgTicket {
		signedTicket, err := ParseTicket(ackMsg.Payload)
		if err != nil {
			log.Printf("[Chunk Protocol] Failed to parse ticket for chunk %x: %v", chunkID, err)
			return false
		}
		if h.ticketHandler != nil {
			if err := h.ticketHandler(signedTicket); err != nil {
				log.Printf("[Chunk Protocol] Ticket verification rejected for chunk %x: %v", chunkID, err)
				h.rejectedPeers.Store(s.Conn().RemotePeer(), struct{}{})
				_ = s.SetWriteDeadline(time.Now().Add(WriteTimeout))
				_ = WriteMessage(s, BuildError(ErrPermissionDenied, fmt.Sprintf("invalid ticket: %v", err)))
				_ = s.SetWriteDeadline(time.Time{})
				return false
			}
		}
	} else if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK or Ticket, got type %d", ackMsg.Type)
		_ = s.SetWriteDeadline(time.Now().Add(WriteTimeout))
		_ = WriteMessage(s, BuildError(ErrBadRequest, "expected ACK or Ticket message"))
		_ = s.SetWriteDeadline(time.Time{})
		return false
	}

	return true
}
