package chunk

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
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
	host            host.Host
	engine          *engine.ContentEngine
	ticketHandler   TicketHandlerFunc
	maxTransactions int
	maxMessages     int
}

func (h *StreamHandler) SetTicketHandler(fn TicketHandlerFunc) {
	h.ticketHandler = fn
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine) *StreamHandler {
	handler := &StreamHandler{
		host:            h,
		engine:          eng,
		maxTransactions: MaxTransactionsPerStream,
		maxMessages:     MaxMessagesPerStream,
	}
	h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	return handler
}

func (h *StreamHandler) SetLimits(maxTxns, maxMsgs int) {
	h.maxTransactions = maxTxns
	h.maxMessages = maxMsgs
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	log.Printf("[Chunk Protocol] New stream from %s", s.Conn().RemotePeer())

	msgCount := 0
	txnCount := 0

	for {
		_ = s.SetReadDeadline(time.Now().Add(ReadTimeout))
		msg, err := ReadMessage(s)
		_ = s.SetReadDeadline(time.Time{})
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", s.Conn().RemotePeer())
				return
			}
			log.Printf("[Chunk Protocol] Error reading message: %v", err)
			return
		}

		msgCount++
		if h.maxMessages > 0 && msgCount > h.maxMessages {
			log.Printf("[Chunk Protocol] Message limit exceeded (%d > %d) from %s", msgCount, h.maxMessages, s.Conn().RemotePeer())
			_ = WriteMessage(s, BuildError(ErrBadRequest, "message limit exceeded"))
			return
		}

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			log.Printf("[Chunk Protocol] Unsupported version %d", msg.Version)
			_ = WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest, MsgRequestChunk:
			txnCount++
			if h.maxTransactions > 0 && txnCount > h.maxTransactions {
				log.Printf("[Chunk Protocol] Transaction limit exceeded (%d > %d) from %s", txnCount, h.maxTransactions, s.Conn().RemotePeer())
				_ = WriteMessage(s, BuildError(ErrBadRequest, "transaction limit exceeded"))
				return
			}
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg, &msgCount)
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			_ = WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
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

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message, msgCount *int) {
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
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return
	}

	// 5. Wait for ACK or TICKET synchronously (sequential protocol requirement)
	_ = s.SetReadDeadline(time.Now().Add(ReadTimeout))
	ackMsg, err := ReadMessage(s)
	_ = s.SetReadDeadline(time.Time{})
	if err != nil {
		log.Printf("[Chunk Protocol] Error reading ACK/Ticket: %v", err)
		return
	}
	if msgCount != nil {
		*msgCount++
	}
	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		log.Printf("[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, msgStr)
		return
	}
	if ackMsg.Type == MsgTicket {
		signedTicket, err := ParseTicket(ackMsg.Payload)
		if err != nil {
			log.Printf("[Chunk Protocol] Failed to parse ticket for chunk %x: %v", chunkID, err)
			return
		}
		if h.ticketHandler != nil {
			if err := h.ticketHandler(signedTicket); err != nil {
				log.Printf("[Chunk Protocol] Ticket verification rejected for chunk %x: %v", chunkID, err)
				WriteMessage(s, BuildError(ErrPermissionDenied, fmt.Sprintf("invalid ticket: %v", err)))
				return
			}
		}
	} else if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK or Ticket, got type %d", ackMsg.Type)
		return
	}
}
