package chunk

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/network/content/engine"
	"cipher/network/payments"
	"cipher/network/protocol"
)

var TestCorruptProb float64

// TicketHandlerFunc validates and stores a signed ticket received from a consumer.
type TicketHandlerFunc func(ticket *payments.SignedTicket) error

// TicketRejectBanDuration is how long a provider refuses chunks to a peer
// after that peer sent a ticket the ticket handler rejected. Streams carry one
// transaction each, so the rejection has to be remembered per peer; otherwise
// a consumer could forge a ticket on every fresh stream and download for free.
const TicketRejectBanDuration = 10 * time.Minute

type StreamHandler struct {
	host          host.Host
	engine        *engine.ContentEngine
	ticketHandler TicketHandlerFunc

	banMu  sync.Mutex
	banned map[peer.ID]time.Time // peer -> ban expiry
}

func (h *StreamHandler) SetTicketHandler(fn TicketHandlerFunc) {
	h.ticketHandler = fn
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine) *StreamHandler {
	handler := &StreamHandler{
		host:   h,
		engine: eng,
		banned: make(map[peer.ID]time.Time),
	}
	h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.handleStream)
	return handler
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	remote := s.Conn().RemotePeer()

	// One transaction per stream (MaxTransactionsPerStream), bounded in time.
	// A slow or silent peer is dropped when the deadline fires instead of
	// holding this goroutine open indefinitely.
	if err := s.SetDeadline(time.Now().Add(StreamTimeout)); err != nil {
		log.Printf("[Chunk Protocol] Failed to set deadline for %s: %v", remote, err)
		s.Reset()
		return
	}

	msg, err := ReadValidatedMessage(s)
	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidProtocolVersion):
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
		case errors.Is(err, ErrUnknownMessageType):
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
		case errors.Is(err, ErrPayloadTooLarge), errors.Is(err, ErrInvalidPayloadSize),
			errors.Is(err, ErrInvalidPayloadLength), errors.Is(err, ErrInvalidContentID),
			errors.Is(err, ErrInvalidChunkID):
			WriteMessage(s, BuildError(ErrBadRequest, "malformed request"))
		}
		log.Printf("[Chunk Protocol] Rejected request from %s: %v", remote, err)
		return
	}

	switch msg.Type {
	case MsgRequestManifest:
		h.handleRequestManifest(s, msg)
	case MsgRequestChunk:
		h.handleRequestChunk(s, msg)
	default:
		// Well-formed, but not a request a provider serves (e.g. a stray ACK).
		log.Printf("[Chunk Protocol] Unexpected message type %d from %s", msg.Type, remote)
		WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
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

func (h *StreamHandler) ban(p peer.ID) {
	h.banMu.Lock()
	defer h.banMu.Unlock()
	now := time.Now()
	for id, until := range h.banned {
		if now.After(until) {
			delete(h.banned, id)
		}
	}
	h.banned[p] = now.Add(TicketRejectBanDuration)
}

func (h *StreamHandler) isBanned(p peer.ID) bool {
	h.banMu.Lock()
	defer h.banMu.Unlock()
	until, ok := h.banned[p]
	if !ok {
		return false
	}
	if time.Now().After(until) {
		delete(h.banned, p)
		return false
	}
	return true
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message) {
	remote := s.Conn().RemotePeer()
	if h.isBanned(remote) {
		WriteMessage(s, BuildError(ErrPermissionDenied, "payment rejected"))
		return
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
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return
	}

	// 5. Wait for ACK or TICKET synchronously (sequential protocol requirement)
	ackMsg, err := ReadValidatedMessage(s)
	if err != nil {
		log.Printf("[Chunk Protocol] Error reading ACK/Ticket: %v", err)
		return
	}
	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		log.Printf("[Chunk Protocol] Client reported error on chunk %x: [%d] %q", chunkID, code, msgStr)
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
				h.ban(remote)
				WriteMessage(s, BuildError(ErrPermissionDenied, fmt.Sprintf("invalid ticket: %v", err)))
				return
			}
		}
	} else if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK or Ticket, got type %d", ackMsg.Type)
		return
	}
}
