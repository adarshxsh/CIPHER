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
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"

	"cipher/network/content/engine"
	"cipher/network/payments"
	"cipher/network/protocol"
)

var (
	TestCorruptProb float64
	StreamTimeout   = 30 * time.Second
)

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	errStr := err.Error()
	return strings.Contains(errStr, "deadline") || strings.Contains(errStr, "i/o timeout") || strings.Contains(errStr, "time out")
}

// TicketHandlerFunc validates and stores a signed ticket received from a consumer.
type TicketHandlerFunc func(ticket *payments.SignedTicket) error

type StreamHandler struct {
	host          host.Host
	engine        *engine.ContentEngine
	ticketHandler TicketHandlerFunc
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
	log.Printf("[Chunk Protocol] New stream from %s", s.Conn().RemotePeer())

	s.SetReadDeadline(time.Now().Add(StreamTimeout))
	s.SetWriteDeadline(time.Now().Add(StreamTimeout))

	for {
		s.SetReadDeadline(time.Now().Add(StreamTimeout))

		msg, err := ReadMessage(s)
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", s.Conn().RemotePeer())
				return
			}
			if isTimeout(err) {
				log.Printf("[Chunk Protocol] Debug: stream deadline expired for %s: %v", s.Conn().RemotePeer(), err)
				return
			}
			log.Printf("[Chunk Protocol] Error reading message: %v", err)
			return
		}

		// Valid message header/payload received - extend deadlines
		s.SetReadDeadline(time.Now().Add(StreamTimeout))
		s.SetWriteDeadline(time.Now().Add(StreamTimeout))

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			log.Printf("[Chunk Protocol] Unsupported version %d", msg.Version)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			h.handleRequestManifest(s, msg)
		case MsgRequestChunk:
			h.handleRequestChunk(s, msg)
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		s.SetWriteDeadline(time.Now().Add(StreamTimeout))
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		return
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		s.SetWriteDeadline(time.Now().Add(StreamTimeout))
		WriteMessage(s, BuildError(ErrContentNotFound, "manifest not found"))
		return
	}

	resp := BuildManifest(contentID, manifestData)
	s.SetWriteDeadline(time.Now().Add(StreamTimeout))
	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
	}
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message) {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		s.SetWriteDeadline(time.Now().Add(StreamTimeout))
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		return
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		s.SetWriteDeadline(time.Now().Add(StreamTimeout))
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
		s.SetWriteDeadline(time.Now().Add(StreamTimeout))
		WriteMessage(s, BuildError(ErrInternal, "failed to build chunk message"))
		return
	}

	s.SetWriteDeadline(time.Now().Add(StreamTimeout))
	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return
	}

	// 5. Wait for ACK or TICKET synchronously (sequential protocol requirement)
	s.SetReadDeadline(time.Now().Add(StreamTimeout))
	ackMsg, err := ReadMessage(s)
	if err != nil {
		if isTimeout(err) {
			log.Printf("[Chunk Protocol] Debug: stream deadline expired waiting for ACK/Ticket from %s: %v", s.Conn().RemotePeer(), err)
			return
		}
		log.Printf("[Chunk Protocol] Error reading ACK/Ticket: %v", err)
		return
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
				s.SetWriteDeadline(time.Now().Add(StreamTimeout))
				WriteMessage(s, BuildError(ErrPermissionDenied, fmt.Sprintf("invalid ticket: %v", err)))
				return
			}
		}
	} else if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK or Ticket, got type %d", ackMsg.Type)
		return
	}
}
