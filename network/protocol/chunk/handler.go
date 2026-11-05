package chunk

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"

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
}

func (h *StreamHandler) SetTicketHandler(fn TicketHandlerFunc) {
	h.ticketHandler = fn
}

func NewStreamHandler(h host.Host, eng *engine.ContentEngine) *StreamHandler {
	handler := &StreamHandler{
		host:   h,
		engine: eng,
	}
	h.SetStreamHandler(protocol.ChunkTransportProtocolID, handler.HandleStream)
	return handler
}

func (h *StreamHandler) HandleStream(s network.Stream) {
	if err := h.processStream(s); err != nil {
		log.Printf("[Chunk Protocol] Stream error: %v", err)
	}
}

func (h *StreamHandler) processStream(s network.Stream) (err error) {
	defer func() {
		if err != nil && err != io.EOF && err.Error() != "stream reset" {
			s.Reset()
		} else {
			s.Close()
		}
	}()

	log.Printf("[Chunk Protocol] New stream from %s", s.Conn().RemotePeer())

	for {
		msg, readErr := ReadMessage(s)
		if readErr != nil {
			if readErr == io.EOF || readErr.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", s.Conn().RemotePeer())
				return nil
			}
			log.Printf("[Chunk Protocol] Error reading message: %v", readErr)
			return readErr
		}

		if msg.Version != CurrentMessageVersion {
			// Older or incompatible version
			log.Printf("[Chunk Protocol] Unsupported version %d", msg.Version)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return fmt.Errorf("unsupported message version %d", msg.Version)
		}

		switch msg.Type {
		case MsgRequestManifest:
			if err := h.handleRequestManifest(s, msg); err != nil {
				return err
			}
		case MsgRequestChunk:
			if err := h.handleRequestChunk(s, msg); err != nil {
				return err
			}
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
			return fmt.Errorf("unsupported message type %d", msg.Type)
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) error {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		return nil
	}

	// Fetch manifest from engine
	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		WriteMessage(s, BuildError(ErrContentNotFound, "manifest not found"))
		return nil
	}

	resp := BuildManifest(contentID, manifestData)
	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
		return err
	}
	return nil
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message) error {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		WriteMessage(s, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		return nil
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		WriteMessage(s, BuildError(ErrChunkNotFound, "chunk not found"))
		return nil
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		// Corrupt the chunk for testing
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		WriteMessage(s, BuildError(ErrInternal, "failed to build chunk message"))
		return nil
	}

	if err := WriteMessage(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return err
	}

	// 5. Wait for ACK or TICKET synchronously (sequential protocol requirement)
	ackMsg, err := ReadMessage(s)
	if err != nil {
		if err == io.EOF || err.Error() == "stream reset" {
			log.Printf("[Chunk Protocol] Stream closed while awaiting ACK/Ticket from %s", s.Conn().RemotePeer())
			return nil
		}
		log.Printf("[Chunk Protocol] Error reading ACK/Ticket: %v", err)
		return err
	}
	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		log.Printf("[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, msgStr)
		return nil
	}
	if ackMsg.Type == MsgTicket {
		signedTicket, err := ParseTicket(ackMsg.Payload)
		if err != nil {
			log.Printf("[Chunk Protocol] Failed to parse ticket for chunk %x: %v", chunkID, err)
			return err
		}
		if h.ticketHandler != nil {
			if err := h.ticketHandler(signedTicket); err != nil {
				log.Printf("[Chunk Protocol] Ticket verification rejected for chunk %x: %v", chunkID, err)
				WriteMessage(s, BuildError(ErrPermissionDenied, fmt.Sprintf("invalid ticket: %v", err)))
				return nil
			}
		}
	} else if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK or Ticket, got type %d", ackMsg.Type)
		return fmt.Errorf("expected ACK or Ticket, got type %d", ackMsg.Type)
	}
	return nil
}
