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

func (h *StreamHandler) writeMessageWithDeadline(s network.Stream, msg *Message) error {
	if err := s.SetWriteDeadline(time.Now().Add(StreamWriteTimeout)); err != nil {
		log.Printf("[Chunk Protocol] Error setting write deadline for peer %s: %v", s.Conn().RemotePeer(), err)
		return err
	}
	return WriteMessage(s, msg)
}

func (h *StreamHandler) writeErrorWithDeadline(s network.Stream, code ErrorCode, msg string) error {
	return h.writeMessageWithDeadline(s, BuildError(code, msg))
}

func isTimeoutErr(err error) bool {
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
	return strings.Contains(errStr, "deadline exceeded") || strings.Contains(errStr, "i/o timeout")
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	peerID := s.Conn().RemotePeer()
	log.Printf("[Chunk Protocol] New stream from %s", peerID)

	var msgCount int
	var txCount int

	for {
		if txCount >= MaxTransactionsPerStream || msgCount >= MaxMessagesPerStream {
			log.Printf("[Chunk Protocol] Stream terminated for peer %s: limit reached (cause: limit reached, transactions: %d/%d, messages: %d/%d)",
				peerID, txCount, MaxTransactionsPerStream, msgCount, MaxMessagesPerStream)
			return
		}

		if err := s.SetReadDeadline(time.Now().Add(StreamReadTimeout)); err != nil {
			log.Printf("[Chunk Protocol] Error setting read deadline for peer %s: %v", peerID, err)
			return
		}

		var readTimedOut bool
		readTimer := time.AfterFunc(StreamReadTimeout, func() {
			readTimedOut = true
			s.Reset()
		})

		msg, err := ReadMessage(s)
		readTimer.Stop()

		if err != nil {
			if readTimedOut || isTimeoutErr(err) {
				log.Printf("[Chunk Protocol] Stream terminated for peer %s: (cause: timeout, error: read deadline exceeded)", peerID)
				return
			}
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by peer %s (cause: clean close)", peerID)
				return
			}
			log.Printf("[Chunk Protocol] Stream terminated for peer %s: (cause: read error, error: %v)", peerID, err)
			return
		}

		msgCount++
		if msgCount > MaxMessagesPerStream {
			log.Printf("[Chunk Protocol] Stream terminated for peer %s: (cause: limit reached, max messages exceeded %d/%d)",
				peerID, msgCount, MaxMessagesPerStream)
			h.writeErrorWithDeadline(s, ErrBadRequest, "max messages per stream exceeded")
			return
		}

		if msg.Version != CurrentMessageVersion {
			log.Printf("[Chunk Protocol] Unsupported message version %d from peer %s", msg.Version, peerID)
			h.writeErrorWithDeadline(s, ErrUnsupportedMessage, "unsupported message version")
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			txDone := h.handleRequestManifest(s, msg)
			if txDone {
				txCount++
			}
		case MsgRequestChunk:
			txDone := h.handleRequestChunk(s, msg, &msgCount)
			if txDone {
				txCount++
			}
		default:
			log.Printf("[Chunk Protocol] Unsupported message type %d from peer %s", msg.Type, peerID)
			h.writeErrorWithDeadline(s, ErrUnsupportedMessage, "unsupported message type")
			return
		}
	}
}

func (h *StreamHandler) handleRequestManifest(s network.Stream, msg *Message) bool {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		h.writeErrorWithDeadline(s, ErrBadRequest, "invalid payload for REQUEST_MANIFEST")
		return true
	}

	ctx := context.Background()
	manifestData, err := h.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		h.writeErrorWithDeadline(s, ErrContentNotFound, "manifest not found")
		return true
	}

	resp := BuildManifest(contentID, manifestData)
	if err := h.writeMessageWithDeadline(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response to %s: %v", s.Conn().RemotePeer(), err)
	}
	return true
}

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message, msgCount *int) bool {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		h.writeErrorWithDeadline(s, ErrBadRequest, "invalid payload for REQUEST_CHUNK")
		return true
	}

	ctx := context.Background()
	chunkData, err := h.engine.GetChunk(ctx, chunkID)
	if err != nil {
		h.writeErrorWithDeadline(s, ErrChunkNotFound, "chunk not found")
		return true
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		h.writeErrorWithDeadline(s, ErrInternal, "failed to build chunk message")
		return true
	}

	if err := h.writeMessageWithDeadline(s, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response to %s: %v", s.Conn().RemotePeer(), err)
		return true
	}

	if *msgCount >= MaxMessagesPerStream {
		log.Printf("[Chunk Protocol] Stream terminated for peer %s: max messages reached before ACK (%d/%d)",
			s.Conn().RemotePeer(), *msgCount, MaxMessagesPerStream)
		return true
	}

	if err := s.SetReadDeadline(time.Now().Add(StreamReadTimeout)); err != nil {
		log.Printf("[Chunk Protocol] Error setting read deadline for ACK from peer %s: %v", s.Conn().RemotePeer(), err)
		return true
	}

	var ackTimedOut bool
	ackTimer := time.AfterFunc(StreamReadTimeout, func() {
		ackTimedOut = true
		s.Reset()
	})

	ackMsg, err := ReadMessage(s)
	ackTimer.Stop()

	if err != nil {
		if ackTimedOut || isTimeoutErr(err) {
			log.Printf("[Chunk Protocol] Stream terminated for peer %s waiting for ACK: (cause: timeout, error: read deadline exceeded)",
				s.Conn().RemotePeer())
		} else {
			log.Printf("[Chunk Protocol] Error reading ACK from peer %s: %v", s.Conn().RemotePeer(), err)
		}
		return true
	}

	*msgCount++

	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		log.Printf("[Chunk Protocol] Client %s reported error on chunk %x: [%d] %s", s.Conn().RemotePeer(), chunkID, code, msgStr)
		return true
	}
	if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Peer %s expected ACK, got type %d", s.Conn().RemotePeer(), ackMsg.Type)
		return true
	}

	return true
}
