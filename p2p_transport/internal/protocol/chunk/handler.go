package chunk

import (
	"context"
	"errors"
	"io"
	"log"
	"math/rand"
	"net"

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

// StreamSession encapsulates message and transaction counters for an incoming stream connection.
type StreamSession struct {
	handler  *StreamHandler
	stream   network.Stream
	msgCount int
	txCount  int
}

func newStreamSession(h *StreamHandler, s network.Stream) *StreamSession {
	return &StreamSession{
		handler: h,
		stream:  s,
	}
}

func (sess *StreamSession) readNextMessage() (*Message, error) {
	msg, err := ReadMessage(sess.stream)
	if err != nil {
		return nil, err
	}
	sess.msgCount++
	return msg, nil
}

func (h *StreamHandler) handleStream(s network.Stream) {
	defer s.Close()
	log.Printf("[Chunk Protocol] New stream from %s", s.Conn().RemotePeer())

	sess := newStreamSession(h, s)
	sess.run()
}

func (sess *StreamSession) run() {
	for {
		msg, err := sess.readNextMessage()
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" || errors.Is(err, net.ErrClosed) {
				log.Printf("[Chunk Protocol] Stream closed by %s", sess.stream.Conn().RemotePeer())
				return
			}
			log.Printf("[Chunk Protocol] Error reading message: %v", err)
			return
		}

		if sess.msgCount > MaxMessagesPerStream {
			log.Printf("[Chunk Protocol] Message limit exceeded (%d > %d) from %s", sess.msgCount, MaxMessagesPerStream, sess.stream.Conn().RemotePeer())
			WriteMessage(sess.stream, BuildError(ErrBadRequest, "stream message limit exceeded"))
			return
		}

		if sess.txCount >= MaxTransactionsPerStream {
			log.Printf("[Chunk Protocol] Transaction limit exceeded (%d >= %d) from %s", sess.txCount, MaxTransactionsPerStream, sess.stream.Conn().RemotePeer())
			WriteMessage(sess.stream, BuildError(ErrBadRequest, "stream transaction limit exceeded"))
			return
		}

		if msg.Version != CurrentMessageVersion {
			log.Printf("[Chunk Protocol] Unsupported version %d", msg.Version)
			WriteMessage(sess.stream, BuildError(ErrUnsupportedMessage, "unsupported message version"))
			return
		}

		switch msg.Type {
		case MsgRequestManifest:
			sess.handleRequestManifest(msg)
			if sess.txCount >= MaxTransactionsPerStream {
				return
			}
		case MsgRequestChunk:
			sess.handleRequestChunk(msg)
			if sess.txCount >= MaxTransactionsPerStream {
				return
			}
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			WriteMessage(sess.stream, BuildError(ErrUnsupportedMessage, "unsupported message type"))
			return
		}
	}
}

func (sess *StreamSession) handleRequestManifest(msg *Message) {
	contentID, err := ParseRequestManifest(msg.Payload)
	if err != nil {
		WriteMessage(sess.stream, BuildError(ErrBadRequest, "invalid payload for REQUEST_MANIFEST"))
		return
	}

	ctx := context.Background()
	manifestData, err := sess.handler.engine.GetManifestBytes(ctx, contentID)
	if err != nil {
		WriteMessage(sess.stream, BuildError(ErrContentNotFound, "manifest not found"))
		return
	}

	resp := BuildManifest(contentID, manifestData)
	if err := WriteMessage(sess.stream, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing MANIFEST response: %v", err)
		return
	}

	sess.txCount++
}

func (sess *StreamSession) handleRequestChunk(msg *Message) {
	chunkID, err := ParseRequestChunk(msg.Payload)
	if err != nil {
		WriteMessage(sess.stream, BuildError(ErrBadRequest, "invalid payload for REQUEST_CHUNK"))
		return
	}

	ctx := context.Background()
	chunkData, err := sess.handler.engine.GetChunk(ctx, chunkID)
	if err != nil {
		WriteMessage(sess.stream, BuildError(ErrChunkNotFound, "chunk not found"))
		return
	}

	if TestCorruptProb > 0 && rand.Float64() < TestCorruptProb && len(chunkData.Data) > 0 {
		log.Printf("[TESTING] Corrupting chunk %x", chunkID)
		chunkData.Data[0] ^= 0xFF
	}

	resp, err := BuildChunk(chunkData)
	if err != nil {
		WriteMessage(sess.stream, BuildError(ErrInternal, "failed to build chunk message"))
		return
	}

	if err := WriteMessage(sess.stream, resp); err != nil {
		log.Printf("[Chunk Protocol] Error writing CHUNK response: %v", err)
		return
	}

	ackMsg, err := sess.readNextMessage()
	if err != nil {
		log.Printf("[Chunk Protocol] Error reading ACK: %v", err)
		return
	}

	if sess.msgCount > MaxMessagesPerStream {
		log.Printf("[Chunk Protocol] Message limit exceeded on ACK (%d > %d)", sess.msgCount, MaxMessagesPerStream)
		WriteMessage(sess.stream, BuildError(ErrBadRequest, "stream message limit exceeded"))
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

	sess.txCount++
}
