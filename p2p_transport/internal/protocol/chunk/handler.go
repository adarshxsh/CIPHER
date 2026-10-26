package chunk

import (
	"context"
	"io"
	"log"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"

	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/protocol"
)

var TestCorruptProb float64

var ansiRegexp = regexp.MustCompile(`\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~]|\][^\x07\x1b]*(?:\x07|\x1b\\))`)

// SanitizeErrorMessage strips non-printable ASCII, ANSI escape sequences, and control characters from error messages,
// and truncates error messages longer than 256 bytes.
func SanitizeErrorMessage(msg string) string {
	// Remove ANSI escape sequences first
	cleaned := ansiRegexp.ReplaceAllString(msg, "")

	// Filter printable ASCII characters (0x20 to 0x7E)
	var builder strings.Builder
	builder.Grow(len(cleaned))
	for i := 0; i < len(cleaned); i++ {
		b := cleaned[i]
		if b >= 0x20 && b <= 0x7E {
			builder.WriteByte(b)
		}
	}

	res := builder.String()
	if len(res) > 256 {
		res = res[:256]
	}
	return res
}

type peerErrorLogger struct {
	peerID       peer.ID
	limiter      *rate.Limiter
	droppedCount int
	mu           sync.Mutex
}

func newPeerErrorLogger(peerID peer.ID) *peerErrorLogger {
	// Maximum 10 error log entries per peer per minute
	return &peerErrorLogger{
		peerID:  peerID,
		limiter: rate.NewLimiter(rate.Every(6*time.Second), 10),
	}
}

func (l *peerErrorLogger) LogChunkError(chunkID core.ChunkID, code ErrorCode, rawMsg string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	safeMsg := SanitizeErrorMessage(rawMsg)

	if !l.limiter.Allow() {
		l.droppedCount++
		return
	}

	if l.droppedCount > 0 {
		log.Printf("[Chunk Protocol] Rate limit exceeded: dropped %d error messages from %s", l.droppedCount, l.peerID)
		l.droppedCount = 0
	}

	log.Printf("[Chunk Protocol] Client reported error on chunk %x: [%d] %s", chunkID, code, safeMsg)
}

func (l *peerErrorLogger) LogGeneralError(code ErrorCode, rawMsg string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	safeMsg := SanitizeErrorMessage(rawMsg)

	if !l.limiter.Allow() {
		l.droppedCount++
		return
	}

	if l.droppedCount > 0 {
		log.Printf("[Chunk Protocol] Rate limit exceeded: dropped %d error messages from %s", l.droppedCount, l.peerID)
		l.droppedCount = 0
	}

	log.Printf("[Chunk Protocol] Client reported error from %s: [%d] %s", l.peerID, code, safeMsg)
}

func (l *peerErrorLogger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.droppedCount > 0 {
		log.Printf("[Chunk Protocol] Rate limit exceeded: dropped %d error messages from %s", l.droppedCount, l.peerID)
		l.droppedCount = 0
	}
}

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
	peerID := s.Conn().RemotePeer()
	logger := newPeerErrorLogger(peerID)
	defer func() {
		logger.Close()
		s.Close()
	}()
	log.Printf("[Chunk Protocol] New stream from %s", peerID)

	for {
		msg, err := ReadMessage(s)
		if err != nil {
			if err == io.EOF || err.Error() == "stream reset" {
				log.Printf("[Chunk Protocol] Stream closed by %s", peerID)
				return
			}
			log.Printf("[Chunk Protocol] Error reading message: %v", err)
			return
		}

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
			h.handleRequestChunk(s, msg, logger)
		case MsgError:
			code, msgStr, _ := ParseError(msg.Payload)
			logger.LogGeneralError(code, msgStr)
		default:
			log.Printf("[Chunk Protocol] Unsupported message type: %d", msg.Type)
			WriteMessage(s, BuildError(ErrUnsupportedMessage, "unsupported message type"))
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

func (h *StreamHandler) handleRequestChunk(s network.Stream, msg *Message, logger *peerErrorLogger) {
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

	// 5. Wait for ACK synchronously (sequential protocol requirement)
	ackMsg, err := ReadMessage(s)
	if err != nil {
		log.Printf("[Chunk Protocol] Error reading ACK: %v", err)
		return
	}
	if ackMsg.Type == MsgError {
		code, msgStr, _ := ParseError(ackMsg.Payload)
		logger.LogChunkError(chunkID, code, msgStr)
		return
	}
	if ackMsg.Type != MsgAck {
		log.Printf("[Chunk Protocol] Expected ACK, got type %d", ackMsg.Type)
		return
	}
}
