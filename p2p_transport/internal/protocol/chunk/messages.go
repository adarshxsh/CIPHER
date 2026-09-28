package chunk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"cipher/internal/content/core"
)

const (
	CurrentMessageVersion uint16 = 1
)

type MessageType uint8

const (
	MsgRequestManifest MessageType = 0x01
	MsgManifest        MessageType = 0x02
	MsgRequestChunk    MessageType = 0x03
	MsgChunk           MessageType = 0x04
	MsgAck             MessageType = 0x05
	MsgError           MessageType = 0x06
)

type ErrorCode uint8

const (
	ErrContentNotFound    ErrorCode = 0x01
	ErrChunkNotFound      ErrorCode = 0x02
	ErrInvalidManifest    ErrorCode = 0x03
	ErrPermissionDenied   ErrorCode = 0x04
	ErrInternal           ErrorCode = 0x05
	ErrIntegrityMismatch  ErrorCode = 0x06
	ErrBadRequest         ErrorCode = 0x07
	ErrUnsupportedMessage ErrorCode = 0x08
)

// Message is the symmetric envelope for all protocol communications.
type Message struct {
	Version uint16
	Type    MessageType
	Payload []byte
}

func encodeChunkHeader(h *core.ChunkHeader, dst []byte) {
	binary.LittleEndian.PutUint16(dst[0:2], h.Version)
	copy(dst[2:34], h.ID[:])
	binary.LittleEndian.PutUint32(dst[34:38], h.Index)
	binary.LittleEndian.PutUint64(dst[38:46], uint64(h.Offset))
	binary.LittleEndian.PutUint32(dst[46:50], h.PlainSize)
	binary.LittleEndian.PutUint32(dst[50:54], h.CipherSize)
	copy(dst[54:66], h.Nonce[:])
}

func decodeChunkHeader(src []byte, h *core.ChunkHeader) {
	h.Version = binary.LittleEndian.Uint16(src[0:2])
	copy(h.ID[:], src[2:34])
	h.Index = binary.LittleEndian.Uint32(src[34:38])
	h.Offset = int64(binary.LittleEndian.Uint64(src[38:46]))
	h.PlainSize = binary.LittleEndian.Uint32(src[46:50])
	h.CipherSize = binary.LittleEndian.Uint32(src[50:54])
	copy(h.Nonce[:], src[54:66])
}

var headerPool = sync.Pool{
	New: func() any {
		b := make([]byte, 7)
		return &b
	},
}

func WriteMessage(w io.Writer, msg *Message) error {
	payloadLen := len(msg.Payload)
	size := uint32(3 + payloadLen)

	hdrPtr := headerPool.Get().(*[]byte)
	hdr := *hdrPtr
	binary.LittleEndian.PutUint32(hdr[0:4], size)
	binary.LittleEndian.PutUint16(hdr[4:6], msg.Version)
	hdr[6] = byte(msg.Type)

	if _, err := w.Write(hdr); err != nil {
		headerPool.Put(hdrPtr)
		return err
	}
	if payloadLen > 0 {
		if _, err := w.Write(msg.Payload); err != nil {
			headerPool.Put(hdrPtr)
			return err
		}
	}
	headerPool.Put(hdrPtr)
	return nil
}

func ReadMessage(r io.Reader) (*Message, error) {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return nil, err
	}
	size := binary.LittleEndian.Uint32(sizeBuf[:])

	if size > 2*1024*1024 { // 2MB max frame size
		return nil, errors.New("message exceeds maximum frame size")
	}
	if size < 3 {
		return nil, errors.New("message frame too short")
	}

	frame := make([]byte, size)
	if _, err := io.ReadFull(r, frame); err != nil {
		return nil, err
	}

	version := binary.LittleEndian.Uint16(frame[0:2])
	msgType := MessageType(frame[2])

	return &Message{
		Version: version,
		Type:    msgType,
		Payload: frame[3:],
	}, nil
}

// -- Payload Builders & Parsers --

func BuildRequestManifest(id core.ContentID) *Message {
	return &Message{
		Version: CurrentMessageVersion,
		Type:    MsgRequestManifest,
		Payload: id[:],
	}
}

func ParseRequestManifest(payload []byte) (core.ContentID, error) {
	var id core.ContentID
	if len(payload) != 32 {
		return id, fmt.Errorf("invalid payload length for REQUEST_MANIFEST: %d", len(payload))
	}
	copy(id[:], payload)
	return id, nil
}

func BuildManifest(id core.ContentID, data []byte) *Message {
	payload := make([]byte, 32+len(data))
	copy(payload[:32], id[:])
	copy(payload[32:], data)
	return &Message{
		Version: CurrentMessageVersion,
		Type:    MsgManifest,
		Payload: payload,
	}
}

func ParseManifest(payload []byte) (core.ContentID, []byte, error) {
	var id core.ContentID
	if len(payload) < 32 {
		return id, nil, fmt.Errorf("invalid payload length for MANIFEST: %d", len(payload))
	}
	copy(id[:], payload[:32])
	return id, payload[32:], nil
}

func BuildRequestChunk(id core.ChunkID) *Message {
	return &Message{
		Version: CurrentMessageVersion,
		Type:    MsgRequestChunk,
		Payload: id[:],
	}
}

func ParseRequestChunk(payload []byte) (core.ChunkID, error) {
	var id core.ChunkID
	if len(payload) != 32 {
		return id, fmt.Errorf("invalid payload length for REQUEST_CHUNK: %d", len(payload))
	}
	copy(id[:], payload)
	return id, nil
}

func BuildChunk(chunk *core.Chunk) (*Message, error) {
	if chunk == nil {
		return nil, errors.New("nil chunk")
	}
	payloadLen := 66 + len(chunk.Data)
	payload := make([]byte, payloadLen)
	encodeChunkHeader(&chunk.Header, payload[:66])
	copy(payload[66:], chunk.Data)
	return &Message{
		Version: CurrentMessageVersion,
		Type:    MsgChunk,
		Payload: payload,
	}, nil
}

func ParseChunk(payload []byte) (*core.Chunk, error) {
	if len(payload) < 66 {
		return nil, errors.New("invalid payload length for CHUNK")
	}
	chunk := &core.Chunk{}
	decodeChunkHeader(payload[:66], &chunk.Header)
	chunk.Data = payload[66:]
	return chunk, nil
}

func BuildAck(id core.ChunkID, status uint8) *Message {
	payload := make([]byte, 33)
	copy(payload[:32], id[:])
	payload[32] = status
	return &Message{
		Version: CurrentMessageVersion,
		Type:    MsgAck,
		Payload: payload,
	}
}

func ParseAck(payload []byte) (core.ChunkID, uint8, error) {
	var id core.ChunkID
	if len(payload) != 33 {
		return id, 0, fmt.Errorf("invalid payload length for ACK: %d", len(payload))
	}
	copy(id[:], payload[:32])
	return id, payload[32], nil
}

func BuildError(code ErrorCode, msg string) *Message {
	payload := make([]byte, 1+len(msg))
	payload[0] = byte(code)
	copy(payload[1:], msg)
	return &Message{
		Version: CurrentMessageVersion,
		Type:    MsgError,
		Payload: payload,
	}
}

func ParseError(payload []byte) (ErrorCode, string, error) {
	if len(payload) < 1 {
		return 0, "", errors.New("invalid payload length for ERROR")
	}
	return ErrorCode(payload[0]), string(payload[1:]), nil
}

