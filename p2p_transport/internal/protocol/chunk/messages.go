package chunk

import (
	"bytes"
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
	ErrContentNotFound  ErrorCode = 0x01
	ErrChunkNotFound    ErrorCode = 0x02
	ErrInvalidManifest  ErrorCode = 0x03
	ErrPermissionDenied ErrorCode = 0x04
	ErrInternal         ErrorCode = 0x05
	ErrIntegrityMismatch ErrorCode = 0x06
	ErrBadRequest       ErrorCode = 0x07
	ErrUnsupportedMessage ErrorCode = 0x08
)

// Message is the symmetric envelope for all protocol communications.
type Message struct {
	Version uint16
	Type    MessageType
	Payload []byte
}

var headerPool = sync.Pool{
	New: func() any {
		return new([7]byte)
	},
}

func WriteMessage(w io.Writer, msg *Message) error {
	if msg == nil {
		return errors.New("nil message")
	}
	payloadLen := len(msg.Payload)
	size := uint32(3 + payloadLen)
	if size > MaxFrameSize {
		return errors.New("message exceeds maximum frame size")
	}

	hdrPtr := headerPool.Get().(*[7]byte)
	binary.LittleEndian.PutUint32(hdrPtr[0:4], size)
	binary.LittleEndian.PutUint16(hdrPtr[4:6], msg.Version)
	hdrPtr[6] = byte(msg.Type)

	_, err := w.Write(hdrPtr[:])
	headerPool.Put(hdrPtr)
	if err != nil {
		return err
	}

	if payloadLen > 0 {
		if _, err := w.Write(msg.Payload); err != nil {
			return err
		}
	}
	return nil
}

func ReadMessage(r io.Reader) (*Message, error) {
	if r == nil {
		return nil, errors.New("nil reader")
	}

	var sizeBuf [4]byte
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return nil, err
	}

	size := binary.LittleEndian.Uint32(sizeBuf[:])

	if size > MaxFrameSize {
		return nil, errors.New("message exceeds maximum frame size")
	}
	if size < 3 {
		return nil, errors.New("message frame too short")
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	version := binary.LittleEndian.Uint16(data[0:2])
	msgType := MessageType(data[2])

	return &Message{
		Version: version,
		Type:    msgType,
		Payload: data[3:],
	}, nil
}

// -- Payload Builders & Parsers --

func BuildRequestManifest(id core.ContentID) *Message {
	payload := make([]byte, 32)
	copy(payload, id[:])
	return &Message{
		Version: CurrentMessageVersion,
		Type:    MsgRequestManifest,
		Payload: payload,
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
	payload := make([]byte, 32)
	copy(payload, id[:])
	return &Message{
		Version: CurrentMessageVersion,
		Type:    MsgRequestChunk,
		Payload: payload,
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
	var dummyHeader core.ChunkHeader
	headerSize := binary.Size(dummyHeader)
	payload := make([]byte, headerSize+len(chunk.Data))

	buf := bytes.NewBuffer(payload[:0])
	if err := binary.Write(buf, binary.LittleEndian, &chunk.Header); err != nil {
		return nil, err
	}
	copy(payload[headerSize:], chunk.Data)

	return &Message{
		Version: CurrentMessageVersion,
		Type:    MsgChunk,
		Payload: payload,
	}, nil
}

func ParseChunk(payload []byte) (*core.Chunk, error) {
	chunk := &core.Chunk{}
	r := bytes.NewReader(payload)
	if err := binary.Read(r, binary.LittleEndian, &chunk.Header); err != nil {
		return nil, err
	}
	offset := len(payload) - r.Len()
	chunk.Data = payload[offset:]
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
