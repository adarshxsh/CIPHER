package push

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"cipher/internal/content/core"
)

const (
	CurrentPushVersion uint16 = 1

	MaxMessageSize  uint32        = 4 * 1024 * 1024 // 4 MB
	MaxChunkSize    uint32        = 2 * 1024 * 1024 // 2 MB
	MaxManifestSize uint32        = 2 * 1024 * 1024 // 2 MB

	ReadTimeout  = 15 * time.Second
	WriteTimeout = 15 * time.Second
	AckTimeout   = 30 * time.Second
)

type PushMessageType uint8

const (
	MsgPushManifest         PushMessageType = 0x10
	MsgPushManifestAck      PushMessageType = 0x11
	MsgPushChunk            PushMessageType = 0x12
	MsgPushChunkAck         PushMessageType = 0x13
	MsgPushBatchComplete    PushMessageType = 0x14
	MsgPushBatchCompleteAck PushMessageType = 0x15
	MsgPushError            PushMessageType = 0x16
)

const (
	PushStatusOK               byte = 0x00
	PushStatusUnauthorized     byte = 0x01
	PushStatusDiskFull         byte = 0x02
	PushStatusMalformed        byte = 0x03
	PushStatusIncomplete       byte = 0x04
	PushStatusHashMismatch     byte = 0x05
	PushStatusNotInAssignedSet byte = 0x06
	PushStatusIOError          byte = 0x07
)

type PushMessage struct {
	Version uint16
	Type    PushMessageType
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

func WritePushMessage(w io.Writer, msg *PushMessage) error {
	payloadLen := len(msg.Payload)
	size := uint32(3 + payloadLen)

	if size > MaxMessageSize {
		return fmt.Errorf("message size %d exceeds limit %d", size, MaxMessageSize)
	}

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

func ReadPushMessage(r io.Reader) (*PushMessage, error) {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return nil, err
	}
	size := binary.LittleEndian.Uint32(sizeBuf[:])

	if size > MaxMessageSize {
		return nil, fmt.Errorf("message size %d exceeds maximum frame size %d", size, MaxMessageSize)
	}
	if size < 3 { // Must have at least Version (2) + Type (1)
		return nil, errors.New("message frame too short")
	}

	frame := make([]byte, size)
	if _, err := io.ReadFull(r, frame); err != nil {
		return nil, err
	}

	version := binary.LittleEndian.Uint16(frame[0:2])
	msgType := PushMessageType(frame[2])

	return &PushMessage{
		Version: version,
		Type:    msgType,
		Payload: frame[3:],
	}, nil
}

// -- Payload Builders & Parsers --

func BuildPushManifest(contentID core.ContentID, assignedChunkIDs []core.ChunkID, manifestData []byte) *PushMessage {
	count := uint32(len(assignedChunkIDs))
	payloadLen := 36 + int(count)*32 + len(manifestData)
	payload := make([]byte, payloadLen)

	copy(payload[0:32], contentID[:])
	binary.LittleEndian.PutUint32(payload[32:36], count)

	offset := 36
	for _, cid := range assignedChunkIDs {
		copy(payload[offset:offset+32], cid[:])
		offset += 32
	}
	copy(payload[offset:], manifestData)

	return &PushMessage{
		Version: CurrentPushVersion,
		Type:    MsgPushManifest,
		Payload: payload,
	}
}

func ParsePushManifest(payload []byte) (core.ContentID, []core.ChunkID, []byte, error) {
	var contentID core.ContentID
	if len(payload) < 36 { // 32 bytes ContentID + 4 bytes count
		return contentID, nil, nil, errors.New("invalid payload length for PUSH_MANIFEST")
	}

	copy(contentID[:], payload[:32])
	count := binary.LittleEndian.Uint32(payload[32:36])

	expectedOffset := 36 + int(count)*32
	if len(payload) < expectedOffset {
		return contentID, nil, nil, fmt.Errorf("payload length %d too short for %d assigned chunks", len(payload), count)
	}

	assignedChunkIDs := make([]core.ChunkID, count)
	for i := 0; i < int(count); i++ {
		offset := 36 + i*32
		copy(assignedChunkIDs[i][:], payload[offset:offset+32])
	}

	manifestData := payload[expectedOffset:]
	return contentID, assignedChunkIDs, manifestData, nil
}

func BuildPushManifestAck(contentID core.ContentID, status byte) *PushMessage {
	payload := make([]byte, 33)
	copy(payload[:32], contentID[:])
	payload[32] = status
	return &PushMessage{
		Version: CurrentPushVersion,
		Type:    MsgPushManifestAck,
		Payload: payload,
	}
}

func ParsePushManifestAck(payload []byte) (core.ContentID, byte, error) {
	var id core.ContentID
	if len(payload) != 33 {
		return id, 0, fmt.Errorf("invalid payload length for PUSH_MANIFEST_ACK: %d", len(payload))
	}
	copy(id[:], payload[:32])
	return id, payload[32], nil
}

func BuildPushChunk(contentID core.ContentID, chunk *core.Chunk) (*PushMessage, error) {
	if chunk == nil {
		return nil, errors.New("nil chunk")
	}
	payloadLen := 32 + 66 + len(chunk.Data)
	payload := make([]byte, payloadLen)

	copy(payload[0:32], contentID[:])
	encodeChunkHeader(&chunk.Header, payload[32:98])
	copy(payload[98:], chunk.Data)

	return &PushMessage{
		Version: CurrentPushVersion,
		Type:    MsgPushChunk,
		Payload: payload,
	}, nil
}

func ParsePushChunk(payload []byte) (core.ContentID, *core.Chunk, error) {
	var contentID core.ContentID
	const headerSize = 66

	if len(payload) < 32+headerSize {
		return contentID, nil, errors.New("invalid payload length for PUSH_CHUNK")
	}

	copy(contentID[:], payload[:32])

	chunk := &core.Chunk{}
	decodeChunkHeader(payload[32:32+headerSize], &chunk.Header)
	chunk.Data = payload[32+headerSize:]

	return contentID, chunk, nil
}

func BuildPushChunkAck(chunkID core.ChunkID, status byte) *PushMessage {
	payload := make([]byte, 33)
	copy(payload[:32], chunkID[:])
	payload[32] = status
	return &PushMessage{
		Version: CurrentPushVersion,
		Type:    MsgPushChunkAck,
		Payload: payload,
	}
}

func ParsePushChunkAck(payload []byte) (core.ChunkID, byte, error) {
	var id core.ChunkID
	if len(payload) != 33 {
		return id, 0, fmt.Errorf("invalid payload length for PUSH_CHUNK_ACK: %d", len(payload))
	}
	copy(id[:], payload[:32])
	return id, payload[32], nil
}

func BuildPushBatchComplete(contentID core.ContentID) *PushMessage {
	payload := make([]byte, 32)
	copy(payload, contentID[:])
	return &PushMessage{
		Version: CurrentPushVersion,
		Type:    MsgPushBatchComplete,
		Payload: payload,
	}
}

func ParsePushBatchComplete(payload []byte) (core.ContentID, error) {
	var id core.ContentID
	if len(payload) != 32 {
		return id, fmt.Errorf("invalid payload length for PUSH_BATCH_COMPLETE: %d", len(payload))
	}
	copy(id[:], payload)
	return id, nil
}

func BuildPushBatchCompleteAck(contentID core.ContentID, status byte) *PushMessage {
	payload := make([]byte, 33)
	copy(payload[:32], contentID[:])
	payload[32] = status
	return &PushMessage{
		Version: CurrentPushVersion,
		Type:    MsgPushBatchCompleteAck,
		Payload: payload,
	}
}

func ParsePushBatchCompleteAck(payload []byte) (core.ContentID, byte, error) {
	var id core.ContentID
	if len(payload) != 33 {
		return id, 0, fmt.Errorf("invalid payload length for PUSH_BATCH_COMPLETE_ACK: %d", len(payload))
	}
	copy(id[:], payload[:32])
	return id, payload[32], nil
}

func BuildPushError(code byte, msg string) *PushMessage {
	payload := make([]byte, 1+len(msg))
	payload[0] = code
	copy(payload[1:], msg)
	return &PushMessage{
		Version: CurrentPushVersion,
		Type:    MsgPushError,
		Payload: payload,
	}
}

func ParsePushError(payload []byte) (byte, string, error) {
	if len(payload) < 1 {
		return 0, "", errors.New("invalid payload length for PUSH_ERROR")
	}
	return payload[0], string(payload[1:]), nil
}

