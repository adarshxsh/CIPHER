package push

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// Frame header:
	// 4 bytes - envelope length, little-endian
	// 2 bytes - protocol version, little-endian
	// 1 byte  - message type
	frameHeaderSize = 7

	frameLengthSize   = 4
	messageHeaderSize = 3
)

var (
	ErrInvalidProtocolVersion = errors.New("invalid protocol version")
	ErrUnknownMessageType     = errors.New("unknown message type")
	ErrInvalidPayloadSize     = errors.New("invalid payload size")
	ErrPayloadTooLarge        = errors.New("payload exceeds protocol limit")
	ErrTruncatedFrame         = errors.New("truncated protocol frame")
)

// PushFrame represents one safely decoded push-protocol frame.
//
// Payload is only allocated after its declared size has been checked against
// the limit for the corresponding message type.
type PushFrame struct {
	Version     uint16
	MessageType PushMessageType
	Payload     []byte
}

// MaxPayloadSizeForPushMessage returns the maximum payload size accepted for a
// particular push protocol message type.
func MaxPayloadSizeForPushMessage(messageType PushMessageType) int {
	switch messageType {
	case MsgPushManifest:
		return int(MaxManifestSize) // 2 MB

	case MsgPushChunk:
		return int(MaxChunkSize) // 2 MB

	case MsgPushBatchComplete:
		return 32 // ContentID is 32 bytes

	case MsgPushManifestAck:
		return 33 // ContentID (32) + status (1)

	case MsgPushChunkAck:
		return 33 // ChunkID (32) + status (1)

	case MsgPushBatchCompleteAck:
		return 33 // ContentID (32) + status (1)

	case MsgPushError:
		return 1024 // 1 byte status/code + error message

	default:
		return 0
	}
}

// DecodePushFrame reads one complete push frame from r.
//
// It first reads the 7-byte header, validates protocol version, message type,
// and declared payload size, and only then allocates memory for the payload.
func DecodePushFrame(r io.Reader) (*PushFrame, error) {
	if r == nil {
		return nil, errors.New("push decoder: nil reader")
	}

	header := make([]byte, frameHeaderSize)

	if _, err := io.ReadFull(r, header); err != nil {
		return nil, fmt.Errorf("%w: failed to read frame header: %v", ErrTruncatedFrame, err)
	}

	frameSize := binary.LittleEndian.Uint32(header[:frameLengthSize])
	if frameSize < messageHeaderSize {
		return nil, fmt.Errorf(
			"%w: frame size %d is smaller than message header",
			ErrInvalidPayloadSize,
			frameSize,
		)
	}

	version := binary.LittleEndian.Uint16(header[frameLengthSize : frameLengthSize+2])
	if version != CurrentPushVersion {
		return nil, fmt.Errorf(
			"%w: received=%d supported=%d",
			ErrInvalidProtocolVersion,
			version,
			CurrentPushVersion,
		)
	}

	messageType := PushMessageType(header[frameLengthSize+2])

	maxPayloadSize := MaxPayloadSizeForPushMessage(messageType)
	if maxPayloadSize <= 0 {
		return nil, fmt.Errorf(
			"%w: %d",
			ErrUnknownMessageType,
			messageType,
		)
	}

	declaredSize := frameSize - messageHeaderSize

	if declaredSize == 0 {
		return nil, fmt.Errorf(
			"%w: message type %d declared an empty payload",
			ErrInvalidPayloadSize,
			messageType,
		)
	}

	if uint64(declaredSize) > uint64(maxPayloadSize) {
		return nil, fmt.Errorf(
			"%w: message type=%d declared=%d maximum=%d",
			ErrPayloadTooLarge,
			messageType,
			declaredSize,
			maxPayloadSize,
		)
	}

	// Allocation happens only after all size checks pass.
	payload := make([]byte, int(declaredSize))

	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf(
			"%w: expected=%d bytes: %v",
			ErrTruncatedFrame,
			declaredSize,
			err,
		)
	}

	return &PushFrame{
		Version:     version,
		MessageType: messageType,
		Payload:     payload,
	}, nil
}

// DecodeExpectedPushFrame decodes a push frame and verifies that it has the expected message type.
func DecodeExpectedPushFrame(
	r io.Reader,
	expected PushMessageType,
) (*PushFrame, error) {
	frame, err := DecodePushFrame(r)
	if err != nil {
		return nil, err
	}

	if frame.MessageType != expected {
		return nil, fmt.Errorf(
			"unexpected message type: received=%d expected=%d",
			frame.MessageType,
			expected,
		)
	}

	return frame, nil
}

// DecodePushFrameHeader reads and validates only a push frame header.
func DecodePushFrameHeader(r io.Reader) (
	version uint16,
	messageType PushMessageType,
	payloadSize uint32,
	err error,
) {
	if r == nil {
		err = errors.New("push decoder: nil reader")
		return
	}

	header := make([]byte, frameHeaderSize)

	if _, readErr := io.ReadFull(r, header); readErr != nil {
		err = fmt.Errorf(
			"%w: failed to read frame header: %v",
			ErrTruncatedFrame,
			readErr,
		)
		return
	}

	frameSize := binary.LittleEndian.Uint32(header[:frameLengthSize])
	if frameSize < messageHeaderSize {
		err = fmt.Errorf(
			"%w: frame size %d is smaller than message header",
			ErrInvalidPayloadSize,
			frameSize,
		)
		return
	}

	version = binary.LittleEndian.Uint16(header[frameLengthSize : frameLengthSize+2])
	if version != CurrentPushVersion {
		err = fmt.Errorf(
			"%w: received=%d supported=%d",
			ErrInvalidProtocolVersion,
			version,
			CurrentPushVersion,
		)
		return
	}

	messageType = PushMessageType(header[frameLengthSize+2])

	maxPayloadSize := MaxPayloadSizeForPushMessage(messageType)
	if maxPayloadSize <= 0 {
		err = fmt.Errorf(
			"%w: %d",
			ErrUnknownMessageType,
			messageType,
		)
		return
	}

	payloadSize = frameSize - messageHeaderSize

	if payloadSize == 0 {
		err = fmt.Errorf(
			"%w: message type %d declared an empty payload",
			ErrInvalidPayloadSize,
			messageType,
		)
		return
	}

	if uint64(payloadSize) > uint64(maxPayloadSize) {
		err = fmt.Errorf(
			"%w: message type=%d declared=%d maximum=%d",
			ErrPayloadTooLarge,
			messageType,
			payloadSize,
			maxPayloadSize,
		)
		return
	}

	return
}

// ReadPushFramePayload reads an exact payload after DecodePushFrameHeader has checked its size.
func ReadPushFramePayload(
	r io.Reader,
	messageType PushMessageType,
	payloadSize uint32,
) ([]byte, error) {
	if r == nil {
		return nil, errors.New("push decoder: nil reader")
	}

	maxPayloadSize := MaxPayloadSizeForPushMessage(messageType)
	if maxPayloadSize <= 0 {
		return nil, fmt.Errorf(
			"%w: %d",
			ErrUnknownMessageType,
			messageType,
		)
	}

	if payloadSize == 0 {
		return nil, ErrInvalidPayloadSize
	}

	if uint64(payloadSize) > uint64(maxPayloadSize) {
		return nil, fmt.Errorf(
			"%w: declared=%d maximum=%d",
			ErrPayloadTooLarge,
			payloadSize,
			maxPayloadSize,
		)
	}

	payload := make([]byte, int(payloadSize))

	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf(
			"%w: expected=%d bytes: %v",
			ErrTruncatedFrame,
			payloadSize,
			err,
		)
	}

	return payload, nil
}
