package chunk_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/protocol/chunk"
)

func TestDecodeFrame_ReadsWriteMessageFrame(t *testing.T) {
	var contentID core.ContentID
	contentID[0] = 0xAA
	contentID[31] = 0xBB

	msg := chunk.BuildRequestManifest(contentID)

	var buf bytes.Buffer
	if err := chunk.WriteMessage(&buf, msg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	frame, err := chunk.DecodeFrame(&buf)
	if err != nil {
		t.Fatalf("DecodeFrame failed: %v", err)
	}

	if frame.Version != chunk.CurrentMessageVersion {
		t.Fatalf("expected version %d, got %d", chunk.CurrentMessageVersion, frame.Version)
	}
	if frame.MessageType != chunk.MsgRequestManifest {
		t.Fatalf("expected message type %d, got %d", chunk.MsgRequestManifest, frame.MessageType)
	}

	parsedID, err := chunk.ParseRequestManifest(frame.Payload)
	if err != nil {
		t.Fatalf("ParseRequestManifest failed: %v", err)
	}
	if parsedID != contentID {
		t.Fatalf("expected content ID %x, got %x", contentID, parsedID)
	}
}

func TestDecodeFrame_RejectsOversizedPayloadBeforeAllocation(t *testing.T) {
	var buf bytes.Buffer

	frameSize := uint32(chunk.MaxChunkRequestSize + 1 + 3)
	if err := binary.Write(&buf, binary.LittleEndian, frameSize); err != nil {
		t.Fatalf("failed to write frame size: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion); err != nil {
		t.Fatalf("failed to write version: %v", err)
	}
	if err := buf.WriteByte(byte(chunk.MsgRequestChunk)); err != nil {
		t.Fatalf("failed to write message type: %v", err)
	}

	_, err := chunk.DecodeFrame(&buf)
	if !errors.Is(err, chunk.ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
}

func TestDecodeFrame_RejectsUnknownMessageTypeBeforeAllocation(t *testing.T) {
	var buf bytes.Buffer

	// 2MB frame length declared, but unknown message type 0x99
	frameSize := uint32(2 * 1024 * 1024)
	if err := binary.Write(&buf, binary.LittleEndian, frameSize); err != nil {
		t.Fatalf("failed to write frame size: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion); err != nil {
		t.Fatalf("failed to write version: %v", err)
	}
	if err := buf.WriteByte(0x99); err != nil {
		t.Fatalf("failed to write message type: %v", err)
	}

	_, err := chunk.DecodeFrame(&buf)
	if !errors.Is(err, chunk.ErrUnknownMessageType) {
		t.Fatalf("expected ErrUnknownMessageType, got %v", err)
	}
}

func TestDecodeFrame_RejectsInvalidPayloadSize(t *testing.T) {
	// 1. Frame size smaller than message header (e.g. 2 bytes < 3 bytes)
	var buf1 bytes.Buffer
	binary.Write(&buf1, binary.LittleEndian, uint32(2))
	binary.Write(&buf1, binary.LittleEndian, chunk.CurrentMessageVersion)
	buf1.WriteByte(byte(chunk.MsgRequestChunk))
	_, err := chunk.DecodeFrame(&buf1)
	if !errors.Is(err, chunk.ErrInvalidPayloadSize) {
		t.Fatalf("expected ErrInvalidPayloadSize for frame size < message header, got %v", err)
	}

	// 2. Declared payload size == 0 (frame size == 3)
	var buf2 bytes.Buffer
	binary.Write(&buf2, binary.LittleEndian, uint32(3))
	binary.Write(&buf2, binary.LittleEndian, chunk.CurrentMessageVersion)
	buf2.WriteByte(byte(chunk.MsgRequestChunk))
	_, err = chunk.DecodeFrame(&buf2)
	if !errors.Is(err, chunk.ErrInvalidPayloadSize) {
		t.Fatalf("expected ErrInvalidPayloadSize for empty payload, got %v", err)
	}
}

func TestDecodeFrame_RejectsInvalidProtocolVersion(t *testing.T) {
	var buf bytes.Buffer

	frameSize := uint32(35)
	binary.Write(&buf, binary.LittleEndian, frameSize)
	binary.Write(&buf, binary.LittleEndian, uint16(999)) // unsupported version
	buf.WriteByte(byte(chunk.MsgRequestManifest))

	_, err := chunk.DecodeFrame(&buf)
	if !errors.Is(err, chunk.ErrInvalidProtocolVersion) {
		t.Fatalf("expected ErrInvalidProtocolVersion, got %v", err)
	}
}

func TestDecodeFrameHeader_ValidAndInvalid(t *testing.T) {
	var buf bytes.Buffer
	frameSize := uint32(35)
	binary.Write(&buf, binary.LittleEndian, frameSize)
	binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion)
	buf.WriteByte(byte(chunk.MsgRequestManifest))

	version, msgType, payloadSize, err := chunk.DecodeFrameHeader(&buf)
	if err != nil {
		t.Fatalf("DecodeFrameHeader failed: %v", err)
	}
	if version != chunk.CurrentMessageVersion {
		t.Errorf("expected version %d, got %d", chunk.CurrentMessageVersion, version)
	}
	if msgType != chunk.MsgRequestManifest {
		t.Errorf("expected msgType %d, got %d", chunk.MsgRequestManifest, msgType)
	}
	if payloadSize != 32 {
		t.Errorf("expected payloadSize 32, got %d", payloadSize)
	}
}
