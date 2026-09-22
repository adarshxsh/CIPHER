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

func TestDecodeFrame_RejectsFrameExceedingMaxFrameSize(t *testing.T) {
	var buf bytes.Buffer

	frameSize := uint32(chunk.MaxFrameSize + 100)
	if err := binary.Write(&buf, binary.LittleEndian, frameSize); err != nil {
		t.Fatalf("failed to write frame size: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion); err != nil {
		t.Fatalf("failed to write version: %v", err)
	}
	if err := buf.WriteByte(byte(chunk.MsgChunk)); err != nil {
		t.Fatalf("failed to write message type: %v", err)
	}

	_, err := chunk.DecodeFrame(&buf)
	if !errors.Is(err, chunk.ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge for frame exceeding MaxFrameSize, got %v", err)
	}
}

func TestDecodeFrame_RejectsInvalidVersion(t *testing.T) {
	var buf bytes.Buffer

	frameSize := uint32(32 + 3)
	badVersion := uint16(999)
	if err := binary.Write(&buf, binary.LittleEndian, frameSize); err != nil {
		t.Fatalf("failed to write frame size: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, badVersion); err != nil {
		t.Fatalf("failed to write version: %v", err)
	}
	if err := buf.WriteByte(byte(chunk.MsgRequestManifest)); err != nil {
		t.Fatalf("failed to write message type: %v", err)
	}

	_, err := chunk.DecodeFrame(&buf)
	if !errors.Is(err, chunk.ErrInvalidProtocolVersion) {
		t.Fatalf("expected ErrInvalidProtocolVersion, got %v", err)
	}
}

func TestDecodeFrame_RejectsUnknownMessageType(t *testing.T) {
	var buf bytes.Buffer

	frameSize := uint32(32 + 3)
	if err := binary.Write(&buf, binary.LittleEndian, frameSize); err != nil {
		t.Fatalf("failed to write frame size: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion); err != nil {
		t.Fatalf("failed to write version: %v", err)
	}
	if err := buf.WriteByte(0xFF); err != nil { // Unknown type
		t.Fatalf("failed to write message type: %v", err)
	}

	_, err := chunk.DecodeFrame(&buf)
	if !errors.Is(err, chunk.ErrUnknownMessageType) {
		t.Fatalf("expected ErrUnknownMessageType, got %v", err)
	}
}

func TestDecodeFrame_RejectsFrameSmallerThanHeader(t *testing.T) {
	var buf bytes.Buffer

	frameSize := uint32(2) // smaller than messageHeaderSize (3)
	if err := binary.Write(&buf, binary.LittleEndian, frameSize); err != nil {
		t.Fatalf("failed to write frame size: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion); err != nil {
		t.Fatalf("failed to write version: %v", err)
	}
	if err := buf.WriteByte(byte(chunk.MsgRequestManifest)); err != nil {
		t.Fatalf("failed to write message type: %v", err)
	}

	_, err := chunk.DecodeFrame(&buf)
	if !errors.Is(err, chunk.ErrInvalidPayloadSize) {
		t.Fatalf("expected ErrInvalidPayloadSize, got %v", err)
	}
}

func TestDecodeFrameHeader_ValidAndInvalid(t *testing.T) {
	t.Run("valid header", func(t *testing.T) {
		var buf bytes.Buffer
		frameSize := uint32(32 + 3)
		binary.Write(&buf, binary.LittleEndian, frameSize)
		binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion)
		buf.WriteByte(byte(chunk.MsgRequestManifest))

		ver, msgType, payloadSize, err := chunk.DecodeFrameHeader(&buf)
		if err != nil {
			t.Fatalf("DecodeFrameHeader failed: %v", err)
		}
		if ver != chunk.CurrentMessageVersion || msgType != chunk.MsgRequestManifest || payloadSize != 32 {
			t.Fatalf("unexpected header decode values: ver=%d msgType=%d payloadSize=%d", ver, msgType, payloadSize)
		}
	})

	t.Run("invalid version", func(t *testing.T) {
		var buf bytes.Buffer
		frameSize := uint32(32 + 3)
		binary.Write(&buf, binary.LittleEndian, frameSize)
		binary.Write(&buf, binary.LittleEndian, uint16(99))
		buf.WriteByte(byte(chunk.MsgRequestManifest))

		_, _, _, err := chunk.DecodeFrameHeader(&buf)
		if !errors.Is(err, chunk.ErrInvalidProtocolVersion) {
			t.Fatalf("expected ErrInvalidProtocolVersion, got %v", err)
		}
	})
}
