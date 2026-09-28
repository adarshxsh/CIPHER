package push_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/protocol/push"
)

func TestDecodePushFrame_ReadsWritePushMessageFrame(t *testing.T) {
	var contentID core.ContentID
	contentID[0] = 0xAA
	contentID[31] = 0xBB

	msg := push.BuildPushBatchComplete(contentID)

	var buf bytes.Buffer
	if err := push.WritePushMessage(&buf, msg); err != nil {
		t.Fatalf("WritePushMessage failed: %v", err)
	}

	frame, err := push.DecodePushFrame(&buf)
	if err != nil {
		t.Fatalf("DecodePushFrame failed: %v", err)
	}

	if frame.Version != push.CurrentPushVersion {
		t.Fatalf("expected version %d, got %d", push.CurrentPushVersion, frame.Version)
	}
	if frame.MessageType != push.MsgPushBatchComplete {
		t.Fatalf("expected message type %d, got %d", push.MsgPushBatchComplete, frame.MessageType)
	}

	parsedID, err := push.ParsePushBatchComplete(frame.Payload)
	if err != nil {
		t.Fatalf("ParsePushBatchComplete failed: %v", err)
	}
	if parsedID != contentID {
		t.Fatalf("expected content ID %x, got %x", contentID, parsedID)
	}
}

func TestDecodePushFrame_RejectsOversizedPayloadBeforeAllocation(t *testing.T) {
	var buf bytes.Buffer

	// PUSH_BATCH_COMPLETE payload limit is 32 bytes.
	// Declare a payload size of 1024 bytes (1024 + 3 = 1027 frame size).
	frameSize := uint32(1024 + 3)
	if err := binary.Write(&buf, binary.LittleEndian, frameSize); err != nil {
		t.Fatalf("failed to write frame size: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, push.CurrentPushVersion); err != nil {
		t.Fatalf("failed to write version: %v", err)
	}
	if err := buf.WriteByte(byte(push.MsgPushBatchComplete)); err != nil {
		t.Fatalf("failed to write message type: %v", err)
	}

	// Buffer has no payload data written.
	_, err := push.DecodePushFrame(&buf)
	if !errors.Is(err, push.ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
}

func TestDecodePushFrame_RejectsInvalidVersion(t *testing.T) {
	var buf bytes.Buffer

	frameSize := uint32(32 + 3)
	if err := binary.Write(&buf, binary.LittleEndian, frameSize); err != nil {
		t.Fatalf("failed to write frame size: %v", err)
	}
	invalidVersion := uint16(99)
	if err := binary.Write(&buf, binary.LittleEndian, invalidVersion); err != nil {
		t.Fatalf("failed to write version: %v", err)
	}
	if err := buf.WriteByte(byte(push.MsgPushBatchComplete)); err != nil {
		t.Fatalf("failed to write message type: %v", err)
	}

	_, err := push.DecodePushFrame(&buf)
	if !errors.Is(err, push.ErrInvalidProtocolVersion) {
		t.Fatalf("expected ErrInvalidProtocolVersion, got %v", err)
	}
}

func TestDecodePushFrame_RejectsUnknownMessageType(t *testing.T) {
	var buf bytes.Buffer

	frameSize := uint32(10 + 3)
	if err := binary.Write(&buf, binary.LittleEndian, frameSize); err != nil {
		t.Fatalf("failed to write frame size: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, push.CurrentPushVersion); err != nil {
		t.Fatalf("failed to write version: %v", err)
	}
	unknownType := byte(0xFF)
	if err := buf.WriteByte(unknownType); err != nil {
		t.Fatalf("failed to write message type: %v", err)
	}

	_, err := push.DecodePushFrame(&buf)
	if !errors.Is(err, push.ErrUnknownMessageType) {
		t.Fatalf("expected ErrUnknownMessageType, got %v", err)
	}
}

func TestDecodePushFrame_RejectsEmptyPayload(t *testing.T) {
	var buf bytes.Buffer

	frameSize := uint32(3) // 0 payload
	if err := binary.Write(&buf, binary.LittleEndian, frameSize); err != nil {
		t.Fatalf("failed to write frame size: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, push.CurrentPushVersion); err != nil {
		t.Fatalf("failed to write version: %v", err)
	}
	if err := buf.WriteByte(byte(push.MsgPushBatchComplete)); err != nil {
		t.Fatalf("failed to write message type: %v", err)
	}

	_, err := push.DecodePushFrame(&buf)
	if !errors.Is(err, push.ErrInvalidPayloadSize) {
		t.Fatalf("expected ErrInvalidPayloadSize, got %v", err)
	}
}
