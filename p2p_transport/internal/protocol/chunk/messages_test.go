package chunk_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/protocol/chunk"
)

func TestMessageEnvelope_Serialization(t *testing.T) {
	// 1. Build a message
	var contentID core.ContentID
	contentID[0] = 0xAA
	contentID[31] = 0xBB

	msg := chunk.BuildRequestManifest(contentID)

	// 2. Write it
	var buf bytes.Buffer
	if err := chunk.WriteMessage(&buf, msg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	// 3. Read it
	parsedMsg, err := chunk.ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}

	if parsedMsg.Version != chunk.CurrentMessageVersion {
		t.Errorf("expected version %d, got %d", chunk.CurrentMessageVersion, parsedMsg.Version)
	}
	if parsedMsg.Type != chunk.MsgRequestManifest {
		t.Errorf("expected type %d, got %d", chunk.MsgRequestManifest, parsedMsg.Type)
	}

	// 4. Parse payload
	parsedID, err := chunk.ParseRequestManifest(parsedMsg.Payload)
	if err != nil {
		t.Fatalf("ParseRequestManifest failed: %v", err)
	}
	if parsedID != contentID {
		t.Errorf("expected contentID %x, got %x", contentID, parsedID)
	}
}

func TestProtocolCompatibility_OldDecoder(t *testing.T) {
	// A new version comes in, ReadMessage now rejects it with ErrInvalidProtocolVersion
	msg := &chunk.Message{
		Version: 2, // Newer version
		Type:    chunk.MsgRequestManifest,
		Payload: make([]byte, 32),
	}

	var buf bytes.Buffer
	if err := chunk.WriteMessage(&buf, msg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	_, err := chunk.ReadMessage(&buf)
	if !errors.Is(err, chunk.ErrInvalidProtocolVersion) {
		t.Fatalf("Expected ErrInvalidProtocolVersion, got: %v", err)
	}
}

func TestProtocolCompatibility_MalformedMessage(t *testing.T) {
	// Payload too short for REQUEST_MANIFEST (e.g. 1 byte), but valid frame size > 0
	msg := &chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    chunk.MsgRequestManifest,
		Payload: []byte{0x00}, // Too short!
	}
	var buf bytes.Buffer
	chunk.WriteMessage(&buf, msg)

	parsedMsg, err := chunk.ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage failed unexpectedly: %v", err)
	}

	// Payload parser should reject it
	_, err = chunk.ParseRequestManifest(parsedMsg.Payload)
	if err == nil {
		t.Error("Expected error parsing malformed REQUEST_MANIFEST, got nil")
	}
}

func TestProtocolCompatibility_UnsupportedMessage(t *testing.T) {
	msg := &chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    0x99, // Unknown type
		Payload: []byte{0x01},
	}
	var buf bytes.Buffer
	chunk.WriteMessage(&buf, msg)

	_, err := chunk.ReadMessage(&buf)
	if !errors.Is(err, chunk.ErrUnknownMessageType) {
		t.Fatalf("Expected ErrUnknownMessageType, got: %v", err)
	}
}

func TestReadMessage_RejectsOversizedControlFrame(t *testing.T) {
	// Send a MsgRequestManifest with declared payload > 512 bytes (e.g. 1000 bytes)
	msg := &chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    chunk.MsgRequestManifest,
		Payload: make([]byte, chunk.MaxChunkRequestSize+100),
	}
	var buf bytes.Buffer
	if err := chunk.WriteMessage(&buf, msg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	_, err := chunk.ReadMessage(&buf)
	if !errors.Is(err, chunk.ErrPayloadTooLarge) {
		t.Fatalf("Expected ErrPayloadTooLarge, got %v", err)
	}
}

func TestReadMessage_RejectsSmallFrameSize(t *testing.T) {
	var buf bytes.Buffer
	// Frame size (2) smaller than messageHeaderSize (3 bytes)
	// Write complete 7-byte header (4-byte size + 2-byte version + 1-byte type)
	frameSize := uint32(2)
	binary.Write(&buf, binary.LittleEndian, frameSize)
	binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion)
	buf.WriteByte(byte(chunk.MsgRequestManifest))

	_, err := chunk.ReadMessage(&buf)
	if !errors.Is(err, chunk.ErrInvalidPayloadSize) {
		t.Fatalf("Expected ErrInvalidPayloadSize, got %v", err)
	}
}
