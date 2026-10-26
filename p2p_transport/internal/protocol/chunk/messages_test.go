package chunk_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
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
	// A new version comes in, ReadMessage rejects it at the header phase
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
		t.Fatalf("expected ErrInvalidProtocolVersion, got %v", err)
	}
}

func TestProtocolCompatibility_MalformedMessage(t *testing.T) {
	// Empty payload for a REQUEST_MANIFEST
	msg := &chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    chunk.MsgRequestManifest,
		Payload: []byte{0x00}, // Too short!
	}
	var buf bytes.Buffer
	chunk.WriteMessage(&buf, msg)

	parsedMsg, err := chunk.ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage unexpectedly failed: %v", err)
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
		Payload: []byte{1, 2, 3},
	}
	var buf bytes.Buffer
	chunk.WriteMessage(&buf, msg)

	_, err := chunk.ReadMessage(&buf)
	if !errors.Is(err, chunk.ErrUnknownMessageType) {
		t.Fatalf("expected ErrUnknownMessageType, got %v", err)
	}
}

func TestReadMessage_OversizedControlRequest(t *testing.T) {
	// Create header for MsgRequestChunk declaring 1MB payload (> MaxChunkRequestSize 512 bytes)
	var buf bytes.Buffer
	frameSize := uint32(1024*1024 + 3)
	if err := binary.Write(&buf, binary.LittleEndian, frameSize); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := buf.WriteByte(byte(chunk.MsgRequestChunk)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Note: Only the 7-byte header is written to buf, 0 payload bytes are written.
	// ReadMessage must inspect the 7-byte header and reject immediately with ErrPayloadTooLarge
	// without attempting to allocate or read the 1MB payload.
	_, err := chunk.ReadMessage(&buf)
	if !errors.Is(err, chunk.ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
}

func TestReadMessage_OversizedFrameHeader(t *testing.T) {
	var buf bytes.Buffer
	frameSize := uint32(chunk.MaxFrameSize + 1024)
	if err := binary.Write(&buf, binary.LittleEndian, frameSize); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := buf.WriteByte(byte(chunk.MsgRequestChunk)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	_, err := chunk.ReadMessage(&buf)
	if !errors.Is(err, chunk.ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
}

func TestReadMessage_CleanEOF(t *testing.T) {
	var buf bytes.Buffer
	_, err := chunk.ReadMessage(&buf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestReadMessage_TruncatedHeader(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0x01, 0x02, 0x03}) // only 3 bytes

	_, err := chunk.ReadMessage(&buf)
	if !errors.Is(err, chunk.ErrTruncatedFrame) {
		t.Fatalf("expected ErrTruncatedFrame, got %v", err)
	}
}
