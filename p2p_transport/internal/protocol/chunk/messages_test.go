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
	// A new version comes in, we read it
	msg := &chunk.Message{
		Version: 2, // Newer version
		Type:    chunk.MsgRequestManifest,
		Payload: []byte("something"),
	}

	var buf bytes.Buffer
	if err := chunk.WriteMessage(&buf, msg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	// When reading, we could theoretically reject it inside ReadMessage if we strictly check version.
	// We didn't enforce it in ReadMessage yet, let's enforce it in the handler/application logic, 
	// or we can add it to ReadMessage. For now, let's just make sure we can parse the envelope and 
	// the application handler can reject `msg.Version != CurrentMessageVersion`.
	parsedMsg, err := chunk.ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if parsedMsg.Version != 2 {
		t.Errorf("Expected parsed version to remain intact")
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

	parsedMsg, _ := chunk.ReadMessage(&buf)
	
	// Payload parser should reject it
	_, err := chunk.ParseRequestManifest(parsedMsg.Payload)
	if err == nil {
		t.Error("Expected error parsing malformed REQUEST_MANIFEST, got nil")
	}
}

func TestProtocolCompatibility_UnsupportedMessage(t *testing.T) {
	msg := &chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    0x99, // Unknown type
		Payload: []byte{},
	}
	var buf bytes.Buffer
	chunk.WriteMessage(&buf, msg)

	_, err := chunk.ReadMessage(&buf)
	if err == nil {
		t.Error("Expected error reading unsupported message type, got nil")
	}
	if !errors.Is(err, chunk.ErrUnknownMessageType) {
		t.Errorf("Expected ErrUnknownMessageType, got %v", err)
	}
}

func TestReadMessage_RejectsInflatedControlMessageFrameSize(t *testing.T) {
	var buf bytes.Buffer

	// Claim 2MB frame size for MsgRequestManifest (which only allows MaxChunkRequestSize = 512 bytes payload)
	inflatedSize := uint32(2 * 1024 * 1024)
	if err := binary.Write(&buf, binary.LittleEndian, inflatedSize); err != nil {
		t.Fatalf("failed to write frame size: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion); err != nil {
		t.Fatalf("failed to write version: %v", err)
	}
	if err := buf.WriteByte(byte(chunk.MsgRequestManifest)); err != nil {
		t.Fatalf("failed to write message type: %v", err)
	}

	// ReadMessage must peek header, detect oversized payload for MsgRequestManifest, and fail immediately without needing 2MB payload data.
	_, err := chunk.ReadMessage(&buf)
	if err == nil {
		t.Fatal("Expected error for inflated frame size, got nil")
	}
	if !errors.Is(err, chunk.ErrPayloadTooLarge) {
		t.Fatalf("Expected ErrPayloadTooLarge, got %v", err)
	}
}

func TestReadMessage_RejectsFrameSmallerThanMessageHeader(t *testing.T) {
	var buf bytes.Buffer
	tooSmallSize := uint32(2) // smaller than 3-byte message header
	if err := binary.Write(&buf, binary.LittleEndian, tooSmallSize); err != nil {
		t.Fatalf("failed to write frame size: %v", err)
	}

	_, err := chunk.ReadMessage(&buf)
	if err == nil {
		t.Fatal("Expected error for frame size smaller than message header, got nil")
	}
	if !errors.Is(err, chunk.ErrInvalidPayloadSize) {
		t.Fatalf("Expected ErrInvalidPayloadSize, got %v", err)
	}
}
