package chunk_test

import (
	"bytes"
	"encoding/binary"
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
		t.Error("Expected error from ReadMessage for unsupported message type 0x99, got nil")
	}
}

func TestReadMessage_InflatedHeaderExploitPrevention(t *testing.T) {
	// Untrusted peer sends a 4-byte frame size of 2,000,000 bytes for MsgAck (type 0x05, max payload 33 bytes)
	var buf bytes.Buffer

	// Size: 2,000,000
	frameSize := uint32(2000000)
	binary.Write(&buf, binary.LittleEndian, frameSize)

	// Envelope: Version = 1, Type = MsgAck (0x05)
	binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion)
	buf.WriteByte(byte(chunk.MsgAck))

	// Followed by dummy data
	buf.Write(make([]byte, 100))

	_, err := chunk.ReadMessage(&buf)
	if err == nil {
		t.Fatal("Expected ReadMessage to reject inflated frame header for MsgAck, got nil error")
	}
}

func TestReadMessage_FrameSizeTooSmall(t *testing.T) {
	var buf bytes.Buffer

	// Size < 3
	frameSize := uint32(2)
	binary.Write(&buf, binary.LittleEndian, frameSize)

	_, err := chunk.ReadMessage(&buf)
	if err == nil {
		t.Fatal("Expected ReadMessage to reject frame size < 3, got nil error")
	}
}

func TestReadMessage_FrameSizeExceedsMaxFrameSize(t *testing.T) {
	var buf bytes.Buffer

	// Size > MaxFrameSize (2MB)
	frameSize := uint32(chunk.MaxFrameSize + 1)
	binary.Write(&buf, binary.LittleEndian, frameSize)

	_, err := chunk.ReadMessage(&buf)
	if err == nil {
		t.Fatal("Expected ReadMessage to reject frame size > MaxFrameSize, got nil error")
	}
}

func TestReadMessage_ZeroAllocBeforeValidation(t *testing.T) {
	// Construct an oversized Ack frame header in a byte slice
	var hdr [7]byte
	binary.LittleEndian.PutUint32(hdr[0:4], 2000000)
	binary.LittleEndian.PutUint16(hdr[4:6], chunk.CurrentMessageVersion)
	hdr[6] = byte(chunk.MsgAck)

	reader := bytes.NewReader(hdr[:])

	allocs := testing.AllocsPerRun(10, func() {
		reader.Reset(hdr[:])
		chunk.ReadMessage(reader)
	})

	// Heap allocations for payload buffering (make([]byte, 2000000)) must NOT occur.
	// Only minor Go interface boxing allocations (2) occur on returning the error.
	if allocs > 2 {
		t.Errorf("Expected <= 2 allocations on rejected oversized frame, got %f", allocs)
	}
}
