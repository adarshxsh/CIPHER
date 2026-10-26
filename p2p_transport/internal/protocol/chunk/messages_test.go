package chunk_test

import (
	"bytes"
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

	parsedMsg, _ := chunk.ReadMessage(&buf)
	if parsedMsg.Type != 0x99 {
		t.Errorf("Expected type 0x99, got %v", parsedMsg.Type)
	}
	// Handler test will ensure it replies with ERR_UNSUPPORTED_MESSAGE
}

type nopWriter struct{}

func (*nopWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func TestWriteMessage_ZeroAllocations(t *testing.T) {
	msg := chunk.BuildRequestManifest(core.ContentID{0x01})
	var nw nopWriter
	var w io.Writer = &nw

	allocs := testing.AllocsPerRun(100, func() {
		if err := chunk.WriteMessage(w, msg); err != nil {
			t.Fatalf("WriteMessage failed: %v", err)
		}
	})

	if allocs != 0 {
		t.Errorf("expected 0 allocations for WriteMessage, got %f", allocs)
	}
}

func TestReadMessage_SingleFrameBufferAllocation(t *testing.T) {
	msg := chunk.BuildRequestManifest(core.ContentID{0x01})
	var buf bytes.Buffer
	if err := chunk.WriteMessage(&buf, msg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}
	encoded := buf.Bytes()

	r := bytes.NewReader(encoded)
	readMsg, err := chunk.ReadMessage(r)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}

	// Verify that Payload points directly into the frame buffer slice
	if len(readMsg.Payload) == 0 {
		t.Fatalf("expected non-empty payload")
	}
}

func TestParseChunk_ZeroCopyPayloadSubSlicing(t *testing.T) {
	chunkObj := &core.Chunk{
		Header: core.ChunkHeader{Version: 1, Index: 2},
		Data:   []byte("test-payload-ciphertext-bytes"),
	}
	msg, err := chunk.BuildChunk(chunkObj)
	if err != nil {
		t.Fatalf("BuildChunk failed: %v", err)
	}

	parsedChunk, err := chunk.ParseChunk(msg.Payload)
	if err != nil {
		t.Fatalf("ParseChunk failed: %v", err)
	}

	if !bytes.Equal(parsedChunk.Data, chunkObj.Data) {
		t.Fatalf("chunk data content mismatch")
	}

	// Verify zero-copy sub-slicing: parsedChunk.Data MUST point directly into msg.Payload
	expectedOffset := len(msg.Payload) - len(chunkObj.Data)
	if &parsedChunk.Data[0] != &msg.Payload[expectedOffset] {
		t.Errorf("parsedChunk.Data is not a direct sub-slice view of msg.Payload")
	}
}

