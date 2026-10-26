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

type trackingReader struct {
	data      []byte
	bytesRead int
}

func (r *trackingReader) Read(p []byte) (n int, err error) {
	if r.bytesRead >= len(r.data) {
		return 0, errors.New("EOF - no more data in tracking reader")
	}
	n = copy(p, r.data[r.bytesRead:])
	r.bytesRead += n
	return n, nil
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

	reader := &trackingReader{data: buf.Bytes()}
	_, err := chunk.DecodeFrame(reader)
	if !errors.Is(err, chunk.ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
	if reader.bytesRead != 7 {
		t.Fatalf("expected exactly 7 bytes read during header inspection before rejection, got %d", reader.bytesRead)
	}
}

func TestReadMessage_RejectsControlRequestInflatedHeader(t *testing.T) {
	// Inflated frame declaring 1MB for a MsgRequestChunk (limit: 512 bytes)
	var buf bytes.Buffer
	inflatedSize := uint32(1000000 + 3)
	_ = binary.Write(&buf, binary.LittleEndian, inflatedSize)
	_ = binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion)
	_ = buf.WriteByte(byte(chunk.MsgRequestChunk))

	reader := &trackingReader{data: buf.Bytes()}
	_, err := chunk.ReadMessage(reader)
	if !errors.Is(err, chunk.ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
	if reader.bytesRead != 7 {
		t.Fatalf("expected exactly 7 bytes read before rejection, got %d", reader.bytesRead)
	}
}

func TestReadMessage_ValidAllTypes(t *testing.T) {
	var cid core.ContentID
	cid[0] = 0x12

	var chkID core.ChunkID
	chkID[0] = 0x34

	testCases := []struct {
		name string
		msg  *chunk.Message
	}{
		{"RequestManifest", chunk.BuildRequestManifest(cid)},
		{"Manifest", chunk.BuildManifest(cid, []byte(`{"version":1}`))},
		{"RequestChunk", chunk.BuildRequestChunk(chkID)},
		{"Ack", chunk.BuildAck(chkID, 0)},
		{"Error", chunk.BuildError(chunk.ErrChunkNotFound, "chunk not found")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := chunk.WriteMessage(&buf, tc.msg); err != nil {
				t.Fatalf("WriteMessage failed: %v", err)
			}

			readMsg, err := chunk.ReadMessage(&buf)
			if err != nil {
				t.Fatalf("ReadMessage failed: %v", err)
			}

			if readMsg.Type != tc.msg.Type {
				t.Errorf("type mismatch: got %d, want %d", readMsg.Type, tc.msg.Type)
			}
			if readMsg.Version != tc.msg.Version {
				t.Errorf("version mismatch: got %d, want %d", readMsg.Version, tc.msg.Version)
			}
		})
	}
}
