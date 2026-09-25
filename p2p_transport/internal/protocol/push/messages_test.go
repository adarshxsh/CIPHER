package push

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"testing"

	"cipher/internal/content/core"
)

func TestPushMessageFraming(t *testing.T) {
	origMsg := BuildPushError(0x05, "disk full error")
	buf := new(bytes.Buffer)

	if err := WritePushMessage(buf, origMsg); err != nil {
		t.Fatalf("WritePushMessage failed: %v", err)
	}

	readMsg, err := ReadPushMessage(buf)
	if err != nil {
		t.Fatalf("ReadPushMessage failed: %v", err)
	}

	if readMsg.Version != origMsg.Version {
		t.Errorf("version mismatch: got %d, want %d", readMsg.Version, origMsg.Version)
	}
	if readMsg.Type != origMsg.Type {
		t.Errorf("type mismatch: got %d, want %d", readMsg.Type, origMsg.Type)
	}

	code, str, err := ParsePushError(readMsg.Payload)
	if err != nil {
		t.Fatalf("ParsePushError failed: %v", err)
	}
	if code != 0x05 || str != "disk full error" {
		t.Errorf("error payload mismatch: got code=%d, msg=%s", code, str)
	}
}

func TestPushManifestSerialization(t *testing.T) {
	var contentID core.ContentID
	_, _ = rand.Read(contentID[:])

	var cid1, cid2, cid3 core.ChunkID
	_, _ = rand.Read(cid1[:])
	_, _ = rand.Read(cid2[:])
	_, _ = rand.Read(cid3[:])

	assigned := []core.ChunkID{cid1, cid2, cid3}
	manifestJSON := []byte(`{"version":1,"descriptor":{"id":"abc"}}`)

	msg := BuildPushManifest(contentID, assigned, manifestJSON)
	parsedCID, parsedAssigned, parsedJSON, err := ParsePushManifest(msg.Payload)
	if err != nil {
		t.Fatalf("ParsePushManifest failed: %v", err)
	}

	if parsedCID != contentID {
		t.Errorf("contentID mismatch")
	}
	if len(parsedAssigned) != len(assigned) {
		t.Fatalf("assigned chunk count mismatch: got %d, want %d", len(parsedAssigned), len(assigned))
	}
	for i := range assigned {
		if parsedAssigned[i] != assigned[i] {
			t.Errorf("chunk %d mismatch", i)
		}
	}
	if string(parsedJSON) != string(manifestJSON) {
		t.Errorf("manifest json mismatch: got %s, want %s", string(parsedJSON), string(manifestJSON))
	}
}

func TestPushChunkSerialization(t *testing.T) {
	var contentID core.ContentID
	var chunkID core.ChunkID
	_, _ = rand.Read(contentID[:])
	_, _ = rand.Read(chunkID[:])

	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         chunkID,
			Index:      4,
			Offset:     1024,
			PlainSize:  512,
			CipherSize: 528,
		},
		Data: []byte("encrypted-ciphertext-data-payload-example"),
	}

	msg, err := BuildPushChunk(contentID, chunk)
	if err != nil {
		t.Fatalf("BuildPushChunk failed: %v", err)
	}

	parsedCID, parsedChunk, err := ParsePushChunk(msg.Payload)
	if err != nil {
		t.Fatalf("ParsePushChunk failed: %v", err)
	}

	if parsedCID != contentID {
		t.Errorf("contentID mismatch")
	}
	if parsedChunk.Header.ID != chunkID {
		t.Errorf("chunk ID mismatch")
	}
	if parsedChunk.Header.Index != 4 {
		t.Errorf("chunk index mismatch")
	}
	if !bytes.Equal(parsedChunk.Data, chunk.Data) {
		t.Errorf("chunk data mismatch")
	}
}

func TestFrameSizeLimits(t *testing.T) {
	// Attempt to create a message larger than MaxMessageSize
	hugePayload := make([]byte, MaxMessageSize+1)
	msg := &PushMessage{
		Version: CurrentPushVersion,
		Type:    MsgPushChunk,
		Payload: hugePayload,
	}

	buf := new(bytes.Buffer)
	err := WritePushMessage(buf, msg)
	if err == nil {
		t.Fatalf("expected error for oversized message, got nil")
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

func TestReadPushMessage_RejectsInflatedControlHeader(t *testing.T) {
	// Send MsgPushChunkAck (limit: 33 bytes) with declared length of 500,000 bytes
	var buf bytes.Buffer
	inflatedSize := uint32(500000 + 3)
	_ = binary.Write(&buf, binary.LittleEndian, inflatedSize)
	_ = binary.Write(&buf, binary.LittleEndian, CurrentPushVersion)
	_ = buf.WriteByte(byte(MsgPushChunkAck))

	reader := &trackingReader{data: buf.Bytes()}
	_, err := ReadPushMessage(reader)
	if !errors.Is(err, ErrPushPayloadTooLarge) {
		t.Fatalf("expected ErrPushPayloadTooLarge, got %v", err)
	}
	if reader.bytesRead != 7 {
		t.Fatalf("expected exactly 7 bytes read before rejection, got %d", reader.bytesRead)
	}
}

func TestReadPushMessage_ValidAllTypes(t *testing.T) {
	var cid core.ContentID
	cid[0] = 0x55

	var chkID core.ChunkID
	chkID[0] = 0x66

	testCases := []struct {
		name string
		msg  *PushMessage
	}{
		{"ManifestAck", BuildPushManifestAck(cid, PushStatusOK)},
		{"ChunkAck", BuildPushChunkAck(chkID, PushStatusOK)},
		{"BatchComplete", BuildPushBatchComplete(cid)},
		{"BatchCompleteAck", BuildPushBatchCompleteAck(cid, PushStatusOK)},
		{"Error", BuildPushError(PushStatusDiskFull, "disk full")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WritePushMessage(&buf, tc.msg); err != nil {
				t.Fatalf("WritePushMessage failed: %v", err)
			}

			readMsg, err := ReadPushMessage(&buf)
			if err != nil {
				t.Fatalf("ReadPushMessage failed: %v", err)
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
