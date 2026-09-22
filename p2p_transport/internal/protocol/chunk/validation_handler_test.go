package chunk_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/content/core"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestValidation_MessageSpecificSizeLimits(t *testing.T) {
	// 1. Control Request > 512 bytes
	t.Run("Oversized Control Request", func(t *testing.T) {
		var buf bytes.Buffer
		frameSize := uint32(513 + 3) // > 512
		binary.Write(&buf, binary.LittleEndian, frameSize)
		binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion)
		buf.WriteByte(byte(chunk.MsgRequestManifest))

		_, err := chunk.DecodeFrame(&buf)
		if !errors.Is(err, chunk.ErrPayloadTooLarge) {
			t.Fatalf("expected ErrPayloadTooLarge for control request > 512 bytes, got %v", err)
		}
	})

	// 2. Acknowledgment > 33 bytes
	t.Run("Oversized ACK", func(t *testing.T) {
		var buf bytes.Buffer
		frameSize := uint32(34 + 3) // > 33
		binary.Write(&buf, binary.LittleEndian, frameSize)
		binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion)
		buf.WriteByte(byte(chunk.MsgAck))

		_, err := chunk.DecodeFrame(&buf)
		if !errors.Is(err, chunk.ErrPayloadTooLarge) {
			t.Fatalf("expected ErrPayloadTooLarge for ACK > 33 bytes, got %v", err)
		}
	})

	// 3. Error Message > 640 bytes
	t.Run("Oversized Error Message", func(t *testing.T) {
		var buf bytes.Buffer
		frameSize := uint32(641 + 3) // > 640
		binary.Write(&buf, binary.LittleEndian, frameSize)
		binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion)
		buf.WriteByte(byte(chunk.MsgError))

		_, err := chunk.DecodeFrame(&buf)
		if !errors.Is(err, chunk.ErrPayloadTooLarge) {
			t.Fatalf("expected ErrPayloadTooLarge for Error message > 640 bytes, got %v", err)
		}
	})
}

func TestValidation_UnsupportedProtocolVersionInFrame(t *testing.T) {
	var buf bytes.Buffer
	frameSize := uint32(32 + 3)
	binary.Write(&buf, binary.LittleEndian, frameSize)
	binary.Write(&buf, binary.LittleEndian, uint16(99)) // unsupported version
	buf.WriteByte(byte(chunk.MsgRequestManifest))

	_, err := chunk.DecodeFrame(&buf)
	if !errors.Is(err, chunk.ErrInvalidProtocolVersion) {
		t.Fatalf("expected ErrInvalidProtocolVersion, got %v", err)
	}
}

func TestStreamHandler_RejectsInvalidVersionAndOversized(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	tp2 := transport.NewTransport(h2)

	// Test 1: Send request with unsupported version
	t.Run("Server Rejects Incompatible Version", func(t *testing.T) {
		s, err := tp2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
		if err != nil {
			t.Fatalf("OpenStream failed: %v", err)
		}
		defer s.Close()

		invalidMsg := &chunk.Message{
			Version: 99,
			Type:    chunk.MsgRequestManifest,
			Payload: make([]byte, 32),
		}
		if err := chunk.WriteMessage(s, invalidMsg); err != nil {
			t.Fatalf("WriteMessage failed: %v", err)
		}

		respFrame, err := chunk.DecodeFrame(s)
		if err != nil {
			t.Fatalf("Expected error response frame from server, got error reading frame: %v", err)
		}

		if respFrame.MessageType != chunk.MsgError {
			t.Fatalf("Expected MsgError, got %d", respFrame.MessageType)
		}
		code, _, _ := chunk.ParseError(respFrame.Payload)
		if code != chunk.ErrUnsupportedMessage {
			t.Fatalf("Expected ErrUnsupportedMessage code (%d), got %d", chunk.ErrUnsupportedMessage, code)
		}
	})

	// Test 2: Send oversized control request (> 512 bytes)
	t.Run("Server Rejects Oversized Control Request", func(t *testing.T) {
		s, err := tp2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
		if err != nil {
			t.Fatalf("OpenStream failed: %v", err)
		}
		defer s.Close()

		var buf bytes.Buffer
		frameSize := uint32(600 + 3) // 600 bytes payload (> 512)
		binary.Write(&buf, binary.LittleEndian, frameSize)
		binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion)
		buf.WriteByte(byte(chunk.MsgRequestManifest))
		buf.Write(make([]byte, 600))

		if _, err := s.Write(buf.Bytes()); err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		respFrame, err := chunk.DecodeFrame(s)
		if err != nil {
			t.Fatalf("Expected error response frame from server, got error reading frame: %v", err)
		}

		if respFrame.MessageType != chunk.MsgError {
			t.Fatalf("Expected MsgError, got %d", respFrame.MessageType)
		}
		code, _, _ := chunk.ParseError(respFrame.Payload)
		if code != chunk.ErrBadRequest {
			t.Fatalf("Expected ErrBadRequest code (%d), got %d", chunk.ErrBadRequest, code)
		}
	})

	// Test 3: Send malformed payload (invalid ContentID length)
	t.Run("Server Rejects Malformed Payload", func(t *testing.T) {
		s, err := tp2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
		if err != nil {
			t.Fatalf("OpenStream failed: %v", err)
		}
		defer s.Close()

		malformedMsg := &chunk.Message{
			Version: chunk.CurrentMessageVersion,
			Type:    chunk.MsgRequestManifest,
			Payload: make([]byte, 10), // Expected 32
		}
		if err := chunk.WriteMessage(s, malformedMsg); err != nil {
			t.Fatalf("WriteMessage failed: %v", err)
		}

		respFrame, err := chunk.DecodeFrame(s)
		if err != nil {
			t.Fatalf("Expected error response frame from server, got error reading frame: %v", err)
		}

		if respFrame.MessageType != chunk.MsgError {
			t.Fatalf("Expected MsgError, got %d", respFrame.MessageType)
		}
		code, _, _ := chunk.ParseError(respFrame.Payload)
		if code != chunk.ErrBadRequest {
			t.Fatalf("Expected ErrBadRequest code (%d), got %d", chunk.ErrBadRequest, code)
		}
	})
}

func TestClient_RejectsMalformedServerResponse(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng2 := createTestEngine(t)

	// Set up server on h1 that sends malformed manifest payload
	h1.SetStreamHandler("/cipher/chunk/1.0.0", func(s network.Stream) {
		defer s.Close()
		frame, err := chunk.DecodeFrame(s)
		if err != nil {
			return
		}
		if frame.MessageType == chunk.MsgRequestManifest {
			// Send manifest with payload shorter than 32 bytes (malformed)
			badManifest := &chunk.Message{
				Version: chunk.CurrentMessageVersion,
				Type:    chunk.MsgManifest,
				Payload: []byte{0x01, 0x02}, // invalid manifest payload
			}
			chunk.WriteMessage(s, badManifest)
		}
	})

	ctx := context.Background()
	c, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer c.Close()

	var cid core.ContentID
	cid[0] = 0x01
	_, err = c.Resolve(ctx, cid)
	if err == nil {
		t.Fatalf("Expected client.Resolve to fail on malformed manifest payload, got nil error")
	}
}
