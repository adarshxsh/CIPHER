package chunk_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
	"golang.org/x/time/rate"
)

func TestSanitizeErrorMessage(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "clean string",
			input:    "simple error message",
			expected: "simple error message",
		},
		{
			name:     "newlines and carriage returns",
			input:    "Error line 1\nError line 2\rError line 3\r\n",
			expected: "Error line 1\\nError line 2\\rError line 3\\r\\n",
		},
		{
			name:     "tabs and NUL bytes",
			input:    "Tab\tseparated\x00value",
			expected: "Tab\\tseparated\\x00value",
		},
		{
			name:     "ANSI escape sequences",
			input:    "\x1b[31mRed Alert\x1b[0m",
			expected: "\\x1b[31mRed Alert\\x1b[0m",
		},
		{
			name:     "log injection attempt",
			input:    "Failed\n[INFO] User admin logged in successfully\r\n[CRITICAL] System compromised",
			expected: "Failed\\n[INFO] User admin logged in successfully\\r\\n[CRITICAL] System compromised",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := chunk.SanitizeErrorMessage(tc.input)
			if got != tc.expected {
				t.Errorf("SanitizeErrorMessage(%q) = %q, expected %q", tc.input, got, tc.expected)
			}
			// Verify output contains no raw control characters
			if strings.ContainsAny(got, "\n\r\t\x1b\x00") {
				t.Errorf("SanitizeErrorMessage(%q) contains raw control characters: %q", tc.input, got)
			}
		})
	}
}

func TestParseError_ValidationAndSanitization(t *testing.T) {
	// 1. Valid error message
	validPayload := append([]byte{byte(chunk.ErrBadRequest)}, []byte("invalid chunk parameter")...)
	code, msgStr, err := chunk.ParseError(validPayload)
	if err != nil {
		t.Fatalf("expected valid ParseError, got %v", err)
	}
	if code != chunk.ErrBadRequest {
		t.Errorf("expected ErrBadRequest, got %v", code)
	}
	if msgStr != "invalid chunk parameter" {
		t.Errorf("expected 'invalid chunk parameter', got %q", msgStr)
	}

	// 2. Empty payload -> should fail structural validation
	_, _, err = chunk.ParseError([]byte{})
	if err == nil {
		t.Fatal("expected error on empty payload, got nil")
	}

	// 3. Unknown error code -> should fail structural validation
	_, _, err = chunk.ParseError([]byte{0xFF, 'a'})
	if err == nil {
		t.Fatal("expected error on unknown error code, got nil")
	}

	// 4. Payload exceeding MaxErrorMessageSize (512 bytes) -> should fail length check
	oversizedMsg := strings.Repeat("A", chunk.MaxErrorMessageSize+1)
	oversizedPayload := append([]byte{byte(chunk.ErrInternal)}, []byte(oversizedMsg)...)
	_, _, err = chunk.ParseError(oversizedPayload)
	if !errors.Is(err, chunk.ErrInvalidErrorMessage) {
		t.Fatalf("expected ErrInvalidErrorMessage for payload exceeding 512 bytes, got %v", err)
	}

	// 5. Payload at exact MaxErrorMessageSize (512 bytes) -> should succeed
	maxMsg := strings.Repeat("B", chunk.MaxErrorMessageSize)
	maxPayload := append([]byte{byte(chunk.ErrInternal)}, []byte(maxMsg)...)
	code, msgStr, err = chunk.ParseError(maxPayload)
	if err != nil {
		t.Fatalf("expected success for 512-byte payload, got %v", err)
	}
	if len(msgStr) != chunk.MaxErrorMessageSize {
		t.Errorf("expected msg length %d, got %d", chunk.MaxErrorMessageSize, len(msgStr))
	}

	// 6. Payload with control characters -> should sanitize
	injectionPayload := append([]byte{byte(chunk.ErrPermissionDenied)}, []byte("Denied\nLOG INJECTION\r\x1b[32mGREEN")...)
	_, sanitizedMsg, err := chunk.ParseError(injectionPayload)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if strings.ContainsAny(sanitizedMsg, "\n\r\x1b") {
		t.Errorf("parsed error message contains unescaped control characters: %q", sanitizedMsg)
	}
	expectedSanitized := "Denied\\nLOG INJECTION\\r\\x1b[32mGREEN"
	if sanitizedMsg != expectedSanitized {
		t.Errorf("expected %q, got %q", expectedSanitized, sanitizedMsg)
	}
}

func TestStreamHandler_PayloadValidationAndRateLimiting(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	handler := chunk.NewStreamHandler(h1, eng1)
	// Set log rate limit: 1 per sec, burst 2
	handler.SetErrorLogRate(rate.Limit(1.0), 2)

	ctx := context.Background()

	// Ingest test chunk in eng1
	chunkData := []byte("hello world rate limit test")
	m, err := eng1.Ingest(ctx, bytes.NewReader(chunkData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	chunkID := m.ChunkIDs[0]

	// Redirect log output to capture logs
	var logBuf bytes.Buffer
	origFlags := log.Flags()
	origOutput := log.Writer()
	log.SetOutput(&logBuf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(origOutput)
		log.SetFlags(origFlags)
	}()

	// Open stream from h2 to h1
	tr2 := transport.NewTransport(h2)
	stream, err := tr2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer stream.Close()

	// Send 10 REQUEST_CHUNK requests, and respond to each chunk with a MsgError carrying injection payload
	for i := 0; i < 10; i++ {
		req := chunk.BuildRequestChunk(chunkID)
		if err := chunk.WriteMessage(stream, req); err != nil {
			t.Fatalf("WriteMessage failed: %v", err)
		}

		// Read CHUNK response
		resp, err := chunk.ReadMessage(stream)
		if err != nil {
			t.Fatalf("ReadMessage failed: %v", err)
		}
		if resp.Type != chunk.MsgChunk {
			t.Fatalf("Expected CHUNK, got type %d", resp.Type)
		}

		// Send MsgError payload with control character injection
		errMsg := chunk.BuildError(chunk.ErrIntegrityMismatch, fmt.Sprintf("Error #%d\nINJECTED LINE\r\x1b[31mRED", i))
		if err := chunk.WriteMessage(stream, errMsg); err != nil {
			t.Fatalf("WriteMessage error failed: %v", err)
		}
	}

	// Give handler loop time to log
	time.Sleep(50 * time.Millisecond)

	logOutput := logBuf.String()

	// 1. Verify zero raw linebreaks or ANSI sequences appear in the log output from remote payload
	if strings.Contains(logOutput, "INJECTED LINE\n") || strings.Contains(logOutput, "\x1b[31mRED") {
		t.Fatalf("Unescaped control characters detected in log output:\n%s", logOutput)
	}

	// 2. Verify rate limiter throttled the error messages
	errorLogCount := strings.Count(logOutput, "Client reported error on chunk")
	if errorLogCount > 2 {
		t.Fatalf("Expected at most 2 error logs due to rate limiting (burst=2), got %d logs:\n%s", errorLogCount, logOutput)
	}
	if errorLogCount == 0 {
		t.Fatalf("Expected at least 1 error log, got 0:\n%s", logOutput)
	}
}

func TestStreamHandler_OversizedErrorAndPayloadRejection(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	ctx := context.Background()

	chunkData := []byte("payload validation test data")
	m, err := eng1.Ingest(ctx, bytes.NewReader(chunkData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	chunkID := m.ChunkIDs[0]

	tr2 := transport.NewTransport(h2)

	// Test 1: Send invalid REQUEST_CHUNK with payload size != 32
	t.Run("invalid request chunk payload size", func(t *testing.T) {
		stream, err := tr2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
		if err != nil {
			t.Fatalf("OpenStream failed: %v", err)
		}
		defer stream.Close()

		invalidReq := &chunk.Message{
			Version: chunk.CurrentMessageVersion,
			Type:    chunk.MsgRequestChunk,
			Payload: []byte("too short"),
		}
		if err := chunk.WriteMessage(stream, invalidReq); err != nil {
			t.Fatalf("WriteMessage failed: %v", err)
		}

		resp, err := chunk.ReadMessage(stream)
		if err != nil {
			t.Fatalf("ReadMessage failed: %v", err)
		}
		if resp.Type != chunk.MsgError {
			t.Fatalf("Expected MsgError response for invalid payload, got %d", resp.Type)
		}
		code, _, err := chunk.ParseError(resp.Payload)
		if err != nil {
			t.Fatalf("ParseError failed: %v", err)
		}
		if code != chunk.ErrBadRequest {
			t.Fatalf("Expected ErrBadRequest (7), got %d", code)
		}
	})

	// Test 2: Send oversized error ACK (513 bytes error string) from client
	t.Run("oversized client error payload rejection", func(t *testing.T) {
		var logBuf bytes.Buffer
		origFlags := log.Flags()
		origOutput := log.Writer()
		log.SetOutput(&logBuf)
		log.SetFlags(0)
		defer func() {
			log.SetOutput(origOutput)
			log.SetFlags(origFlags)
		}()

		stream, err := tr2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
		if err != nil {
			t.Fatalf("OpenStream failed: %v", err)
		}
		defer stream.Close()

		req := chunk.BuildRequestChunk(chunkID)
		if err := chunk.WriteMessage(stream, req); err != nil {
			t.Fatalf("WriteMessage failed: %v", err)
		}

		resp, err := chunk.ReadMessage(stream)
		if err != nil {
			t.Fatalf("ReadMessage failed: %v", err)
		}
		if resp.Type != chunk.MsgChunk {
			t.Fatalf("Expected CHUNK, got %d", resp.Type)
		}

		// Send oversized MsgError payload (> 512 bytes error message)
		oversizedMsg := strings.Repeat("X", 513)
		oversizedErrPayload := append([]byte{byte(chunk.ErrInternal)}, []byte(oversizedMsg)...)
		oversizedErrMsg := &chunk.Message{
			Version: chunk.CurrentMessageVersion,
			Type:    chunk.MsgError,
			Payload: oversizedErrPayload,
		}

		if err := chunk.WriteMessage(stream, oversizedErrMsg); err != nil {
			t.Fatalf("WriteMessage failed: %v", err)
		}

		time.Sleep(30 * time.Millisecond)

		logOutput := logBuf.String()
		if strings.Contains(logOutput, "Client reported error") {
			t.Fatalf("Server logged error for oversized MsgError payload when it should have rejected it:\n%s", logOutput)
		}
		if !strings.Contains(logOutput, "Invalid payload") {
			t.Fatalf("Expected server to log 'Invalid payload' for oversized error, got:\n%s", logOutput)
		}
	})
}
