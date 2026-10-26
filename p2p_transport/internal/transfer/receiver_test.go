package transfer_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/transfer"
)

const testProtocolID = "/cipher/transfer/test/1.0.0"

func setupMockNetwork(t testing.TB) (host.Host, host.Host) {
	mocknet := mocknet.New()

	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	h2, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}
	if err := mocknet.ConnectAllButSelf(); err != nil {
		t.Fatal(err)
	}
	return h1, h2
}

func sendCustomHeaderAndData(t *testing.T, hSender, hReceiver host.Host, header *transfer.Header, data []byte) error {
	ctx := context.Background()
	var recvErr error
	done := make(chan struct{})

	hReceiver.SetStreamHandler(testProtocolID, func(s network.Stream) {
		recvErr = transfer.Receive(s)
		close(done)
	})

	s, err := hSender.NewStream(ctx, hReceiver.ID(), testProtocolID)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	if err := header.WriteTo(s); err != nil {
		s.Close()
		return err
	}

	if len(data) > 0 {
		if _, err := s.Write(data); err != nil {
			s.Close()
			return err
		}
	}

	s.CloseWrite()
	<-done
	return recvErr
}

func TestReceive_BenignFilename(t *testing.T) {
	tmpDir := t.TempDir()
	t.Chdir(tmpDir)

	h1, h2 := setupMockNetwork(t)
	defer h1.Close()
	defer h2.Close()

	content := []byte("hello world")
	hasher := sha256.New()
	hasher.Write(content)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	header := &transfer.Header{
		Version:  transfer.ProtocolVersion1,
		Type:     transfer.MsgTypeFileTransfer,
		Filename: "hello.txt",
		FileSize: uint64(len(content)),
		Checksum: checksum,
	}

	err := sendCustomHeaderAndData(t, h1, h2, header, content)
	if err != nil {
		t.Fatalf("Expected Receive to succeed, got: %v", err)
	}

	expectedPath := filepath.Join("downloads", "hello.txt")
	got, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Failed to read expected output file %s: %v", expectedPath, err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("Content mismatch: expected %q, got %q", content, got)
	}
}

func TestReceive_RelativePathTraversal(t *testing.T) {
	tmpDir := t.TempDir()
	t.Chdir(tmpDir)

	h1, h2 := setupMockNetwork(t)
	defer h1.Close()
	defer h2.Close()

	content := []byte("traversal test content")
	hasher := sha256.New()
	hasher.Write(content)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	// Attempt relative traversal to write outside downloads dir
	header := &transfer.Header{
		Version:  transfer.ProtocolVersion1,
		Type:     transfer.MsgTypeFileTransfer,
		Filename: "../../secret.txt",
		FileSize: uint64(len(content)),
		Checksum: checksum,
	}

	err := sendCustomHeaderAndData(t, h1, h2, header, content)
	if err != nil {
		t.Fatalf("Expected Receive to sanitize relative traversal and succeed, got: %v", err)
	}

	// Verify it was written inside downloads/secret.txt
	expectedPath := filepath.Join("downloads", "secret.txt")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Fatalf("Expected sanitized file at %s, but was not found", expectedPath)
	}

	// Verify file was NOT created in parent directory
	escapedPath := filepath.Join(tmpDir, "secret.txt")
	if _, err := os.Stat(escapedPath); err == nil {
		t.Fatalf("File escaped downloads directory to %s!", escapedPath)
	}
}

func TestReceive_AbsolutePathTraversal(t *testing.T) {
	tmpDir := t.TempDir()
	t.Chdir(tmpDir)

	h1, h2 := setupMockNetwork(t)
	defer h1.Close()
	defer h2.Close()

	content := []byte("absolute traversal content")
	hasher := sha256.New()
	hasher.Write(content)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	header := &transfer.Header{
		Version:  transfer.ProtocolVersion1,
		Type:     transfer.MsgTypeFileTransfer,
		Filename: "/etc/passwd",
		FileSize: uint64(len(content)),
		Checksum: checksum,
	}

	err := sendCustomHeaderAndData(t, h1, h2, header, content)
	if err != nil {
		t.Fatalf("Expected Receive to sanitize absolute path and succeed, got: %v", err)
	}

	expectedPath := filepath.Join("downloads", "passwd")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Fatalf("Expected sanitized file at %s, but was not found", expectedPath)
	}
}

func TestReceive_InvalidFilenamesRejected(t *testing.T) {
	invalidFilenames := []string{
		"..",
		"../..",
		".",
		"",
		"/",
		"\\",
	}

	for _, invalidFilename := range invalidFilenames {
		t.Run("Filename_"+invalidFilename, func(t *testing.T) {
			tmpDir := t.TempDir()
			t.Chdir(tmpDir)

			h1, h2 := setupMockNetwork(t)
			defer h1.Close()
			defer h2.Close()

			content := []byte("bad filename content")
			hasher := sha256.New()
			hasher.Write(content)
			var checksum [32]byte
			copy(checksum[:], hasher.Sum(nil))

			header := &transfer.Header{
				Version:  transfer.ProtocolVersion1,
				Type:     transfer.MsgTypeFileTransfer,
				Filename: invalidFilename,
				FileSize: uint64(len(content)),
				Checksum: checksum,
			}

			err := sendCustomHeaderAndData(t, h1, h2, header, content)
			if err == nil {
				t.Fatalf("Expected error for invalid filename %q, got nil", invalidFilename)
			}
		})
	}
}

func TestReceive_SendReceiveIntegration(t *testing.T) {
	tmpDir := t.TempDir()
	t.Chdir(tmpDir)

	h1, h2 := setupMockNetwork(t)
	defer h1.Close()
	defer h2.Close()

	// Create a local source file
	sourceFile := filepath.Join(tmpDir, "source.txt")
	content := []byte("testing normal Send and Receive workflow")
	if err := os.WriteFile(sourceFile, content, 0644); err != nil {
		t.Fatalf("Failed to write source file: %v", err)
	}

	ctx := context.Background()
	done := make(chan error, 1)

	h2.SetStreamHandler(testProtocolID, func(s network.Stream) {
		done <- transfer.Receive(s)
	})

	s, err := h1.NewStream(ctx, h2.ID(), testProtocolID)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	if err := transfer.Send(s, sourceFile); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	if err := <-done; err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	receivedFile := filepath.Join("downloads", "source.txt")
	got, err := os.ReadFile(receivedFile)
	if err != nil {
		t.Fatalf("Failed to read received file: %v", err)
	}

	if !bytes.Equal(got, content) {
		t.Fatalf("Received content mismatch: expected %q, got %q", content, got)
	}
}
