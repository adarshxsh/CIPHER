package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

type trackingStream struct {
	network.Stream
	resetCalled bool
	closeCalled bool
}

func (ts *trackingStream) Reset() error {
	ts.resetCalled = true
	return ts.Stream.Reset()
}

func (ts *trackingStream) Close() error {
	ts.closeCalled = true
	return ts.Stream.Close()
}

func setupTestNetwork(t *testing.T) (host.Host, host.Host) {
	t.Helper()
	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate peer 1: %v", err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate peer 2: %v", err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatalf("failed to link peers: %v", err)
	}
	return h1, h2
}

func countOpenFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Logf("unable to read /proc/self/fd: %v", err)
		return -1
	}
	return len(entries)
}

func TestReceive_Success(t *testing.T) {
	h1, h2 := setupTestNetwork(t)

	testDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get wd: %v", err)
	}
	if err := os.Chdir(testDir); err != nil {
		t.Fatalf("failed to chdir to testDir: %v", err)
	}
	defer func() { _ = os.Chdir(origDir) }()

	const protoID libp2p_protocol.ID = "/test/transfer/1.0"
	payload := []byte("hello world file content")
	checksum := sha256.Sum256(payload)

	done := make(chan struct{})
	var receiveErr error
	var ts2 *trackingStream

	h2.SetStreamHandler(protoID, func(s network.Stream) {
		ts2 = &trackingStream{Stream: s}
		receiveErr = Receive(ts2)
		close(done)
	})

	s1, err := h1.NewStream(context.Background(), h2.ID(), protoID)
	if err != nil {
		t.Fatalf("failed to create new stream: %v", err)
	}

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "sample.txt",
		FileSize: uint64(len(payload)),
		Checksum: checksum,
	}
	if err := header.WriteTo(s1); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	if _, err := s1.Write(payload); err != nil {
		t.Fatalf("failed to write payload: %v", err)
	}
	s1.Close()

	<-done

	if receiveErr != nil {
		t.Fatalf("Receive failed unexpectedly: %v", receiveErr)
	}

	outPath := filepath.Join("downloads", "sample.txt")
	tmpPath := outPath + ".tmp"

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("payload mismatch: got %q, want %q", data, payload)
	}

	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("staging file %s still exists after successful transfer", tmpPath)
	}

	if ts2 != nil && ts2.resetCalled {
		t.Errorf("Reset() was called on stream for successful transfer")
	}
}

func TestReceive_InterruptedTransfer(t *testing.T) {
	h1, h2 := setupTestNetwork(t)

	testDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get wd: %v", err)
	}
	if err := os.Chdir(testDir); err != nil {
		t.Fatalf("failed to chdir to testDir: %v", err)
	}
	defer func() { _ = os.Chdir(origDir) }()

	const protoID libp2p_protocol.ID = "/test/transfer/1.0"
	done := make(chan struct{})

	var receiveErr error
	var ts2 *trackingStream

	h2.SetStreamHandler(protoID, func(s network.Stream) {
		ts2 = &trackingStream{Stream: s}
		receiveErr = Receive(ts2)
		close(done)
	})

	s1, err := h1.NewStream(context.Background(), h2.ID(), protoID)
	if err != nil {
		t.Fatalf("failed to create new stream: %v", err)
	}

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "truncated.dat",
		FileSize: 1024 * 1024, // Expect 1MB
		Checksum: [32]byte{1, 2, 3},
	}
	if err := header.WriteTo(s1); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	// Write only 100 bytes and reset stream to simulate network interrupt
	_, _ = s1.Write([]byte("short chunk"))
	_ = s1.Reset()

	<-done

	if receiveErr == nil {
		t.Fatalf("expected error from interrupted transfer, got nil")
	}

	tmpPath := filepath.Join("downloads", "truncated.dat.tmp")
	outPath := filepath.Join("downloads", "truncated.dat")

	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("staging file %s was not cleaned up after error", tmpPath)
	}
	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Fatalf("output file %s exists after failed transfer", outPath)
	}

	if ts2 != nil && !ts2.resetCalled {
		t.Errorf("expected Reset() to be called on network stream on failure")
	}
}

func TestReceive_ChecksumMismatch(t *testing.T) {
	h1, h2 := setupTestNetwork(t)

	testDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get wd: %v", err)
	}
	if err := os.Chdir(testDir); err != nil {
		t.Fatalf("failed to chdir to testDir: %v", err)
	}
	defer func() { _ = os.Chdir(origDir) }()

	const protoID libp2p_protocol.ID = "/test/transfer/1.0"
	done := make(chan struct{})

	var receiveErr error
	var ts2 *trackingStream

	h2.SetStreamHandler(protoID, func(s network.Stream) {
		ts2 = &trackingStream{Stream: s}
		receiveErr = Receive(ts2)
		close(done)
	})

	s1, err := h1.NewStream(context.Background(), h2.ID(), protoID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	payload := []byte("content with wrong checksum")
	wrongChecksum := [32]byte{0xde, 0xad, 0xbe, 0xef}

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "corrupt.txt",
		FileSize: uint64(len(payload)),
		Checksum: wrongChecksum,
	}
	if err := header.WriteTo(s1); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	_, _ = s1.Write(payload)
	s1.Close()

	<-done

	if receiveErr == nil {
		t.Fatalf("expected checksum mismatch error, got nil")
	}

	tmpPath := filepath.Join("downloads", "corrupt.txt.tmp")
	outPath := filepath.Join("downloads", "corrupt.txt")

	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("staging file %s was not cleaned up on checksum mismatch", tmpPath)
	}
	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Fatalf("output file %s was created despite checksum mismatch", outPath)
	}

	if ts2 != nil && !ts2.resetCalled {
		t.Errorf("expected Reset() to be called on checksum mismatch")
	}
}

func TestReceive_PathTraversal(t *testing.T) {
	h1, h2 := setupTestNetwork(t)

	testDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get wd: %v", err)
	}
	if err := os.Chdir(testDir); err != nil {
		t.Fatalf("failed to chdir to testDir: %v", err)
	}
	defer func() { _ = os.Chdir(origDir) }()

	const protoID libp2p_protocol.ID = "/test/transfer/1.0"
	done := make(chan struct{})

	var receiveErr error

	h2.SetStreamHandler(protoID, func(s network.Stream) {
		receiveErr = Receive(s)
		close(done)
	})

	s1, err := h1.NewStream(context.Background(), h2.ID(), protoID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "../../../etc/passwd",
		FileSize: 10,
		Checksum: [32]byte{},
	}
	_ = header.WriteTo(s1)
	s1.Close()

	<-done

	if receiveErr == nil {
		t.Fatalf("expected error on path traversal filename, got nil")
	}
}

func TestReceive_ZeroLeakedFDsOnRepeatedFailures(t *testing.T) {
	h1, h2 := setupTestNetwork(t)

	testDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get wd: %v", err)
	}
	if err := os.Chdir(testDir); err != nil {
		t.Fatalf("failed to chdir to testDir: %v", err)
	}
	defer func() { _ = os.Chdir(origDir) }()

	const protoID libp2p_protocol.ID = "/test/transfer/1.0"

	h2.SetStreamHandler(protoID, func(s network.Stream) {
		_ = Receive(s)
	})

	startFDs := countOpenFDs(t)

	for i := 0; i < 10; i++ {
		s1, err := h1.NewStream(context.Background(), h2.ID(), protoID)
		if err != nil {
			t.Fatalf("failed to create stream at iteration %d: %v", i, err)
		}
		header := Header{
			Version:  ProtocolVersion1,
			Type:     MsgTypeFileTransfer,
			Filename: fmt.Sprintf("failed_%d.tmp", i),
			FileSize: 1000,
			Checksum: [32]byte{1, 2, 3},
		}
		_ = header.WriteTo(s1)
		_ = s1.Reset()
	}

	endFDs := countOpenFDs(t)
	if startFDs != -1 && endFDs != -1 {
		if endFDs > startFDs+2 { // allow minor temporary runtime variance
			t.Errorf("FD leak detected: started with %d open FDs, ended with %d open FDs", startFDs, endFDs)
		}
	}
}

type errorStream struct {
	network.Stream
	closeErr error
}

func (es *errorStream) Close() error {
	return es.closeErr
}

func TestReceive_NamedErrorPreservation(t *testing.T) {
	h1, h2 := setupTestNetwork(t)

	testDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get wd: %v", err)
	}
	if err := os.Chdir(testDir); err != nil {
		t.Fatalf("failed to chdir to testDir: %v", err)
	}
	defer func() { _ = os.Chdir(origDir) }()

	const protoID libp2p_protocol.ID = "/test/transfer/1.0"
	done := make(chan struct{})

	var receiveErr error

	h2.SetStreamHandler(protoID, func(s network.Stream) {
		es := &errorStream{
			Stream:   s,
			closeErr: fmt.Errorf("secondary stream close error"),
		}
		receiveErr = Receive(es)
		close(done)
	})

	s1, err := h1.NewStream(context.Background(), h2.ID(), protoID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	header := Header{
		Version:  99, // Invalid version -> primary error
		Type:     MsgTypeFileTransfer,
		Filename: "test.txt",
		FileSize: 10,
		Checksum: [32]byte{},
	}
	_ = header.WriteTo(s1)
	s1.Close()

	<-done

	if receiveErr == nil {
		t.Fatalf("expected primary error, got nil")
	}
	if !bytes.Contains([]byte(receiveErr.Error()), []byte("unsupported protocol version")) {
		t.Errorf("expected primary error 'unsupported protocol version' to be preserved, got: %v", receiveErr)
	}
}
