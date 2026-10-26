package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

func TestSendSinglePassAndTrailingChecksum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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

	testProto := protocol.ID("/cipher/testtransfer/1.0.0")

	tmpDir := t.TempDir()
	srcFilePath := filepath.Join(tmpDir, "sample_stream.bin")
	data := bytes.Repeat([]byte("CIPHER-WIRE-STREAMING-CHECKSUM-"), 1000)
	if err := os.WriteFile(srcFilePath, data, 0644); err != nil {
		t.Fatalf("failed to write src file: %v", err)
	}

	errChan := make(chan error, 1)

	h2.SetStreamHandler(testProto, func(s network.Stream) {
		oldWd, _ := os.Getwd()
		if err := os.Chdir(tmpDir); err != nil {
			errChan <- err
			return
		}
		defer os.Chdir(oldWd)

		if err := Receive(s); err != nil {
			errChan <- err
			return
		}
		errChan <- nil
	})

	s, err := h1.NewStream(ctx, h2.ID(), testProto)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	if err := Send(s, srcFilePath); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("Receive failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Receive")
	}

	receivedPath := filepath.Join(tmpDir, "downloads", "sample_stream.bin")
	receivedData, err := os.ReadFile(receivedPath)
	if err != nil {
		t.Fatalf("failed to read received file: %v", err)
	}

	if !bytes.Equal(receivedData, data) {
		t.Fatalf("data mismatch")
	}
}

func TestReceiveChecksumMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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

	testProto := protocol.ID("/cipher/corrupttest/1.0.0")

	tmpDir := t.TempDir()
	errChan := make(chan error, 1)

	h2.SetStreamHandler(testProto, func(s network.Stream) {
		oldWd, _ := os.Getwd()
		if err := os.Chdir(tmpDir); err != nil {
			errChan <- err
			return
		}
		defer os.Chdir(oldWd)

		errChan <- Receive(s)
	})

	s, err := h1.NewStream(ctx, h2.ID(), testProto)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	payload := []byte("Corrupted Payload Stream")
	hdr := &Header{
		Version:  ProtocolVersion2,
		Type:     MsgTypeFileTransfer,
		Filename: "corrupt.dat",
		FileSize: uint64(len(payload)),
		Checksum: [32]byte{},
	}

	if err := hdr.WriteTo(s); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	// Write payload
	s.Write(payload)

	// Write invalid trailing checksum
	badChecksum := [32]byte{0xff, 0xff, 0xff}
	s.Write(badChecksum[:])
	s.Close()

	select {
	case err := <-errChan:
		if err == nil {
			t.Fatalf("expected checksum mismatch error, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Receive")
	}
}

func TestReceiveLegacyProtocolVersion1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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

	testProto := protocol.ID("/cipher/legacytest/1.0.0")

	tmpDir := t.TempDir()
	payload := []byte("Legacy Protocol Version 1 Transfer")
	hasher := sha256.New()
	hasher.Write(payload)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	errChan := make(chan error, 1)

	h2.SetStreamHandler(testProto, func(s network.Stream) {
		oldWd, _ := os.Getwd()
		if err := os.Chdir(tmpDir); err != nil {
			errChan <- err
			return
		}
		defer os.Chdir(oldWd)

		errChan <- Receive(s)
	})

	s, err := h1.NewStream(ctx, h2.ID(), testProto)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	hdr := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "legacy.dat",
		FileSize: uint64(len(payload)),
		Checksum: checksum,
	}

	if err := hdr.WriteTo(s); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	s.Write(payload)
	s.Close()

	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("Receive failed for legacy header: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Receive")
	}

	receivedPath := filepath.Join(tmpDir, "downloads", "legacy.dat")
	receivedData, err := os.ReadFile(receivedPath)
	if err != nil {
		t.Fatalf("failed to read legacy file: %v", err)
	}

	if !bytes.Equal(receivedData, payload) {
		t.Fatalf("legacy payload mismatch")
	}
}
