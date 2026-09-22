package transfer_test

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"cipher/internal/protocol"
	"cipher/internal/transfer"

	"github.com/libp2p/go-libp2p/core/network"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

func TestSendAndReceive_InlineChecksum(t *testing.T) {
	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}

	// Create test file
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "testdata.bin")
	data := make([]byte, 512*1024) // 512 KB
	for i := range data {
		data[i] = byte(i % 251)
	}
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		t.Fatal(err)
	}

	var receiveErr error
	var wg sync.WaitGroup
	wg.Add(1)

	// Receiver setup
	h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		defer wg.Done()
		receiveErr = transfer.Receive(s)
	})

	// Sender open stream
	s, err := h1.NewStream(t.Context(), h2.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("NewStream failed: %v", err)
	}

	sendErr := transfer.Send(s, filePath)
	if sendErr != nil {
		t.Fatalf("Send failed: %v", sendErr)
	}

	wg.Wait()

	if receiveErr != nil {
		t.Fatalf("Receive failed: %v", receiveErr)
	}

	// Check output file in downloads directory
	downloadPath := filepath.Join("downloads", "testdata.bin")
	defer os.RemoveAll("downloads")

	downloadedData, err := os.ReadFile(downloadPath)
	if err != nil {
		t.Fatalf("Failed to read downloaded file: %v", err)
	}

	if !bytes.Equal(downloadedData, data) {
		t.Fatal("Downloaded data does not match original data")
	}
}

func TestSendAndReceive_LegacyHeaderChecksum(t *testing.T) {
	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}

	data := []byte("Legacy header checksum test data")
	hasher := sha256.New()
	hasher.Write(data)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	var receiveErr error
	var wg sync.WaitGroup
	wg.Add(1)

	h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		defer wg.Done()
		receiveErr = transfer.Receive(s)
	})

	s, err := h1.NewStream(t.Context(), h2.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("NewStream failed: %v", err)
	}

	// Manually write header with pre-populated checksum
	header := &transfer.Header{
		Version:  transfer.ProtocolVersion1,
		Type:     transfer.MsgTypeFileTransfer,
		Filename: "legacy.txt",
		FileSize: uint64(len(data)),
		Checksum: checksum,
	}

	go func() {
		defer s.Close()
		if err := header.WriteTo(s); err != nil {
			t.Errorf("WriteTo failed: %v", err)
			return
		}
		if _, err := s.Write(data); err != nil {
			t.Errorf("Write data failed: %v", err)
		}
	}()

	wg.Wait()

	if receiveErr != nil {
		t.Fatalf("Receive failed with legacy header: %v", receiveErr)
	}

	downloadPath := filepath.Join("downloads", "legacy.txt")
	defer os.RemoveAll("downloads")

	downloadedData, err := os.ReadFile(downloadPath)
	if err != nil {
		t.Fatalf("Failed to read downloaded file: %v", err)
	}

	if !bytes.Equal(downloadedData, data) {
		t.Fatal("Downloaded data does not match legacy data")
	}
}

func TestSendAndReceive_ChecksumMismatch(t *testing.T) {
	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}

	data := []byte("Some content to be corrupted")

	var receiveErr error
	var wg sync.WaitGroup
	wg.Add(1)

	h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		defer wg.Done()
		receiveErr = transfer.Receive(s)
	})

	s, err := h1.NewStream(t.Context(), h2.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("NewStream failed: %v", err)
	}

	header := &transfer.Header{
		Version:  transfer.ProtocolVersion1,
		Type:     transfer.MsgTypeFileTransfer,
		Filename: "corrupt.txt",
		FileSize: uint64(len(data)),
		Checksum: [32]byte{}, // Zero header checksum -> trailer expected
	}

	go func() {
		defer s.Close()
		header.WriteTo(s)
		s.Write(data)
		// Write invalid checksum trailer
		badChecksum := [32]byte{1, 2, 3, 4}
		s.Write(badChecksum[:])
	}()

	wg.Wait()
	defer os.RemoveAll("downloads")

	if receiveErr == nil {
		t.Fatal("Expected error due to checksum mismatch, but got nil")
	}
}
