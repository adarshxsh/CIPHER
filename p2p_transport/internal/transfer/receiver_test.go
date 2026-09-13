package transfer_test

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/protocol"
	"cipher/internal/transfer"
	"cipher/internal/transport"
)

func TestTransfer_ReceiveReadDeadlineTimeout(t *testing.T) {
	origDeadline := transfer.DefaultTransferDeadline
	transfer.DefaultTransferDeadline = 100 * time.Millisecond
	defer func() { transfer.DefaultTransferDeadline = origDeadline }()

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

	transport.SetupStreamHandler(h1)

	tp2 := transport.NewTransport(h2)
	ctx := context.Background()

	stream, err := tp2.OpenStream(ctx, h1.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// Send header indicating 100KB file
	header := &transfer.Header{
		Version:  transfer.ProtocolVersion1,
		Type:     transfer.MsgTypeFileTransfer,
		Filename: "testfile.txt",
		FileSize: 100000,
	}
	if err := header.WriteTo(stream); err != nil {
		t.Fatalf("Failed to write header: %v", err)
	}

	// Do NOT send payload data. Server's Receive should hit read deadline timeout and close stream.
	time.Sleep(250 * time.Millisecond)

	buf := make([]byte, 10)
	_, err = stream.Read(buf)
	if err == nil {
		t.Fatal("Expected stream to be closed after transfer receiver deadline timeout")
	}
}
