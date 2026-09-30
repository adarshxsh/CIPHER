package payment

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

// SignPaymentState authorizes a cumulative payment state with the publisher's
// Ed25519 private key.
func SignPaymentState(paymentState PaymentState, publisherPrivateKey ed25519.PrivateKey) (PaymentState, error) {
	if len(publisherPrivateKey) != ed25519.PrivateKeySize {
		return PaymentState{}, errors.New("invalid publisher private key")
	}
	if paymentState.ContractID == "" || paymentState.Publisher == "" || paymentState.Provider == "" {
		return PaymentState{}, errors.New("payment state is incomplete")
	}
	paymentState.PublisherSignature = ed25519.Sign(publisherPrivateKey, paymentStateDigest(paymentState))
	return paymentState, nil
}

func paymentStateDigest(paymentState PaymentState) []byte {
	hash := sha256.New()
	writeString := func(value string) {
		length := make([]byte, 8)
		binary.BigEndian.PutUint64(length, uint64(len(value)))
		hash.Write(length)
		hash.Write([]byte(value))
	}
	writeUint64 := func(value uint64) {
		encoded := make([]byte, 8)
		binary.BigEndian.PutUint64(encoded, value)
		hash.Write(encoded)
	}
	writeString(paymentState.ContractID)
	writeString(paymentState.Publisher)
	writeString(paymentState.Provider)
	writeUint64(paymentState.Sequence)
	writeUint64(paymentState.Period)
	writeUint64(paymentState.CumulativePayment)
	writeString(paymentState.LastChallengeID)
	writeString(string(paymentState.Status))
	return hash.Sum(nil)
}
