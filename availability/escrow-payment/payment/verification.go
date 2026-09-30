package payment

import "crypto/ed25519"

// VerifyPaymentState verifies the publisher authorization over every signed
// payment-state field.
func VerifyPaymentState(paymentState PaymentState, publisherPublicKey ed25519.PublicKey) bool {
	return len(publisherPublicKey) == ed25519.PublicKeySize &&
		len(paymentState.PublisherSignature) == ed25519.SignatureSize &&
		ed25519.Verify(publisherPublicKey, paymentStateDigest(paymentState), paymentState.PublisherSignature)
}
