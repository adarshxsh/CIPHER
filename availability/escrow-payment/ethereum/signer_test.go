package ethereum

import (
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	interfaces "cipher/availability/escrow-payment/interfaces"
	payment "cipher/availability/escrow-payment/payment"
)

func TestSignerProducesRecoverableSolidityCompatibleSignature(t *testing.T) {
	privateKey, err := crypto.ToECDSA(common.FromHex("0x0123456789012345678901234567890123456789012345678901234567890123"))
	if err != nil {
		t.Fatalf("ToECDSA returned error: %v", err)
	}
	publisher := crypto.PubkeyToAddress(privateKey.PublicKey)
	provider := common.HexToAddress("0x1111111111111111111111111111111111111111")
	escrow := common.HexToAddress("0x2222222222222222222222222222222222222222")
	signer, err := NewSigner(privateKey, escrow, big.NewInt(31337))
	if err != nil {
		t.Fatalf("NewSigner returned error: %v", err)
	}
	state := payment.PaymentState{ContractID: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Publisher: publisher.Hex(), Provider: provider.Hex(), Sequence: 1, Period: 1, CumulativePayment: 50, LastChallengeID: "challenge-1", ValidUntil: uint64(time.Now().Add(time.Hour).Unix()), Status: interfaces.AvailabilityPass}
	signed, err := signer.Sign(state)
	if err != nil {
		t.Fatalf("Sign returned error: %v", err)
	}
	if len(signed.PublisherSignature) != 65 || signed.PublisherSignature[64] < 27 {
		t.Fatalf("signature has invalid Ethereum format: %x", signed.PublisherSignature)
	}
	digest, err := paymentStateDigest(escrow, big.NewInt(31337), signed)
	if err != nil {
		t.Fatalf("paymentStateDigest returned error: %v", err)
	}
	signature := append([]byte(nil), signed.PublisherSignature...)
	signature[64] -= 27
	publicKey, err := crypto.SigToPub(accounts.TextHash(digest.Bytes()), signature)
	if err != nil || crypto.PubkeyToAddress(*publicKey) != publisher {
		t.Fatalf("signature recovery = %v, %v; want %s", publicKey, err, publisher)
	}
}
