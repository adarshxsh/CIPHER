package availability

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	availabilitytypes "cipher/availability/availability-contracts/types"
	coordinator "cipher/availability/escrow-payment/coordinator"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestCoordinatorBridge_DispatchPassAndFail(t *testing.T) {
	// Publisher keys
	pubEdPub, pubEdPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate publisher Ed25519 key: %v", err)
	}

	pubEthPriv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate publisher Ethereum key: %v", err)
	}
	pubEthAddr := crypto.PubkeyToAddress(pubEthPriv.PublicKey)

	publisherPeerID := "12D3KooWPublisher12345"

	// Provider keys
	provEthPriv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate provider Ethereum key: %v", err)
	}
	provEthAddr := crypto.PubkeyToAddress(provEthPriv.PublicKey)
	providerPeerID := "12D3KooWProvider67890"

	// 1. Initialize bridge
	bridge, err := NewCoordinatorBridge(CoordinatorBridgeConfig{
		PublisherPeerID:  publisherPeerID,
		PublisherEthKey:  pubEthPriv,
		PublisherEdKey:   pubEdPriv,
		FailureThreshold: 3,
	})
	if err != nil {
		t.Fatalf("failed to create coordinator bridge: %v", err)
	}

	// 2. Register provider binding
	if err := bridge.RegisterProvider(providerPeerID, provEthAddr); err != nil {
		t.Fatalf("failed to register provider: %v", err)
	}

	// 3. Register contract schedule
	contractID := availabilitytypes.ContractID("avail-contract-100")
	var escrowID [32]byte
	rand.Read(escrowID[:])

	paymentPerPass := uint64(250)
	maxPayment := uint64(1000)

	if err := bridge.RegisterContractSchedule(contractID, escrowID, paymentPerPass, maxPayment); err != nil {
		t.Fatalf("failed to register contract schedule: %v", err)
	}

	// 4. Test PASS Dispatch
	passResult := availabilitytypes.AvailabilityResult{
		ContractID:  contractID,
		ProviderID:  providerPeerID,
		Period:      1,
		ChallengeID: "challenge-1",
		Result:      availabilitytypes.AvailabilityPass,
		Timestamp:   time.Now().UTC(),
	}

	res, err := bridge.DispatchAvailabilityResult(passResult)
	if err != nil {
		t.Fatalf("DispatchAvailabilityResult (PASS) failed: %v", err)
	}

	if res.Kind != coordinator.DispatchKindPass {
		t.Fatalf("expected DispatchKindPass, got %s", res.Kind)
	}
	if res.PaymentState.CumulativePayment != 250 {
		t.Fatalf("expected cumulative payment 250, got %d", res.PaymentState.CumulativePayment)
	}
	if res.PaymentState.Sequence != 1 {
		t.Fatalf("expected sequence 1, got %d", res.PaymentState.Sequence)
	}

	// Verify signature on voucher
	if !VerifyPaymentVoucher(res.PaymentState, pubEdPub, pubEthAddr) {
		t.Fatalf("voucher signature verification failed")
	}

	// 5. Test Consecutive FAIL Dispatch and Slashing Threshold
	for i := 1; i <= 3; i++ {
		failResult := availabilitytypes.AvailabilityResult{
			ContractID:  contractID,
			ProviderID:  providerPeerID,
			Period:      uint64(i + 1),
			ChallengeID: availabilitytypes.ChallengeID(common.BytesToHash([]byte{byte(i + 10)}).Hex()),
			Result:      availabilitytypes.AvailabilityFail,
			Timestamp:   time.Now().UTC(),
		}

		failRes, err := bridge.DispatchAvailabilityResult(failResult)
		if err != nil {
			t.Fatalf("DispatchAvailabilityResult (FAIL #%d) failed: %v", i, err)
		}

		if failRes.Kind != coordinator.DispatchKindFail {
			t.Fatalf("expected DispatchKindFail, got %s", failRes.Kind)
		}
		if failRes.ConsecutiveFailures != uint32(i) {
			t.Fatalf("expected %d consecutive failures, got %d", i, failRes.ConsecutiveFailures)
		}

		if i == 3 {
			if !failRes.SlashEligible {
				t.Fatalf("expected result to be SlashEligible on 3rd failure")
			}
			if len(failRes.SlashEvidence) != 3 {
				t.Fatalf("expected 3 evidence records, got %d", len(failRes.SlashEvidence))
			}
		} else {
			if failRes.SlashEligible {
				t.Fatalf("did not expect SlashEligible on failure #%d", i)
			}
		}
	}
}
