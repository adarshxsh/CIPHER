// Package ethereum contains EVM-specific payment and escrow adapters.
package ethereum

import (
	"crypto/ecdsa"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	payment "cipher/availability/escrow-payment/payment"
)

// Signer signs exactly the EIP-191 payment-state digest verified by
// EscrowContract.sol. The publisher field must match this private key.
type Signer struct {
	privateKey     *ecdsa.PrivateKey
	escrowContract common.Address
	chainID        *big.Int
}

func NewSigner(privateKey *ecdsa.PrivateKey, escrowContract common.Address, chainID *big.Int) (*Signer, error) {
	if privateKey == nil || escrowContract == (common.Address{}) || chainID == nil || chainID.Sign() <= 0 {
		return nil, errors.New("private key, escrow contract address, and positive chain ID are required")
	}
	return &Signer{privateKey: privateKey, escrowContract: escrowContract, chainID: new(big.Int).Set(chainID)}, nil
}

func (s *Signer) Sign(state payment.PaymentState) (payment.PaymentState, error) {
	if state.ValidUntil == 0 {
		return payment.PaymentState{}, errors.New("payment state valid-until timestamp is required for Ethereum signing")
	}
	publisher := common.HexToAddress(state.Publisher)
	if publisher == (common.Address{}) || publisher != crypto.PubkeyToAddress(s.privateKey.PublicKey) {
		return payment.PaymentState{}, errors.New("payment-state publisher must match Ethereum signer")
	}
	digest, err := paymentStateDigest(s.escrowContract, s.chainID, state)
	if err != nil {
		return payment.PaymentState{}, err
	}
	signature, err := crypto.Sign(accounts.TextHash(digest.Bytes()), s.privateKey)
	if err != nil {
		return payment.PaymentState{}, err
	}
	signature[64] += 27
	state.PublisherSignature = signature
	return state, nil
}

func paymentStateDigest(escrowContract common.Address, chainID *big.Int, state payment.PaymentState) (common.Hash, error) {
	contractID := common.HexToHash(state.ContractID)
	publisher := common.HexToAddress(state.Publisher)
	provider := common.HexToAddress(state.Provider)
	if contractID == (common.Hash{}) || publisher == (common.Address{}) || provider == (common.Address{}) {
		return common.Hash{}, errors.New("contract ID, publisher, and provider must be valid Ethereum values")
	}
	arguments := abi.Arguments{
		{Type: mustType("address")}, {Type: mustType("uint256")}, {Type: mustType("bytes32")}, {Type: mustType("address")}, {Type: mustType("address")},
		{Type: mustType("uint64")}, {Type: mustType("uint64")}, {Type: mustType("uint256")}, {Type: mustType("bytes32")}, {Type: mustType("uint64")}, {Type: mustType("uint8")},
	}
	packed, err := arguments.Pack(escrowContract, chainID, contractID, publisher, provider, state.Sequence, state.Period, new(big.Int).SetUint64(state.CumulativePayment), crypto.Keccak256Hash([]byte(state.LastChallengeID)), state.ValidUntil, statusCode(string(state.Status)))
	if err != nil {
		return common.Hash{}, err
	}
	return crypto.Keccak256Hash(packed), nil
}

func mustType(name string) abi.Type {
	value, err := abi.NewType(name, "", nil)
	if err != nil {
		panic(err)
	}
	return value
}
func statusCode(status string) uint8 {
	if status == "PASS" {
		return 1
	}
	return 2
}
