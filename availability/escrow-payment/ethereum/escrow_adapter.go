package ethereum

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	payment "cipher/availability/escrow-payment/payment"
)

const escrowABI = `[{"inputs":[{"internalType":"bytes32","name":"contractID","type":"bytes32"},{"components":[{"internalType":"bytes32","name":"contractID","type":"bytes32"},{"internalType":"address","name":"publisher","type":"address"},{"internalType":"address","name":"provider","type":"address"},{"internalType":"uint64","name":"sequence","type":"uint64"},{"internalType":"uint64","name":"period","type":"uint64"},{"internalType":"uint256","name":"cumulativePayment","type":"uint256"},{"internalType":"bytes32","name":"lastChallengeID","type":"bytes32"},{"internalType":"uint64","name":"validUntil","type":"uint64"},{"internalType":"uint8","name":"status","type":"uint8"},{"internalType":"bytes","name":"signature","type":"bytes"}],"internalType":"struct EscrowTypes.PaymentState","name":"paymentState","type":"tuple"}],"name":"submitPaymentState","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"contractID","type":"bytes32"},{"internalType":"enum EscrowTypes.FailureReason","name":"reason","type":"uint8"}],"name":"markFailure","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"contractID","type":"bytes32"},{"internalType":"uint256","name":"penalty","type":"uint256"}],"name":"slashCollateral","outputs":[],"stateMutability":"nonpayable","type":"function"}]`

type solidityPaymentState struct {
	ContractID        [32]byte
	Publisher         common.Address
	Provider          common.Address
	Sequence          uint64
	Period            uint64
	CumulativePayment *big.Int
	LastChallengeID   [32]byte
	ValidUntil        uint64
	Status            uint8
	Signature         []byte
}

// EscrowAdapter is a real Ethereum-RPC implementation of payment.EscrowSubmitter.
type EscrowAdapter struct {
	client   *ethclient.Client
	contract *bind.BoundContract
	auth     *bind.TransactOpts
}

func DialEscrowAdapter(ctx context.Context, rpcURL string, contractAddress common.Address, senderPrivateKey *ecdsa.PrivateKey, chainID *big.Int) (*EscrowAdapter, error) {
	if rpcURL == "" || contractAddress == (common.Address{}) || senderPrivateKey == nil || chainID == nil || chainID.Sign() <= 0 {
		return nil, errors.New("RPC URL, contract address, sender key, and positive chain ID are required")
	}
	client, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, err
	}
	parsedABI, err := abi.JSON(strings.NewReader(escrowABI))
	if err != nil {
		client.Close()
		return nil, err
	}
	auth, err := bind.NewKeyedTransactorWithChainID(senderPrivateKey, chainID)
	if err != nil {
		client.Close()
		return nil, err
	}
	return &EscrowAdapter{client: client, contract: bind.NewBoundContract(contractAddress, parsedABI, client, client, client), auth: auth}, nil
}

func (a *EscrowAdapter) Close() { a.client.Close() }

// SubmitPaymentState sends the contract transaction; callers can later wait
// for the returned transaction through a higher-level chain monitor.
func (a *EscrowAdapter) SubmitPaymentState(contractID string, state payment.PaymentState) error {
	if a == nil || a.contract == nil || a.auth == nil || contractID != state.ContractID {
		return errors.New("matching signed payment state and escrow adapter are required")
	}
	parsedContractID := common.HexToHash(contractID)
	publisher := common.HexToAddress(state.Publisher)
	provider := common.HexToAddress(state.Provider)
	if parsedContractID == (common.Hash{}) || publisher == (common.Address{}) || provider == (common.Address{}) || len(state.PublisherSignature) != 65 || state.ValidUntil == 0 {
		return errors.New("payment state is not Ethereum-compatible")
	}
	challengeHash := crypto.Keccak256Hash([]byte(state.LastChallengeID))
	solidityState := solidityPaymentState{ContractID: parsedContractID, Publisher: publisher, Provider: provider, Sequence: state.Sequence, Period: state.Period, CumulativePayment: new(big.Int).SetUint64(state.CumulativePayment), LastChallengeID: challengeHash, ValidUntil: state.ValidUntil, Status: statusCode(string(state.Status)), Signature: state.PublisherSignature}
	options := *a.auth
	_, err := a.contract.Transact(&options, "submitPaymentState", parsedContractID, solidityState)
	return err
}

var _ payment.EscrowSubmitter = (*EscrowAdapter)(nil)

// MarkFailure records a verified Availability failure. The adapter account
// must be the escrow agreement's publisher, as required by the Solidity contract.
func (a *EscrowAdapter) MarkFailure(contractID string) error {
	if a == nil || a.contract == nil || a.auth == nil || common.HexToHash(contractID) == (common.Hash{}) {
		return errors.New("valid escrow adapter and contract ID are required")
	}
	options := *a.auth
	_, err := a.contract.Transact(&options, "markFailure", common.HexToHash(contractID), uint8(1))
	return err
}

// SlashCollateral applies an eligible publisher-authorized penalty in wei.
func (a *EscrowAdapter) SlashCollateral(contractID string, penalty uint64) error {
	if a == nil || a.contract == nil || a.auth == nil || common.HexToHash(contractID) == (common.Hash{}) || penalty == 0 {
		return errors.New("valid escrow adapter, contract ID, and positive penalty are required")
	}
	options := *a.auth
	_, err := a.contract.Transact(&options, "slashCollateral", common.HexToHash(contractID), new(big.Int).SetUint64(penalty))
	return err
}

var _ payment.EscrowFailureHandler = (*EscrowAdapter)(nil)
