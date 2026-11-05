// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "forge-std/Test.sol";
import "../EscrowContract.sol";

contract EscrowFailureTest is Test {
    EscrowContract escrow;

    uint256 publisherPrivateKey = 0xA11CE;
    address publisher;
    address provider = address(0x2);

    bytes32 fileID = keccak256("file-1");

    uint256 reward = 1 ether;
    uint64 periods = 10;
    uint64 duration = 1 days;
    bytes32 rulesHash = keccak256("rules");
    uint256 collateralRequired = 0.5 ether;

    function setUp() public {
        escrow = new EscrowContract();
        publisher = vm.addr(publisherPrivateKey);
    }

    function testMarkFailure() public {
        bytes32 contractID = _activateContract();

        // Contract starts Active.
        assertEq(
            uint256(escrow.getContractState(contractID)),
            uint256(EscrowTypes.ContractState.Active)
        );

        // Publisher marks the contract as failed.
        vm.prank(publisher);
        escrow.markFailure(
            contractID,
            EscrowTypes.FailureReason.AvailabilityFailure
        );

        // Contract should now be Failed.
        assertEq(
            uint256(escrow.getContractState(contractID)),
            uint256(EscrowTypes.ContractState.Failed)
        );
    }

    function _activateContract()
        internal
        returns (bytes32 contractID)
    {
        contractID = _createAndFundContract();

        vm.deal(provider, collateralRequired);

        vm.prank(provider);
        escrow.depositCollateral{value: collateralRequired}(contractID);

        vm.prank(publisher);
        escrow.activateContract(contractID);
    }

    function _createAndFundContract()
        internal
        returns (bytes32 contractID)
    {
        vm.prank(publisher);

        contractID = escrow.createContract(
            provider,
            fileID,
            reward,
            periods,
            duration,
            rulesHash,
            collateralRequired
        );

        vm.deal(publisher, reward);

        vm.prank(publisher);
        escrow.fundEscrow{value: reward}(contractID);
    }
    function testSlashCollateral() public {
    bytes32 contractID = _activateContract();

    // Mark the contract as failed.
    vm.prank(publisher);
    escrow.markFailure(
        contractID,
        EscrowTypes.FailureReason.AvailabilityFailure
    );

    uint256 publisherBalanceBefore = publisher.balance;
    uint256 collateralBefore = escrow.getCollateral(contractID);

    uint256 penalty = 0.2 ether;

    // Publisher slashes provider collateral.
    vm.prank(publisher);
    escrow.slashCollateral(contractID, penalty);

    // Collateral should decrease.
    assertEq(
        escrow.getCollateral(contractID),
        collateralBefore - penalty
    );

    // Publisher should receive the penalty.
    assertEq(
        publisher.balance,
        publisherBalanceBefore + penalty
    );

    // Contract should remain Failed.
    assertEq(
        uint256(escrow.getContractState(contractID)),
        uint256(EscrowTypes.ContractState.Failed)
    );
}
function testDisputeFailure() public {
    bytes32 contractID = _activateContract();

    // Publisher marks the contract as failed.
    vm.prank(publisher);
    escrow.markFailure(
        contractID,
        EscrowTypes.FailureReason.AvailabilityFailure
    );

    assertEq(
        uint256(escrow.getContractState(contractID)),
        uint256(EscrowTypes.ContractState.Failed)
    );

    // Provider presents a valid publisher-signed voucher.
    EscrowTypes.PaymentState memory paymentState =
        _createSignedPaymentState(contractID);

    vm.prank(provider);
    escrow.disputeFailure(contractID, paymentState);

    // Failure should be overturned.
    assertEq(
        uint256(escrow.getContractState(contractID)),
        uint256(EscrowTypes.ContractState.Active)
    );

    // Voucher should now be stored.
    EscrowTypes.PaymentState memory stored =
        escrow.getPaymentState(contractID);

    assertEq(stored.contractID, contractID);
    assertEq(stored.publisher, publisher);
    assertEq(stored.provider, provider);
    assertEq(stored.sequence, 1);
    assertEq(stored.cumulativePayment, 0.4 ether);
}
function _createSignedPaymentState(bytes32 contractID)
    internal
    returns (EscrowTypes.PaymentState memory paymentState)
{
    uint64 sequence = 1;
    uint64 period = 1;
    uint256 cumulativePayment = 0.4 ether;
    bytes32 lastChallengeID = keccak256("challenge-1");
    uint64 validUntil = uint64(block.timestamp + 1 days);
    uint8 status = 1;

    bytes32 digest = keccak256(
        abi.encode(
            address(escrow),
            block.chainid,
            contractID,
            publisher,
            provider,
            sequence,
            period,
            cumulativePayment,
            lastChallengeID,
            validUntil,
            status
        )
    );

    bytes32 messageHash = keccak256(
        abi.encodePacked(
            "\x19Ethereum Signed Message:\n32",
            digest
        )
    );

    (uint8 v, bytes32 r, bytes32 s) =
        vm.sign(publisherPrivateKey, messageHash);

    paymentState = EscrowTypes.PaymentState({
        contractID: contractID,
        publisher: publisher,
        provider: provider,
        sequence: sequence,
        period: period,
        cumulativePayment: cumulativePayment,
        lastChallengeID: lastChallengeID,
        validUntil: validUntil,
        status: status,
        signature: abi.encodePacked(r, s, v)
    });
}
}