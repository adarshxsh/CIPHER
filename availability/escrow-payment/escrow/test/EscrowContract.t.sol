// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "forge-std/Test.sol";
import "../EscrowContract.sol";

contract EscrowContractTest is Test {
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

    function testCreateContract() public {
        vm.prank(publisher);

        bytes32 contractID = escrow.createContract(
            provider,
            fileID,
            reward,
            periods,
            duration,
            rulesHash,
            collateralRequired
        );

        assertTrue(contractID != bytes32(0));

        assertEq(
            uint256(escrow.getContractState(contractID)),
            uint256(EscrowTypes.ContractState.Created)
        );

        assertEq(escrow.getEscrowBalance(contractID), 0);
        assertEq(escrow.getCollateral(contractID), 0);
    }

    function testFundEscrow() public {
        bytes32 contractID = _createAndFundContract();

        assertEq(
            uint256(escrow.getContractState(contractID)),
            uint256(EscrowTypes.ContractState.Funded)
        );

        assertEq(escrow.getEscrowBalance(contractID), reward);
    }

    function testDepositCollateralAndActivate() public {
        bytes32 contractID = _createAndFundContract();

        vm.deal(provider, collateralRequired);

        vm.prank(provider);
        escrow.depositCollateral{value: collateralRequired}(contractID);

        assertEq(
            escrow.getCollateral(contractID),
            collateralRequired
        );

        vm.prank(publisher);
        escrow.activateContract(contractID);

        assertEq(
            uint256(escrow.getContractState(contractID)),
            uint256(EscrowTypes.ContractState.Active)
        );
    }

    function testSubmitPaymentStateAndSettle() public {
    bytes32 contractID = _activateContract();

    EscrowTypes.PaymentState memory paymentState =
        _createSignedPaymentState(contractID);

    // Submit signed voucher.
    vm.prank(provider);
    escrow.submitPaymentState(contractID, paymentState);

    // Contract should now remember the voucher.
    EscrowTypes.PaymentState memory stored =
        escrow.getPaymentState(contractID);

    assertEq(stored.contractID, contractID);
    assertEq(stored.publisher, publisher);
    assertEq(stored.provider, provider);
    assertEq(stored.sequence, 1);
    assertEq(stored.cumulativePayment, 0.4 ether);

    // Settlement pays provider and refunds publisher.
    uint256 providerBalanceBefore = provider.balance;
    uint256 publisherBalanceBefore = publisher.balance;

    vm.prank(provider);
    escrow.settleContract(contractID, paymentState);

    assertEq(
        uint256(escrow.getContractState(contractID)),
        uint256(EscrowTypes.ContractState.Settled)
    );

    assertEq(escrow.getEscrowBalance(contractID), 0);

    assertEq(
        provider.balance,
        providerBalanceBefore + 0.4 ether
    );

    assertEq(
        publisher.balance,
        publisherBalanceBefore + 0.6 ether
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
}