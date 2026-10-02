// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "forge-std/Test.sol";
import "../EscrowContract.sol";

contract EscrowContractTest is Test {
    EscrowContract escrow;

    address publisher = address(0x1);
    address provider = address(0x2);

    bytes32 fileID = keccak256("file-1");

    uint256 reward = 1 ether;
    uint64 periods = 10;
    uint64 duration = 1 days;
    bytes32 rulesHash = keccak256("rules");
    uint256 collateralRequired = 0.5 ether;

    function setUp() public {
        escrow = new EscrowContract();
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

        vm.deal(publisher, reward);

        vm.prank(publisher);
        escrow.fundEscrow{value: reward}(contractID);

        assertEq(
            uint256(escrow.getContractState(contractID)),
            uint256(EscrowTypes.ContractState.Funded)
        );

        assertEq(escrow.getEscrowBalance(contractID), reward);
    }

    function testDepositCollateralAndActivate() public {
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

    // Publisher funds escrow
    vm.deal(publisher, reward);

    vm.prank(publisher);
    escrow.fundEscrow{value: reward}(contractID);

    // Provider deposits collateral
    vm.deal(provider, collateralRequired);

    vm.prank(provider);
    escrow.depositCollateral{value: collateralRequired}(contractID);

    assertEq(
        escrow.getCollateral(contractID),
        collateralRequired
    );

    // Publisher activates the contract
    vm.prank(publisher);
    escrow.activateContract(contractID);

    assertEq(
        uint256(escrow.getContractState(contractID)),
        uint256(EscrowTypes.ContractState.Active)
    );
}

}