// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "./contract_state.sol";

interface IEscrowQueries {
    function getContractState(bytes32 contractID) external view returns (EscrowTypes.ContractState);
    function getPaymentState(bytes32 contractID) external view returns (EscrowTypes.PaymentState memory);
    function getEscrowBalance(bytes32 contractID) external view returns (uint256);
    function getCollateral(bytes32 contractID) external view returns (uint256);
}
