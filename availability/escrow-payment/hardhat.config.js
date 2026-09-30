require("@nomicfoundation/hardhat-toolbox");

module.exports = {
  solidity: {
    version: "0.8.20",
    settings: { optimizer: { enabled: true, runs: 200 } }
  },
  paths: {
    sources: "./escrow",
    tests: "./solidity-tests",
    cache: "./.hardhat-cache",
    artifacts: "./artifacts"
  }
};
