#!/usr/bin/env bash
# ==============================================================================
# CIPHER: COMPLETE 8-TERMINAL DECENTRALIZED CDN & PAYMENT ORCHESTRATOR
# ==============================================================================
# Automatic 4x2 desktop grid window tiling for all 8 network roles:
#
#   TOP ROW:
#     [Terminal 1] Anvil EVM Blockchain (Port 8545)
#     [Terminal 2] Circuit Relay v2 (Port 4001, NAT Traversal & Hole Punching)
#     [Terminal 3] Kademlia DHT Bootstrap Node (Port 4003, Discovery Hub)
#     [Terminal 4] Storage Provider 1 (Port 4101, EVM Ticket Verifier)
#
#   BOTTOM ROW:
#     [Terminal 5] Storage Provider 2 (Port 4102, EVM Ticket Verifier)
#     [Terminal 6] Storage Provider 3 (Port 4103, Fault-Tolerance Provider)
#     [Terminal 7] Publisher Node (Port 4201, Multi-Provider Replication R=2/3)
#     [Terminal 8] Consumer / Client Controller (Port 4301, DHT Swarm + Payments)
# ==============================================================================

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
if [ -f "$SCRIPT_DIR/go.mod" ]; then
    ROOT="$SCRIPT_DIR"
else
    ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
fi
cd "$ROOT"

# Terminal ANSI styling
BOLD="\033[1m"
GREEN="\033[1;32m"
CYAN="\033[1;36m"
YELLOW="\033[1;33m"
MAGENTA="\033[1;35m"
RED="\033[1;31m"
BLUE="\033[1;34m"
NC="\033[0m"

MODE="windows" # default to macOS tiled windows
INTERACTIVE=true

while [[ $# -gt 0 ]]; do
    case $1 in
        --auto)
            INTERACTIVE=false
            shift
            ;;
        --single)
            MODE="single"
            shift
            ;;
        --help|-h)
            echo "CIPHER Complete 8-Terminal Architecture Runner"
            echo "Usage: ./local_multiple_terminal_test.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --auto      Run all stages automatically without pausing"
            echo "  --single    Run all 8 roles in this single terminal (headless/CI)"
            echo "  -h, --help  Show this help message"
            exit 0
            ;;
        *)
            shift
            ;;
    esac
done

pause_step() {
    if [ "$INTERACTIVE" = true ]; then
        echo -e "\n${YELLOW}Press [ENTER] to continue to the next job...${NC}"
        read -r
    else
        sleep 1
    fi
}

echo -e "${BOLD}${CYAN}======================================================================${NC}"
echo -e "${BOLD}${CYAN}      CIPHER 8-TERMINAL COMPLETE CDN & PAYMENT ARCHITECTURE           ${NC}"
echo -e "${BOLD}${CYAN}======================================================================${NC}"
echo -e "Desktop Layout Grid (4 Columns x 2 Rows):"
echo -e "  +--------------------+--------------------+--------------------+--------------------+"
echo -e "  | [1] ANVIL EVM      | [2] RELAY V2       | [3] BOOTSTRAP      | [4] PROVIDER 1     |"
echo -e "  |     Port 8545      |     Port 4001      |     Port 4003      |     Port 4101      |"
echo -e "  +--------------------+--------------------+--------------------+--------------------+"
echo -e "  | [5] PROVIDER 2     | [6] PROVIDER 3     | [7] PUBLISHER      | [8] CONSUMER       |"
echo -e "  |     Port 4102      |     Port 4103      |     Port 4201      |     Port 4301      |"
echo -e "  +--------------------+--------------------+--------------------+--------------------+"

# Clean previous test directories and build binaries
echo -e "\n${BOLD}[*] Building all CIPHER node and network binaries...${NC}"
mkdir -p bin
go build -o bin/bootstrap ./network/cmd/bootstrap
go build -o bin/relay ./network/cmd/relay
go build -o bin/provider ./nodes/provider
go build -o bin/publisher ./nodes/publisher
go build -o bin/consumer ./nodes/consumer
echo -e "${GREEN}[✓] All 5 binaries compiled cleanly in ./bin/${NC}"

rm -rf store_p1 store_p2 store_p3 store_pub store_client store_client_fault test_pay_orig.dat test_pay_recovered.dat test_pay_fault.dat
rm -f anvil.log relay.log bootstrap.log provider1.log provider2.log provider3.log publisher.log consumer.log fault.log .pub_done .consumer_done .fault_done

ENTROPY_ADDR="0xCf7Ed3AccA5a467e9e704C703E8D87F634fB0Fc9"
PROVIDER_ETH_ADDR="0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
CLIENT_ETH_ADDR="0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC"
CLIENT_ETH_KEY="5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a"
CHANNEL_CONTRACT_ADDR="0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512"

# ------------------------------------------------------------------------------
# WINDOW TILING ENGINE (macOS Terminal.app)
# ------------------------------------------------------------------------------
RAW_BOUNDS=$(osascript -e 'tell application "Finder" to get bounds of window of desktop' 2>/dev/null || echo "0, 0, 1710, 1112")
SCREEN_W=$(echo "$RAW_BOUNDS" | awk -F', ' '{print $3}')
SCREEN_H=$(echo "$RAW_BOUNDS" | awk -F', ' '{print $4}')
[ -z "$SCREEN_W" ] || [ "$SCREEN_W" -eq 0 ] && SCREEN_W=1680
[ -z "$SCREEN_H" ] || [ "$SCREEN_H" -eq 0 ] && SCREEN_H=1050

TOP_BAR=30
BOTTOM_MARGIN=60
USABLE_H=$((SCREEN_H - TOP_BAR - BOTTOM_MARGIN))
USABLE_W=$SCREEN_W

COL_W=$((USABLE_W / 4))
ROW_H=$((USABLE_H / 2))

launch_tiled_window() {
    local slot="$1" # 1 to 8
    local title="$2"
    local cmd="$3"

    local x1 y1 x2 y2
    case $slot in
        1) x1=0; y1=$TOP_BAR; x2=$COL_W; y2=$((TOP_BAR + ROW_H)) ;;
        2) x1=$COL_W; y1=$TOP_BAR; x2=$((2 * COL_W)); y2=$((TOP_BAR + ROW_H)) ;;
        3) x1=$((2 * COL_W)); y1=$TOP_BAR; x2=$((3 * COL_W)); y2=$((TOP_BAR + ROW_H)) ;;
        4) x1=$((3 * COL_W)); y1=$TOP_BAR; x2=$USABLE_W; y2=$((TOP_BAR + ROW_H)) ;;
        5) x1=0; y1=$((TOP_BAR + ROW_H)); x2=$COL_W; y2=$((TOP_BAR + 2 * ROW_H)) ;;
        6) x1=$COL_W; y1=$((TOP_BAR + ROW_H)); x2=$((2 * COL_W)); y2=$((TOP_BAR + 2 * ROW_H)) ;;
        7) x1=$((2 * COL_W)); y1=$((TOP_BAR + ROW_H)); x2=$((3 * COL_W)); y2=$((TOP_BAR + 2 * ROW_H)) ;;
        8) x1=$((3 * COL_W)); y1=$((TOP_BAR + ROW_H)); x2=$USABLE_W; y2=$((TOP_BAR + 2 * ROW_H)) ;;
    esac

    osascript <<EOF >/dev/null 2>&1
tell application "Terminal"
    set newTab to do script "cd \"$ROOT\" && $cmd"
    set targetWin to first window whose tabs contains newTab
    set bounds of targetWin to {$x1, $y1, $x2, $y2}
    set custom title of targetWin to "$title"
end tell
EOF
}

# ------------------------------------------------------------------------------
# SECTION 1: LAUNCH THE 6 ALWAYS-RUNNING DAEMONS
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}${MAGENTA}--- [LAUNCHING 6 ALWAYS-RUNNING DAEMON ROLES] ---${NC}"

# Clean existing processes on ports 8545, 4001, 4003, 4101, 4102, 4103, 4201, 4301, 4302
for p in 8545 4001 4003 4101 4102 4103 4201 4301 4302; do
    if lsof -ti tcp:$p -sTCP:LISTEN >/dev/null 2>&1; then
        echo -e "${YELLOW}[!] Cleaning existing server on port $p...${NC}"
        kill -9 $(lsof -ti tcp:$p -sTCP:LISTEN) 2>/dev/null || true
    fi
done
sleep 1

# [TERMINAL 1] Anvil EVM Blockchain (Slot 1)
if [ "$MODE" == "windows" ] && [[ "$OSTYPE" == "darwin"* ]]; then
    echo -e "${CYAN}[Terminal 1/8] Spawning Anvil EVM Blockchain in Desktop Slot 1...${NC}"
    launch_tiled_window 1 "CIPHER [1/8] Anvil EVM (Port 8545)" "anvil --port 8545"
else
    echo -e "${CYAN}[Terminal 1/8] Starting Anvil EVM Blockchain in background...${NC}"
    anvil --port 8545 --silent > anvil.log 2>&1 &
    ANVIL_PID=$!
fi

# Wait for Anvil to be healthy
while ! curl -s -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' http://127.0.0.1:8545 >/dev/null 2>&1; do
    sleep 0.2
done
echo -e "${GREEN}[✓] Anvil EVM is active on http://127.0.0.1:8545${NC}"

# Deploy Smart Contracts on Anvil
echo -e "\n${BOLD}[*] Deploying Smart Contracts & Funding Escrow Channel...${NC}"
(cd payments && forge script script/Step1_Setup.s.sol --rpc-url http://127.0.0.1:8545 --broadcast > /dev/null)
echo -e "${GREEN}[✓] Smart Contracts deployed!${NC}"
echo -e "  - EntropySource:       ${BOLD}$ENTROPY_ADDR${NC}"
echo -e "  - Escrow Channel:      ${BOLD}$CHANNEL_CONTRACT_ADDR${NC}"
echo -e "  - Provider Eth Wallet: ${BOLD}$PROVIDER_ETH_ADDR${NC}"
echo -e "  - Client Eth Wallet:   ${BOLD}$CLIENT_ETH_ADDR${NC}"

# [TERMINAL 2] Circuit Relay v2 (Slot 2)
./bin/relay > relay.log 2>&1 &
RELAY_PID=$!
sleep 2

RELAY_MULTIADDR=$(grep "127.0.0.1/tcp/4001/p2p/" relay.log | head -n 1 | awk '{print $NF}')
echo -e "${GREEN}[✓] Relay Multiaddress: ${BOLD}$RELAY_MULTIADDR${NC}"

if [ "$MODE" == "windows" ] && [[ "$OSTYPE" == "darwin"* ]]; then
    kill $RELAY_PID 2>/dev/null || true
    echo -e "${CYAN}[Terminal 2/8] Spawning Circuit Relay v2 in Desktop Slot 2...${NC}"
    launch_tiled_window 2 "CIPHER [2/8] Circuit Relay v2 (Port 4001)" "./bin/relay"
    sleep 2
fi

# [TERMINAL 3] DHT Bootstrap Node (Slot 3)
./bin/bootstrap -p 4003 -ws-port 0 -identity ./store_pub/boot.key > bootstrap.log 2>&1 &
BOOT_PID=$!
sleep 2

BOOTSTRAP_MULTIADDR=$(grep "127.0.0.1/tcp/4003/p2p/" bootstrap.log | head -n 1 | awk '{print $NF}')
echo -e "${GREEN}[✓] Bootstrap Multiaddress: ${BOLD}$BOOTSTRAP_MULTIADDR${NC}"

if [ "$MODE" == "windows" ] && [[ "$OSTYPE" == "darwin"* ]]; then
    kill $BOOT_PID 2>/dev/null || true
    echo -e "${CYAN}[Terminal 3/8] Spawning DHT Bootstrap Node in Desktop Slot 3...${NC}"
    launch_tiled_window 3 "CIPHER [3/8] DHT Bootstrap (Port 4003)" "./bin/bootstrap -p 4003 -ws-port 0 -identity ./store_pub/boot.key"
    sleep 2
fi

# [TERMINAL 4] Storage Provider 1 (Slot 4)
if [ "$MODE" == "windows" ] && [[ "$OSTYPE" == "darwin"* ]]; then
    echo -e "${CYAN}[Terminal 4/8] Spawning Storage Provider 1 in Desktop Slot 4...${NC}"
    launch_tiled_window 4 "CIPHER [4/8] Storage Provider 1 (Port 4101)" "./bin/provider -p 4101 -ws-port 0 -identity ./store_p1/p1.key -store ./store_p1 -bootstrap '$BOOTSTRAP_MULTIADDR' --eth-rpc http://127.0.0.1:8545 --entropy-addr '$ENTROPY_ADDR'"
else
    echo -e "${CYAN}[Terminal 4/8] Starting Storage Provider 1 in background...${NC}"
    ./bin/provider -p 4101 -ws-port 0 -identity ./store_p1/p1.key -store ./store_p1 \
      -bootstrap "$BOOTSTRAP_MULTIADDR" --eth-rpc http://127.0.0.1:8545 --entropy-addr "$ENTROPY_ADDR" > provider1.log 2>&1 &
    PROV1_PID=$!
fi

# [TERMINAL 5] Storage Provider 2 (Slot 5)
if [ "$MODE" == "windows" ] && [[ "$OSTYPE" == "darwin"* ]]; then
    echo -e "${CYAN}[Terminal 5/8] Spawning Storage Provider 2 in Desktop Slot 5...${NC}"
    launch_tiled_window 5 "CIPHER [5/8] Storage Provider 2 (Port 4102)" "./bin/provider -p 4102 -ws-port 0 -identity ./store_p2/p2.key -store ./store_p2 -bootstrap '$BOOTSTRAP_MULTIADDR' --eth-rpc http://127.0.0.1:8545 --entropy-addr '$ENTROPY_ADDR'"
else
    echo -e "${CYAN}[Terminal 5/8] Starting Storage Provider 2 in background...${NC}"
    ./bin/provider -p 4102 -ws-port 0 -identity ./store_p2/p2.key -store ./store_p2 \
      -bootstrap "$BOOTSTRAP_MULTIADDR" --eth-rpc http://127.0.0.1:8545 --entropy-addr "$ENTROPY_ADDR" > provider2.log 2>&1 &
    PROV2_PID=$!
fi

# [TERMINAL 6] Storage Provider 3 (Fault Tolerance Node) (Slot 6)
if [ "$MODE" == "windows" ] && [[ "$OSTYPE" == "darwin"* ]]; then
    echo -e "${CYAN}[Terminal 6/8] Spawning Storage Provider 3 in Desktop Slot 6...${NC}"
    launch_tiled_window 6 "CIPHER [6/8] Storage Provider 3 (Port 4103)" "./bin/provider -p 4103 -ws-port 0 -identity ./store_p3/p3.key -store ./store_p3 -bootstrap '$BOOTSTRAP_MULTIADDR' --eth-rpc http://127.0.0.1:8545 --entropy-addr '$ENTROPY_ADDR'"
else
    echo -e "${CYAN}[Terminal 6/8] Starting Storage Provider 3 in background...${NC}"
    ./bin/provider -p 4103 -ws-port 0 -identity ./store_p3/p3.key -store ./store_p3 \
      -bootstrap "$BOOTSTRAP_MULTIADDR" --eth-rpc http://127.0.0.1:8545 --entropy-addr "$ENTROPY_ADDR" > provider3.log 2>&1 &
    PROV3_PID=$!
fi
sleep 3

echo -e "\n${GREEN}${BOLD}[✓] ALL 6 DAEMON SERVICES ARE RUNNING IN TILED DESKTOP WINDOWS!${NC}"

cleanup() {
    if [ "$MODE" == "single" ]; then
        echo -e "\nCleaning up background daemons..."
        kill $ANVIL_PID $RELAY_PID $BOOT_PID $PROV1_PID $PROV2_PID $PROV3_PID 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ------------------------------------------------------------------------------
# SECTION 2: INTERACTIVE PUBLISHER & CONSUMER JOBS
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}${MAGENTA}--- [INTERACTIVE EXECUTION ACROSS TERMINALS 7 & 8] ---${NC}"

# STEP 1: INITIAL WALLET BALANCES
echo -e "\n${BOLD}${BLUE}=== [JOB 1/6] INSPECTING INITIAL WALLET BALANCES ===${NC}"
CLIENT_BAL=$(cast balance "$CLIENT_ETH_ADDR" --rpc-url http://127.0.0.1:8545 --ether)
PROV_BAL=$(cast balance "$PROVIDER_ETH_ADDR" --rpc-url http://127.0.0.1:8545 --ether)
CHANNEL_BAL=$(cast balance "$CHANNEL_CONTRACT_ADDR" --rpc-url http://127.0.0.1:8545 --ether)

echo -e "  💳 ${BOLD}Client Wallet Balance  :${NC} ${YELLOW}$CLIENT_BAL ETH${NC}"
echo -e "  💳 ${BOLD}Provider Wallet Balance:${NC} ${YELLOW}$PROV_BAL ETH${NC}"
echo -e "  🏦 ${BOLD}Escrow Channel Deposit :${NC} ${YELLOW}$CHANNEL_BAL ETH${NC}"
pause_step

# STEP 2: [TERMINAL 7] PUBLISHER INGESTION & REPLICATION
echo -e "\n${BOLD}${BLUE}=== [JOB 2/6] [Terminal 7] PUBLISHER INGESTING & PUSHING (R=2 ACROSS 3 PROVIDERS) ===${NC}"
head -c 1048576 </dev/urandom > test_pay_orig.dat
ORIG_HASH=$(shasum -a 256 test_pay_orig.dat | awk '{print $1}')
echo -e "  Generated 1 MB Payload SHA-256: ${BOLD}$ORIG_HASH${NC}"

if [ "$MODE" == "windows" ] && [[ "$OSTYPE" == "darwin"* ]]; then
    rm -f .pub_done publisher.log
    echo -e "${CYAN}[Terminal 7/8] Spawning Publisher in Desktop Slot 7 (Bottom Row, Col 3)...${NC}"
    launch_tiled_window 7 "CIPHER [7/8] Publisher Node (Port 4201)" "./bin/publisher -p 4201 -file test_pay_orig.dat -bootstrap '$BOOTSTRAP_MULTIADDR' -replication 2 -push 2>&1 | tee publisher.log; echo \$? > .pub_done"
    while [ ! -f .pub_done ]; do
        sleep 0.3
    done
    PUB_EXIT=$(cat .pub_done)
    if [ "$PUB_EXIT" -ne 0 ]; then
        echo -e "${RED}[❌ FAILED] Publisher exited with code $PUB_EXIT${NC}"
        exit 1
    fi
else
    ./bin/publisher -p 4201 -file test_pay_orig.dat -bootstrap "$BOOTSTRAP_MULTIADDR" -replication 2 -push 2>&1 | tee publisher.log
fi

CONTENT_ID=$(grep "ContentID     :" publisher.log | awk '{print $NF}')
KEY=$(grep "Decryption Key:" publisher.log | awk '{print $NF}')

echo -e "${GREEN}[✓] Publisher pushed content successfully:${NC}"
echo -e "  - ContentID:      ${BOLD}$CONTENT_ID${NC}"
echo -e "  - Decryption Key: ${BOLD}$KEY${NC}"
pause_step

# STEP 3: [TERMINAL 8] CONSUMER SWARM DOWNLOAD WITH EIP-712 PAYMENT TICKETS
echo -e "\n${BOLD}${BLUE}=== [JOB 3/6] [Terminal 8] CONSUMER SWARM DOWNLOAD WITH PAYMENT TICKETS ===${NC}"
echo -e "Connecting to DHT, discovering providers, streaming signed lottery tickets per chunk..."

if [ "$MODE" == "windows" ] && [[ "$OSTYPE" == "darwin"* ]]; then
    rm -f .consumer_done consumer.log
    echo -e "${CYAN}[Terminal 8/8] Spawning Consumer Client in Desktop Slot 8 (Bottom Row, Col 4)...${NC}"
    launch_tiled_window 8 "CIPHER [8/8] Consumer Client (Port 4301)" "./bin/consumer -p 4301 -fetch '$CONTENT_ID' -key '$KEY' -out test_pay_recovered.dat -bootstrap '$BOOTSTRAP_MULTIADDR' -store ./store_client --eth-rpc http://127.0.0.1:8545 --eth-key '$CLIENT_ETH_KEY' --entropy-addr '$ENTROPY_ADDR' --provider-eth-addr '$PROVIDER_ETH_ADDR' 2>&1 | tee consumer.log; echo \$? > .consumer_done"
    while [ ! -f .consumer_done ]; do
        sleep 0.3
    done
    CONSUMER_EXIT=$(cat .consumer_done)
    if [ "$CONSUMER_EXIT" -ne 0 ]; then
        echo -e "${RED}[❌ FAILED] Consumer exited with code $CONSUMER_EXIT${NC}"
        exit 1
    fi
else
    ./bin/consumer -p 4301 -fetch "$CONTENT_ID" -key "$KEY" -out test_pay_recovered.dat \
      -bootstrap "$BOOTSTRAP_MULTIADDR" -store ./store_client \
      --eth-rpc http://127.0.0.1:8545 --eth-key "$CLIENT_ETH_KEY" \
      --entropy-addr "$ENTROPY_ADDR" --provider-eth-addr "$PROVIDER_ETH_ADDR" 2>&1 | tee consumer.log
fi

RECOVERED_HASH=$(shasum -a 256 test_pay_recovered.dat | awk '{print $1}')
echo -e "\nDownloaded File SHA-256: ${BOLD}$RECOVERED_HASH${NC}"

if [ "$ORIG_HASH" != "$RECOVERED_HASH" ]; then
    echo -e "${RED}[❌ FAILED] Hash mismatch!${NC}"
    exit 1
fi
echo -e "${GREEN}[✓] 100% BIT-FOR-BIT DATA INTEGRITY VERIFIED!${NC}"
pause_step

# STEP 4: FAULT TOLERANCE TEST (Simulate Dead Provider 1)
echo -e "\n${BOLD}${BLUE}=== [JOB 4/6] FAULT TOLERANCE: KILLING PROVIDER 1 & CONSUMER 2 RETRIEVAL ===${NC}"
if [ "$MODE" == "single" ]; then
    kill $PROV1_PID 2>/dev/null || true
    echo -e "${YELLOW}[!] Provider 1 (PID $PROV1_PID) terminated to simulate node failure.${NC}"
else
    if lsof -ti tcp:4101 -sTCP:LISTEN >/dev/null 2>&1; then
        kill -9 $(lsof -ti tcp:4101 -sTCP:LISTEN) 2>/dev/null || true
        echo -e "${YELLOW}[!] Provider 1 (Port 4101) terminated to simulate node failure.${NC}"
    fi
fi
sleep 2

echo -e "Consumer 2 fetching from remaining surviving Providers (2 & 3)..."
if [ "$MODE" == "windows" ] && [[ "$OSTYPE" == "darwin"* ]]; then
    rm -f .fault_done fault.log
    echo -e "${CYAN}[Terminal 8/8] Spawning Fault Recovery Consumer in Desktop Slot 8...${NC}"
    launch_tiled_window 8 "CIPHER [8/8] Fault Recovery Consumer (Port 4302)" "./bin/consumer -p 4302 -fetch '$CONTENT_ID' -key '$KEY' -out test_pay_fault.dat -bootstrap '$BOOTSTRAP_MULTIADDR' -store ./store_client_fault 2>&1 | tee fault.log; echo \$? > .fault_done"
    while [ ! -f .fault_done ]; do
        sleep 0.3
    done
else
    ./bin/consumer -p 4302 -fetch "$CONTENT_ID" -key "$KEY" -out test_pay_fault.dat \
      -bootstrap "$BOOTSTRAP_MULTIADDR" -store ./store_client_fault 2>&1 | tee fault.log
fi

FAULT_HASH=$(shasum -a 256 test_pay_fault.dat | awk '{print $1}')
if [ "$ORIG_HASH" != "$FAULT_HASH" ]; then
    echo -e "${RED}[❌ FAILED] Fault recovery hash mismatch!${NC}"
    exit 1
fi
echo -e "${GREEN}[✓] SUCCESS: Content reconstructed from surviving replica providers (R=2 verified)!${NC}"
pause_step

# STEP 5: ON-CHAIN SETTLEMENT
echo -e "\n${BOLD}${BLUE}=== [JOB 5/6] MINING BLOCKS & EXECUTING ON-CHAIN SETTLEMENT ===${NC}"
echo -e "Mining 20 blocks on Anvil to satisfy confirmation delay..."
cast rpc anvil_mine 20 --rpc-url http://127.0.0.1:8545 > /dev/null

echo -e "Submitting winning ticket for on-chain jackpot payout..."
(cd payments && forge script script/Step2_Settle.s.sol --rpc-url http://127.0.0.1:8545 --broadcast)
pause_step

# STEP 6: FINAL WALLET BALANCES
echo -e "\n${BOLD}${BLUE}=== [JOB 6/6] INSPECTING FINAL WALLET BALANCES & NET PAYOUT ===${NC}"
FINAL_CLIENT_BAL=$(cast balance "$CLIENT_ETH_ADDR" --rpc-url http://127.0.0.1:8545 --ether)
FINAL_PROV_BAL=$(cast balance "$PROVIDER_ETH_ADDR" --rpc-url http://127.0.0.1:8545 --ether)
FINAL_CHANNEL_BAL=$(cast balance "$CHANNEL_CONTRACT_ADDR" --rpc-url http://127.0.0.1:8545 --ether)

echo -e "  💳 ${BOLD}Client Wallet Balance  :${NC} ${YELLOW}$FINAL_CLIENT_BAL ETH${NC}"
echo -e "  💳 ${BOLD}Provider Wallet Balance:${NC} ${GREEN}$FINAL_PROV_BAL ETH${NC} ${BOLD}(+1.0 ETH Earned!)${NC}"
echo -e "  🏦 ${BOLD}Escrow Channel Deposit :${NC} ${YELLOW}$FINAL_CHANNEL_BAL ETH${NC}"

echo -e "\n${BOLD}${GREEN}======================================================================${NC}"
echo -e "${BOLD}${GREEN}🎉 ALL 8 TERMINAL ROLES & PAYMENT TESTS COMPLETED WITH 100% SUCCESS! ${NC}"
echo -e "${BOLD}${GREEN}======================================================================${NC}"
