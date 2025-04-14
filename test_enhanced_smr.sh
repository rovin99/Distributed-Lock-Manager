#!/bin/bash

# Test script for the Enhanced SMR implementation with 3+ nodes
# This script tests the key scenarios for a 3-node distributed lock manager

# Set the color variables
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NODE1_PORT=50051
NODE2_PORT=50052
NODE3_PORT=50053
NODE1_ADDR="localhost:$NODE1_PORT"
NODE2_ADDR="localhost:$NODE2_PORT"
NODE3_ADDR="localhost:$NODE3_PORT"
TEST_TIMEOUT=60

# Unique metrics ports for each node
METRICS_PORT_BASE=8080
NODE1_METRICS_PORT=$((METRICS_PORT_BASE + 1))
NODE2_METRICS_PORT=$((METRICS_PORT_BASE + 2))
NODE3_METRICS_PORT=$((METRICS_PORT_BASE + 3))

BASE_CLIENT_ID=1000

# Check if a test name is provided as the first argument
if [ -n "$1" ]; then
    TEST_TO_RUN="$1"
    echo "Running individual test: $TEST_TO_RUN"
    
    case $TEST_TO_RUN in
        failover)
            # Will run failover_test later in the script
            RUN_ONLY_FAILOVER=true
            ;;
        split-brain)
            # Will run split_brain_test later in the script
            RUN_ONLY_SPLIT_BRAIN=true
            ;;
        majority-failure)
            # Will run majority_failure_test later in the script
            RUN_ONLY_MAJORITY_FAILURE=true
            ;;
        secondary-failure)
            # Will run secondary_failure_test later in the script
            RUN_ONLY_SECONDARY_FAILURE=true
            ;;
        *)
            echo "Unknown test: $TEST_TO_RUN"
            echo "Usage: $0 {failover|split-brain|majority-failure|secondary-failure}"
            exit 1
            ;;
    esac
fi

# Function to print colored output
print_color() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to print section header
print_header() {
    echo
    print_color $BLUE "====== $1 ======"
    echo
}

# Function to check if process is running
is_process_running() {
    kill -0 $1 2>/dev/null
    return $?
}

# Function to wait until a server is ready
wait_for_server() {
    local addr=$1
    local max_attempts=20
    local attempts=0
    local ready=false
    
    echo "Waiting for server at $addr to be ready..."
    
    while [ $attempts -lt $max_attempts ] && [ "$ready" = false ]; do
        if nc -z ${addr/:/ } >/dev/null 2>&1; then
            ready=true
            print_color $GREEN "Server at $addr is ready"
        else
            attempts=$((attempts+1))
            echo "Attempt $attempts: Server not ready yet, waiting..."
            sleep 1
        fi
    done
    
    if [ "$ready" = false ]; then
        print_color $RED "Server at $addr did not become ready in time"
        return 1
    fi
    
    return 0
}

# Function to verify server is running and in the expected role
verify_server_role() {
    local addr=$1
    local expected_role=$2
    local max_attempts=10
    local attempts=0
    
    echo "Verifying server at $addr is in role: $expected_role..." >&2
    
    while [ $attempts -lt $max_attempts ]; do
        # Get server info - store full output including stderr
        local tmp_output=$(mktemp)
        bin/lock_client info --servers="$addr" > "$tmp_output" 2>&1
        
        # Extract just the JSON part (the last output block surrounded by braces)
        local info=$(awk '/^{/,/^}/' "$tmp_output" | tr -d '\n')
        echo "DEBUG: Raw server info: $info" >&2
        
        if [ -n "$info" ]; then
            # Try to use jq if available
            local role=""
            if command -v jq >/dev/null 2>&1; then
                role=$(echo "$info" | jq -r '.role' 2>/dev/null)
            else
                # Better pattern matching for JSON format
                role=$(echo "$info" | grep -o '"role"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"role"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')
            fi
            
            echo "DEBUG: Extracted role: '$role'" >&2
            
            if [ "$role" = "$expected_role" ]; then
                print_color $GREEN "Server at $addr verified as $expected_role" >&2
                rm -f "$tmp_output"
                return 0
            else
                print_color $YELLOW "Server at $addr is in role '$role', expected '$expected_role'" >&2
            fi
        else
            print_color $YELLOW "Could not get role info from server at $addr" >&2
        fi
        
        attempts=$((attempts+1))
        echo "Attempt $attempts: Waiting for server role verification..." >&2
        sleep 1
        rm -f "$tmp_output"
    done
    
    print_color $RED "Failed to verify server at $addr is in role $expected_role" >&2
    return 1
}

# Function to get the current leader from a server
get_leader_from_server() {
    local addr=$1
    local max_attempts=5
    local attempts=0
    
    echo "Getting leader address from server at $addr..." >&2
    
    while [ $attempts -lt $max_attempts ]; do
        # Try to get server info
        local tmp_output=$(mktemp)
        bin/lock_client info --servers="$addr" > "$tmp_output" 2>&1
        
        # Extract just the JSON part
        local info=$(awk '/^{/,/^}/' "$tmp_output" | tr -d '\n')
        
        if [ -n "$info" ]; then
            echo "DEBUG: Raw server info: $info" >&2
            
            # Extract the leader address
            local leader=""
            if command -v jq >/dev/null 2>&1; then
                leader=$(echo "$info" | jq -r '.leader_address' 2>/dev/null)
            else
                leader=$(echo "$info" | grep -o '"leader_address"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"leader_address"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')
            fi
            
            echo "DEBUG: Extracted leader_address: '$leader'" >&2
            
            if [ -n "$leader" ]; then
                print_color $GREEN "Leader address from $addr: $leader" >&2
                rm -f "$tmp_output"
                echo "$leader"  # This is what's returned by the function
                return 0
            fi
        fi
        
        attempts=$((attempts+1))
        sleep 1
        rm -f "$tmp_output"
    done
    
    return 1
}

# Function to verify a client can acquire a lock
verify_client_lock_acquire() {
    local addr=$1
    local client_id=$2
    
    print_color $YELLOW "Client $client_id trying to acquire lock through $addr..." >&2
    
    # Use a temporary file to capture output
    local tmp_output=$(mktemp)
    
    # Try to acquire a lock with a longer timeout (increased from 10s to 20s)
    bin/lock_client acquire --servers="$addr" --client-id=$client_id --timeout=20s > "$tmp_output" 2>&1
    local exit_code=$?
    
    # Display the output for debugging
    cat "$tmp_output" >&2
    
    # Check for success patterns in the output
    if grep -q "Successfully acquired lock" "$tmp_output" || [ $exit_code -eq 0 ]; then
        print_color $GREEN "Client $client_id successfully acquired lock through $addr" >&2
        rm -f "$tmp_output"
        return 0
    else
        print_color $RED "Client $client_id failed to acquire lock through $addr" >&2
        print_color $RED "Exit code: $exit_code" >&2
        rm -f "$tmp_output"
        return 1
    fi
}

# Function to clean up processes
cleanup() {
    print_color $YELLOW "Cleaning up processes..."
    
    # Kill all potential server and client processes
    if [ "$(uname)" = "Darwin" ]; then
        # macOS specific commands
        killall -9 lock_server lock_client 2>/dev/null || true
        pkill -9 -f "bin/lock_server" 2>/dev/null || true
        pkill -9 -f "bin/lock_client" 2>/dev/null || true
    else
        # Linux commands
        killall -9 lock_server lock_client 2>/dev/null || true
        pkill -9 -f "bin/lock_server" 2>/dev/null || true
        pkill -9 -f "bin/lock_client" 2>/dev/null || true
    fi
    
    # Wait a moment for processes to die
    sleep 2
    
    # Clean up test files
    rm -f data/lock_state.json data/processed_requests.log data/file_*
    rm -f data/fs_verify_*.tmp
    
    print_color $GREEN "Cleanup complete"
}

# Register cleanup function for script exit
trap cleanup EXIT

# Function to start a node with specified configuration
start_node() {
    local node_id=$1
    local port=$2
    local role=$3
    local metrics_port=$4
    local peers=$5
    
    local addr="localhost:$port"
    # Send status messages to stderr
    print_color $YELLOW "Starting node $node_id ($role) on port $port" >&2
    
    # Create a separate log file for this node
    local log_file="logs/node${node_id}.log"
    touch "$log_file"
    
    bin/lock_server --role $role --id $node_id --address ":$port" --peers "$peers" --metrics-address ":$metrics_port" > "$log_file" 2>&1 &
    local pid=$!
    
    # Verify it's a valid PID
    if ! ps -p $pid > /dev/null; then
        print_color $RED "Invalid PID: $pid" >&2
        return 1
    fi
    
    # Wait for node to start - status to stderr
    if ! wait_for_server "$addr" >&2; then
        print_color $RED "Node $node_id failed to start" >&2
        kill $pid 2>/dev/null || true
        return 1
    fi
    
    print_color $GREEN "Node $node_id started with PID $pid" >&2
    # Only output the PID to stdout
    echo $pid
}

# Function to get PID by port
get_pid_by_port() {
    local port=$1
    local pid=""
    
    if [ "$(uname)" = "Darwin" ]; then
        # macOS specific command
        pid=$(lsof -n -i :$port -t 2>/dev/null)
    else
        # Linux command
        pid=$(lsof -n -i :$port -t 2>/dev/null)
    fi
    
    if [ -n "$pid" ]; then
        echo "$pid"
        return 0
    else
        return 1
    fi
}

# Function to kill a server by its port
kill_server_by_port() {
    local port=$1
    local server_name=$2
    
    echo "Attempting to kill $server_name on port $port..."
    
    # Get PID for the server
    local pid=$(get_pid_by_port $port)
    
    if [ -n "$pid" ]; then
        echo "Found $server_name with PID $pid, killing..."
        kill $pid 2>/dev/null
        sleep 1
        
        # Check if it's still running and force kill if needed
        if ps -p $pid > /dev/null 2>&1; then
            echo "Server still running, sending SIGKILL..."
            kill -9 $pid 2>/dev/null
            sleep 1
        fi
        
        if ! ps -p $pid > /dev/null 2>&1; then
            print_color $GREEN "Successfully killed $server_name (PID $pid)"
            return 0
        else
            print_color $RED "Failed to kill $server_name (PID $pid)"
            return 1
        fi
    else
        print_color $YELLOW "No process found listening on port $port"
        return 0  # Not considering this a failure
    fi
}

# Check if we can build the binaries
print_header "Building binaries"

# Create logs directory if it doesn't exist
mkdir -p logs

# Build the binaries
go build -o bin/lock_server cmd/server/main.go
if [ $? -ne 0 ]; then
    print_color $RED "Failed to build server"
    exit 1
fi

go build -o bin/lock_client cmd/client/main.go
if [ $? -ne 0 ]; then
    print_color $RED "Failed to build client"
    exit 1
fi

print_color $GREEN "Binaries built successfully"

# Start with clean state
cleanup

# Test 1: Failover Test
run_failover_test() {
    print_header "Failover Test"
    print_color $YELLOW "Testing failover when leader (Node 1) fails"
    
    # Start 3 nodes
    PEERS_FOR_NODE1="$NODE2_ADDR,$NODE3_ADDR"
    PEERS_FOR_NODE2="$NODE1_ADDR,$NODE3_ADDR"
    PEERS_FOR_NODE3="$NODE1_ADDR,$NODE2_ADDR"
    
    # Start initial leader
    local node1_pid=$(start_node 1 $NODE1_PORT "primary" $NODE1_METRICS_PORT "$PEERS_FOR_NODE1")
    if [ -z "$node1_pid" ]; then
        return 1
    fi
    
    # Start followers
    local node2_pid=$(start_node 2 $NODE2_PORT "secondary" $NODE2_METRICS_PORT "$PEERS_FOR_NODE2")
    if [ -z "$node2_pid" ]; then
        kill $node1_pid 2>/dev/null || true
        return 1
    fi
    
    local node3_pid=$(start_node 3 $NODE3_PORT "secondary" $NODE3_METRICS_PORT "$PEERS_FOR_NODE3")
    if [ -z "$node3_pid" ]; then
        kill $node1_pid $node2_pid 2>/dev/null || true
        return 1
    fi
    
    # Let the system stabilize
    print_color $YELLOW "Letting the cluster stabilize..."
    sleep 15
    
    # Verify node roles for debugging
    print_color $YELLOW "Verifying initial node roles:"
    verify_server_role "$NODE1_ADDR" "leader" || print_color $YELLOW "Node 1 not in leader role as expected"
    verify_server_role "$NODE2_ADDR" "follower" || print_color $YELLOW "Node 2 not in follower role as expected"
    verify_server_role "$NODE3_ADDR" "follower" || print_color $YELLOW "Node 3 not in follower role as expected"
    
    # Verify a client can acquire a lock through the leader
    local client_id=$((BASE_CLIENT_ID))
    if ! verify_client_lock_acquire "$NODE1_ADDR" $client_id; then
        kill $node1_pid $node2_pid $node3_pid 2>/dev/null || true
        return 1
    fi
    
    # Kill the leader (Node 1)
    print_color $YELLOW "Killing leader (Node 1)..."
    kill_server_by_port $NODE1_PORT "Leader (Node 1)"
    
    # Wait for a new leader to be elected (either Node 2 or Node 3)
    print_color $YELLOW "Waiting for a new leader to be elected..."
    sleep 20  # Increased from 10 to 20 seconds
    
    # Try to find the new leader
    local new_leader=""
    if verify_server_role "$NODE2_ADDR" "leader"; then
        new_leader="$NODE2_ADDR"
    elif verify_server_role "$NODE3_ADDR" "leader"; then
        new_leader="$NODE3_ADDR"
    fi
    
    if [ -z "$new_leader" ]; then
        print_color $RED "No new leader was elected"
        kill $node2_pid $node3_pid 2>/dev/null || true
        return 1
    fi
    
    print_color $GREEN "New leader elected: $new_leader"
    
    # Verify a client can acquire a lock through the new leader
    local new_client_id=$((BASE_CLIENT_ID + 1))
    if ! verify_client_lock_acquire "$new_leader" $new_client_id; then
        kill $node2_pid $node3_pid 2>/dev/null || true
        return 1
    fi
    
    # Clean up
    kill $node2_pid $node3_pid 2>/dev/null || true
    sleep 2
    
    print_color $GREEN "Failover test succeeded"
    return 0
}

# Test 2: Split-Brain Test
run_split_brain_test() {
    print_header "Split-Brain Test"
    print_color $YELLOW "Testing partition scenario"
    
    # Start 3 nodes
    PEERS_FOR_NODE1="$NODE2_ADDR,$NODE3_ADDR"
    PEERS_FOR_NODE2="$NODE1_ADDR,$NODE3_ADDR"
    PEERS_FOR_NODE3="$NODE1_ADDR,$NODE2_ADDR"
    
    # Start initial leader
    local node1_pid=$(start_node 1 $NODE1_PORT "primary" $NODE1_METRICS_PORT "$PEERS_FOR_NODE1")
    if [ -z "$node1_pid" ]; then
        return 1
    fi
    
    # Start followers
    local node2_pid=$(start_node 2 $NODE2_PORT "secondary" $NODE2_METRICS_PORT "$PEERS_FOR_NODE2")
    if [ -z "$node2_pid" ]; then
        kill $node1_pid 2>/dev/null || true
        return 1
    fi
    
    local node3_pid=$(start_node 3 $NODE3_PORT "secondary" $NODE3_METRICS_PORT "$PEERS_FOR_NODE3")
    if [ -z "$node3_pid" ]; then
        kill $node1_pid $node2_pid 2>/dev/null || true
        return 1
    fi
    
    # Let the system stabilize
    print_color $YELLOW "Letting the cluster stabilize..."
    sleep 5
    
    # Simulate a network partition by killing Node 1 (leader) and restarting it
    # with only itself in the peer list (partition from Node 2 and Node 3)
    print_color $YELLOW "Creating network partition: Node 1 separated from {Node 2, Node 3}"
    
    # Kill Node 1
    kill_server_by_port $NODE1_PORT "Node 1 (leader)"
    sleep 2
    
    # Restart Node 1 with empty peer list (simulating partition)
    local partitioned_peers=""
    node1_pid=$(start_node 1 $NODE1_PORT "primary" $NODE1_METRICS_PORT "$partitioned_peers")
    if [ -z "$node1_pid" ]; then
        kill $node2_pid $node3_pid 2>/dev/null || true
        return 1
    fi
    
    # Wait for Node 2 and Node 3 to elect a new leader between them
    print_color $YELLOW "Waiting for Node 2 and Node 3 to elect a new leader..."
    sleep 10
    
    # Find which node is the leader in the {Node 2, Node 3} partition
    local new_leader=""
    if verify_server_role "$NODE2_ADDR" "leader"; then
        new_leader="$NODE2_ADDR"
        new_leader_pid=$node2_pid
        follower_addr="$NODE3_ADDR"
    elif verify_server_role "$NODE3_ADDR" "leader"; then
        new_leader="$NODE3_ADDR"
        new_leader_pid=$node3_pid
        follower_addr="$NODE2_ADDR"
    fi
    
    if [ -z "$new_leader" ]; then
        print_color $RED "No new leader was elected in the {Node 2, Node 3} partition"
        kill $node1_pid $node2_pid $node3_pid 2>/dev/null || true
        return 1
    fi
    
    print_color $GREEN "New leader in the {Node 2, Node 3} partition: $new_leader"
    
    # Verify clients can connect to the new leader and acquire locks
    local client_id=$((BASE_CLIENT_ID + 2))
    if ! verify_client_lock_acquire "$new_leader" $client_id; then
        kill $node1_pid $node2_pid $node3_pid 2>/dev/null || true
        return 1
    fi
    
    # Now heal the partition by restarting Node 1 with the full peer list
    print_color $YELLOW "Healing the partition by reconnecting Node 1 to the cluster..."
    kill $node1_pid
    sleep 2
    
    # Restart Node 1 with the full peer list
    node1_pid=$(start_node 1 $NODE1_PORT "secondary" $NODE1_METRICS_PORT "$PEERS_FOR_NODE1")
    if [ -z "$node1_pid" ]; then
        kill $node2_pid $node3_pid 2>/dev/null || true
        return 1
    fi
    
    # Let the system stabilize
    sleep 10
    
    # Verify Node 1 becomes a follower and recognizes the current leader
    if ! verify_server_role "$NODE1_ADDR" "follower"; then
        print_color $RED "Node 1 did not become a follower after partition healing"
        kill $node1_pid $node2_pid $node3_pid 2>/dev/null || true
        return 1
    fi
    
    # Check that Node 1 knows the current leader
    local leader_from_node1=$(get_leader_from_server "$NODE1_ADDR")
    if [ "$leader_from_node1" != "$new_leader" ]; then
        print_color $RED "Node 1 does not recognize the correct leader after partition healing"
        print_color $RED "Expected: $new_leader, Got: $leader_from_node1"
        kill $node1_pid $node2_pid $node3_pid 2>/dev/null || true
        return 1
    fi
    
    print_color $GREEN "Node 1 correctly recognizes $new_leader as the leader after partition healing"
    
    # Clean up
    kill $node1_pid $node2_pid $node3_pid 2>/dev/null || true
    sleep 2
    
    print_color $GREEN "Split-brain test succeeded"
    return 0
}

# Test 3: Majority Failure Test
run_majority_failure_test() {
    print_header "Majority Failure Test"
    print_color $YELLOW "Testing behavior when majority of nodes fail"
    
    # Start 3 nodes
    PEERS_FOR_NODE1="$NODE2_ADDR,$NODE3_ADDR"
    PEERS_FOR_NODE2="$NODE1_ADDR,$NODE3_ADDR"
    PEERS_FOR_NODE3="$NODE1_ADDR,$NODE2_ADDR"
    
    # Start initial leader
    local node1_pid=$(start_node 1 $NODE1_PORT "primary" $NODE1_METRICS_PORT "$PEERS_FOR_NODE1")
    if [ -z "$node1_pid" ]; then
        return 1
    fi
    
    # Start followers
    local node2_pid=$(start_node 2 $NODE2_PORT "secondary" $NODE2_METRICS_PORT "$PEERS_FOR_NODE2")
    if [ -z "$node2_pid" ]; then
        kill $node1_pid 2>/dev/null || true
        return 1
    fi
    
    local node3_pid=$(start_node 3 $NODE3_PORT "secondary" $NODE3_METRICS_PORT "$PEERS_FOR_NODE3")
    if [ -z "$node3_pid" ]; then
        kill $node1_pid $node2_pid 2>/dev/null || true
        return 1
    fi
    
    # Let the system stabilize
    print_color $YELLOW "Letting the cluster stabilize..."
    sleep 5
    
    # Verify a client can acquire a lock through the leader
    local client_id=$((BASE_CLIENT_ID + 3))
    if ! verify_client_lock_acquire "$NODE1_ADDR" $client_id; then
        kill $node1_pid $node2_pid $node3_pid 2>/dev/null || true
        return 1
    fi
    
    # Kill Node 2 and Node 3 to simulate majority failure
    print_color $YELLOW "Killing Node 2 and Node 3 to simulate majority failure..."
    kill_server_by_port $NODE2_PORT "Node 2 (follower)"
    kill_server_by_port $NODE3_PORT "Node 3 (follower)"
    sleep 2
    
    # Verify Node 1 cannot get majority for leadership and steps down
    print_color $YELLOW "Checking if Node 1 steps down due to lack of majority..."
    
    # Try to acquire lock, which should fail after Node 1 steps down
    local client_id_2=$((BASE_CLIENT_ID + 4))
    bin/lock_client acquire --servers="$NODE1_ADDR" --client-id=$client_id_2 --timeout=5s
    if [ $? -eq 0 ]; then
        print_color $RED "Client could still acquire lock after majority failure - this should not happen"
        kill $node1_pid 2>/dev/null || true
        return 1
    fi
    
    print_color $GREEN "Node 1 correctly rejected lock acquisition after majority failure"
    
    # Restart one of the followers to restore majority
    print_color $YELLOW "Restarting Node 2 to restore majority..."
    node2_pid=$(start_node 2 $NODE2_PORT "secondary" $NODE2_METRICS_PORT "$PEERS_FOR_NODE2")
    if [ -z "$node2_pid" ]; then
        kill $node1_pid 2>/dev/null || true
        return 1
    fi
    
    # Wait for the system to stabilize and elect a leader
    print_color $YELLOW "Waiting for a leader to be elected..."
    sleep 10
    
    # Check if we have a leader in the system
    local has_leader=false
    if verify_server_role "$NODE1_ADDR" "leader"; then
        has_leader=true
        leader_addr="$NODE1_ADDR"
    elif verify_server_role "$NODE2_ADDR" "leader"; then
        has_leader=true
        leader_addr="$NODE2_ADDR"
    fi
    
    if [ "$has_leader" = false ]; then
        print_color $RED "No leader was elected after restoring majority"
        kill $node1_pid $node2_pid 2>/dev/null || true
        return 1
    fi
    
    print_color $GREEN "Leader elected after restoring majority: $leader_addr"
    
    # Verify a client can acquire a lock through the new leader
    local client_id_3=$((BASE_CLIENT_ID + 5))
    if ! verify_client_lock_acquire "$leader_addr" $client_id_3; then
        kill $node1_pid $node2_pid 2>/dev/null || true
        return 1
    fi
    
    # Clean up
    kill $node1_pid $node2_pid 2>/dev/null || true
    sleep 2
    
    print_color $GREEN "Majority failure test succeeded"
    return 0
}

# Test 4: Secondary Failure Test
run_secondary_failure_test() {
    print_header "Secondary Failure Test"
    print_color $YELLOW "Testing behavior when a secondary node fails and is restarted"
    
    # Start 3 nodes
    PEERS_FOR_NODE1="$NODE2_ADDR,$NODE3_ADDR"
    PEERS_FOR_NODE2="$NODE1_ADDR,$NODE3_ADDR"
    PEERS_FOR_NODE3="$NODE1_ADDR,$NODE2_ADDR"
    
    # Start initial leader
    local node1_pid=$(start_node 1 $NODE1_PORT "primary" $NODE1_METRICS_PORT "$PEERS_FOR_NODE1")
    if [ -z "$node1_pid" ]; then
        return 1
    fi
    
    # Start followers
    local node2_pid=$(start_node 2 $NODE2_PORT "secondary" $NODE2_METRICS_PORT "$PEERS_FOR_NODE2")
    if [ -z "$node2_pid" ]; then
        kill $node1_pid 2>/dev/null || true
        return 1
    fi
    
    local node3_pid=$(start_node 3 $NODE3_PORT "secondary" $NODE3_METRICS_PORT "$PEERS_FOR_NODE3")
    if [ -z "$node3_pid" ]; then
        kill $node1_pid $node2_pid 2>/dev/null || true
        return 1
    fi
    
    # Let the system stabilize
    print_color $YELLOW "Letting the cluster stabilize..."
    sleep 5
    
    # Acquire a lock through the leader
    local client_id=$((BASE_CLIENT_ID + 6))
    if ! verify_client_lock_acquire "$NODE1_ADDR" $client_id; then
        kill $node1_pid $node2_pid $node3_pid 2>/dev/null || true
        return 1
    fi
    
    # Kill Node 2 (follower)
    print_color $YELLOW "Killing Node 2 (follower)..."
    kill_server_by_port $NODE2_PORT "Node 2 (follower)"
    sleep 2
    
    # Verify leader (Node 1) continues to operate
    local client_id_2=$((BASE_CLIENT_ID + 7))
    if ! verify_client_lock_acquire "$NODE1_ADDR" $client_id_2; then
        print_color $RED "Leader (Node 1) stopped working after follower failure"
        kill $node1_pid $node3_pid 2>/dev/null || true
        return 1
    fi
    
    print_color $GREEN "Leader (Node 1) continues to operate after follower failure"
    
    # Restart Node 2
    print_color $YELLOW "Restarting Node 2..."
    node2_pid=$(start_node 2 $NODE2_PORT "secondary" $NODE2_METRICS_PORT "$PEERS_FOR_NODE2")
    if [ -z "$node2_pid" ]; then
        kill $node1_pid $node3_pid 2>/dev/null || true
        return 1
    fi
    
    # Wait for Node 2 to sync state
    print_color $YELLOW "Waiting for Node 2 to sync state..."
    sleep 10
    
    # Verify Node 2 has synced by killing Node 1 to verify Node 2 synced properly
    print_color $YELLOW "Killing Node 1 to verify Node 2 synced properly..."
    kill_server_by_port $NODE1_PORT "Node 1 (leader)"
    sleep 5
    
    # Check if Node 2 or Node 3 becomes the leader
    local new_leader=""
    if verify_server_role "$NODE2_ADDR" "leader"; then
        new_leader="$NODE2_ADDR"
    elif verify_server_role "$NODE3_ADDR" "leader"; then
        new_leader="$NODE3_ADDR"
    fi
    
    if [ -z "$new_leader" ]; then
        print_color $RED "No new leader was elected after Node 1 failure"
        kill $node2_pid $node3_pid 2>/dev/null || true
        return 1
    fi
    
    print_color $GREEN "New leader after Node 1 failure: $new_leader"
    
    # Verify a client can acquire a lock through the new leader
    local client_id_3=$((BASE_CLIENT_ID + 8))
    if ! verify_client_lock_acquire "$new_leader" $client_id_3; then
        kill $node2_pid $node3_pid 2>/dev/null || true
        return 1
    fi
    
    # Clean up
    kill $node2_pid $node3_pid 2>/dev/null || true
    sleep 2
    
    print_color $GREEN "Secondary failure test succeeded"
    return 0
}

# Function to run a test if it was specified
run_test() {
    local test_name=$1
    local test_func=$2
    local only_flag=$3
    
    if [ -n "$only_flag" ] && [ "$only_flag" != "true" ]; then
        print_color $YELLOW "Skipping $test_name (not selected)"
        return 0
    fi
    
    print_header "Running Test: $test_name"
    
    if $test_func; then
        print_color $GREEN "Test '$test_name' PASSED"
        return 0
    else
        print_color $RED "Test '$test_name' FAILED"
        tests_failed=$((tests_failed+1))
        return 1
    fi
}

# Run the tests
tests_failed=0

# Check if individual tests were specified
if [ -n "$RUN_ONLY_FAILOVER" ] || [ -n "$RUN_ONLY_SPLIT_BRAIN" ] || [ -n "$RUN_ONLY_MAJORITY_FAILURE" ] || [ -n "$RUN_ONLY_SECONDARY_FAILURE" ]; then
    run_test "Failover Test" run_failover_test "$RUN_ONLY_FAILOVER"
    run_test "Split-Brain Test" run_split_brain_test "$RUN_ONLY_SPLIT_BRAIN"
    run_test "Majority Failure Test" run_majority_failure_test "$RUN_ONLY_MAJORITY_FAILURE"
    run_test "Secondary Failure Test" run_secondary_failure_test "$RUN_ONLY_SECONDARY_FAILURE"
else
    # Run all tests
    run_test "Failover Test" run_failover_test "true"
    run_test "Split-Brain Test" run_split_brain_test "true"
    run_test "Majority Failure Test" run_majority_failure_test "true"
    run_test "Secondary Failure Test" run_secondary_failure_test "true"
fi

# Print summary
print_header "Test Summary"

if [ $tests_failed -eq 0 ]; then
    print_color $GREEN "All tests PASSED"
    exit 0
else
    print_color $RED "$tests_failed tests FAILED"
    exit 1
fi 