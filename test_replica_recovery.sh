#!/bin/bash

# Test script for Replica Node Failure with Fast Recovery
# This tests the scenario where a replica node fails and recovers quickly during file operations

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

# Create logs directory if it doesn't exist
mkdir -p logs

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

# Function to verify if port is open
wait_for_port() {
    local port=$1
    local max_attempts=20
    local attempts=0
    
    print_color $YELLOW "Waiting for port $port to be open..."
    
    while [ $attempts -lt $max_attempts ]; do
        if nc -z localhost $port 2>/dev/null; then
            print_color $GREEN "Port $port is open"
            return 0
        fi
        
        attempts=$((attempts+1))
        echo "Attempt $attempts: Port not open yet, waiting..."
        sleep 1
    done
    
    print_color $RED "Port $port did not open in time"
    return 1
}

# Function to clean up processes
cleanup() {
    print_color $YELLOW "Cleaning up processes..."
    
    # Kill all server and client processes
    pkill -9 -f "bin/lock_server" 2>/dev/null || true
    pkill -9 -f "bin/lock_client" 2>/dev/null || true
    
    # Remove data files
    rm -f data/file_1 data/file_2 data/file_3 2>/dev/null || true
    
    sleep 2
    print_color $GREEN "Cleanup complete"
}

# Register cleanup on exit
trap cleanup EXIT

# Function to get the current leader address
get_leader_address() {
    local potential_leaders=("$NODE1_ADDR" "$NODE2_ADDR" "$NODE3_ADDR")
    
    for addr in "${potential_leaders[@]}"; do
        print_color $YELLOW "Checking if $addr is leader..." >&2
        local info=$(bin/lock_client info --servers="$addr" 2>/dev/null)
        
        if echo "$info" | grep -q '"role"[[:space:]]*:[[:space:]]*"leader"'; then
            print_color $GREEN "Found leader at $addr" >&2
            echo "$addr"
            return 0
        fi
    done
    
    print_color $RED "Failed to find a leader" >&2
    return 1
}

# Function to wait for a stable leader
wait_for_stable_leader() {
    print_color $YELLOW "Waiting for a stable leader..." >&2
    local max_attempts=30
    local attempts=0
    local stable_count=0
    local last_leader=""
    
    while [ $attempts -lt $max_attempts ]; do
        local leader=$(get_leader_address)
        
        if [ $? -ne 0 ]; then
            print_color $YELLOW "No leader found, waiting..." >&2
            stable_count=0
            sleep 2
            attempts=$((attempts+1))
            continue
        fi
        
        if [ "$leader" = "$last_leader" ]; then
            stable_count=$((stable_count+1))
            print_color $YELLOW "Leader at $leader stable for $stable_count checks" >&2
            
            # If we have the same leader for 3 consecutive checks, consider it stable
            if [ $stable_count -ge 3 ]; then
                print_color $GREEN "Leader at $leader is stable" >&2
                echo "$leader"
                return 0
            fi
        else
            print_color $YELLOW "Leader changed from $last_leader to $leader" >&2
            stable_count=1
            last_leader="$leader"
        fi
        
        sleep 2
        attempts=$((attempts+1))
    done
    
    print_color $RED "Failed to find a stable leader after $max_attempts attempts" >&2
    return 1
}

# Main test function
test_replica_failure_recovery() {
    print_header "Replica Node Failure with Fast Recovery Test"
    
    # Step 1: Start 3 servers
    print_color $YELLOW "Starting 3 servers..."
    
    # Start Server 1 (Primary)
    bin/lock_server --role=primary --id=1 --address=":$NODE1_PORT" --peers="$NODE2_ADDR,$NODE3_ADDR" --skip-verifications > logs/server1.log 2>&1 &
    SERVER1_PID=$!
    
    # Wait for server to be ready
    if ! wait_for_port $NODE1_PORT; then
        print_color $RED "Server 1 failed to start"
        return 1
    fi
    
    # Start Server 2 (Replica)
    bin/lock_server --role=secondary --id=2 --address=":$NODE2_PORT" --peers="$NODE1_ADDR,$NODE3_ADDR" --skip-verifications > logs/server2.log 2>&1 &
    SERVER2_PID=$!
    
    # Wait for server to be ready
    if ! wait_for_port $NODE2_PORT; then
        print_color $RED "Server 2 failed to start"
        return 1
    fi
    
    # Start Server 3 (Replica)
    bin/lock_server --role=secondary --id=3 --address=":$NODE3_PORT" --peers="$NODE1_ADDR,$NODE2_ADDR" --skip-verifications > logs/server3.log 2>&1 &
    SERVER3_PID=$!
    
    # Wait for server to be ready
    if ! wait_for_port $NODE3_PORT; then
        print_color $RED "Server 3 failed to start"
        return 1
    fi
    
    # Let the system stabilize and find a stable leader
    print_color $YELLOW "Letting the cluster stabilize..."
    sleep 10
    
    LEADER_ADDR=$(wait_for_stable_leader)
    if [ $? -ne 0 ]; then
        print_color $RED "Failed to find a stable leader, aborting test"
        return 1
    fi
    
    # Step 2: Client 1 acquires lock and appends to file_1
    print_color $YELLOW "Client 1 acquiring lock through $LEADER_ADDR..."
    bin/lock_client acquire --servers="$LEADER_ADDR" --client-id=1
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 failed to acquire lock"
        return 1
    fi
    
    print_color $YELLOW "Client 1 appending 'A' to file_1..."
    bin/lock_client append --servers="$LEADER_ADDR" --client-id=1 --file="file_1" --content="A"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 failed to append to file_1"
        return 1
    fi
    
    # Step 3: Client 1 appends to file_2
    print_color $YELLOW "Client 1 appending 'A' to file_2..."
    bin/lock_client append --servers="$LEADER_ADDR" --client-id=1 --file="file_2" --content="A"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 failed to append to file_2"
        return 1
    fi
    
    # Step 4: Kill Server 2
    print_color $YELLOW "Killing Server 2 (replica)..."
    kill $SERVER2_PID
    sleep 5
    
    # Verify leader is still available after Server 2 failure
    LEADER_ADDR=$(get_leader_address)
    if [ $? -ne 0 ]; then
        print_color $RED "Lost leader after killing Server 2"
        return 1
    fi
    
    # Step 5: Client 1 appends to file_3
    print_color $YELLOW "Client 1 appending 'A' to file_3 with Server 2 down through $LEADER_ADDR..."
    bin/lock_client append --servers="$LEADER_ADDR" --client-id=1 --file="file_3" --content="A"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 failed to append to file_3 with Server 2 down"
        return 1
    fi
    
    # Step 6: Restart Server 2
    print_color $YELLOW "Restarting Server 2..."
    bin/lock_server --role=secondary --id=2 --address=":$NODE2_PORT" --peers="$NODE1_ADDR,$NODE3_ADDR" --skip-verifications > logs/server2.log 2>&1 &
    SERVER2_PID=$!
    
    # Wait for server to be ready
    if ! wait_for_port $NODE2_PORT; then
        print_color $RED "Failed to restart Server 2"
        return 1
    fi
    
    # Let Server 2 sync state
    print_color $YELLOW "Letting Server 2 sync state..."
    sleep 10
    
    # Re-verify the leader after Server 2 restart
    LEADER_ADDR=$(get_leader_address)
    if [ $? -ne 0 ]; then
        print_color $RED "Lost leader after restarting Server 2"
        return 1
    fi
    
    # Step 7: Client 1 releases lock
    print_color $YELLOW "Client 1 releasing lock through $LEADER_ADDR..."
    bin/lock_client release --servers="$LEADER_ADDR" --client-id=1
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 failed to release lock"
        return 1
    fi
    
    # Step 8: Client 2 acquires lock
    print_color $YELLOW "Client 2 acquiring lock through $LEADER_ADDR..."
    bin/lock_client acquire --servers="$LEADER_ADDR" --client-id=2
    if [ $? -ne 0 ]; then
        print_color $RED "Client 2 failed to acquire lock"
        return 1
    fi
    
    # Step 9-11: Client 2 appends to all three files
    print_color $YELLOW "Client 2 appending 'B' to file_1..."
    bin/lock_client append --servers="$LEADER_ADDR" --client-id=2 --file="file_1" --content="B"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 2 failed to append to file_1"
        return 1
    fi
    
    print_color $YELLOW "Client 2 appending 'B' to file_2..."
    bin/lock_client append --servers="$LEADER_ADDR" --client-id=2 --file="file_2" --content="B"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 2 failed to append to file_2"
        return 1
    fi
    
    print_color $YELLOW "Client 2 appending 'B' to file_3..."
    bin/lock_client append --servers="$LEADER_ADDR" --client-id=2 --file="file_3" --content="B"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 2 failed to append to file_3"
        return 1
    fi
    
    # Step 12: Client 2 releases lock
    print_color $YELLOW "Client 2 releasing lock through $LEADER_ADDR..."
    bin/lock_client release --servers="$LEADER_ADDR" --client-id=2
    if [ $? -ne 0 ]; then
        print_color $RED "Client 2 failed to release lock"
        return 1
    fi
    
    # Step 13: Verify file contents
    print_color $YELLOW "Verifying file contents..."
    
    # Check file_1
    local content=$(cat data/file_1)
    print_color $YELLOW "File 1 content: $content"
    if [ "$content" != "AB" ]; then
        print_color $RED "File 1 content mismatch. Expected: AB, Got: $content"
        return 1
    fi
    
    # Check file_2
    content=$(cat data/file_2)
    print_color $YELLOW "File 2 content: $content"
    if [ "$content" != "AB" ]; then
        print_color $RED "File 2 content mismatch. Expected: AB, Got: $content"
        return 1
    fi
    
    # Check file_3
    content=$(cat data/file_3)
    print_color $YELLOW "File 3 content: $content"
    if [ "$content" != "AB" ]; then
        print_color $RED "File 3 content mismatch. Expected: AB, Got: $content"
        return 1
    fi
    
    print_color $GREEN "All files have correct content (AB)"
    print_color $GREEN "Replica Node Failure with Fast Recovery test succeeded"
    return 0
}

# Run the test
cleanup
test_replica_failure_recovery
if [ $? -eq 0 ]; then
    print_color $GREEN "TEST PASSED"
    exit 0
else
    print_color $RED "TEST FAILED"
    exit 1
fi 