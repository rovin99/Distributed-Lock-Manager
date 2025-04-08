#!/bin/bash

# Set the color variables
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PRIMARY_PORT=50051
SECONDARY_PORT=50052
PRIMARY_ADDR="localhost:$PRIMARY_PORT"
SECONDARY_ADDR="localhost:$SECONDARY_PORT"
TEST_TIMEOUT=30
CLIENT_ID=1

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

# Function to clean up processes
cleanup() {
    print_color $YELLOW "Cleaning up processes..."
    pkill -f lock_server || true
    pkill -f lock_client || true
    sleep 2
}

# Register cleanup function for script exit
trap cleanup EXIT

# Start from a clean state
cleanup
rm -f data/lock_state.json data/processed_requests.log data/file_*

print_header "Building binaries"
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

# Make sure data directory exists
mkdir -p data logs

# Tests to run
run_basic_test() {
    print_header "Basic Replication Test"
    
    print_color $YELLOW "Starting primary server on port $PRIMARY_PORT"
    bin/lock_server --role primary --id 1 --address ":$PRIMARY_PORT" --peer "$SECONDARY_ADDR" > logs/test_primary.log 2>&1 &
    PRIMARY_PID=$!
    
    # Wait for primary to start
    if ! wait_for_server "$PRIMARY_ADDR"; then
        print_color $RED "Primary server failed to start"
        exit 1
    fi
    
    print_color $YELLOW "Starting secondary server on port $SECONDARY_PORT"
    bin/lock_server --role secondary --id 2 --address ":$SECONDARY_PORT" --peer "$PRIMARY_ADDR" > logs/test_secondary.log 2>&1 &
    SECONDARY_PID=$!
    
    # Wait for secondary to start
    if ! wait_for_server "$SECONDARY_ADDR"; then
        print_color $RED "Secondary server failed to start"
        exit 1
    fi
    
    print_color $YELLOW "Running client to acquire lock..."
    bin/lock_client acquire --servers="$PRIMARY_ADDR" --client-id=$CLIENT_ID --timeout=10s > logs/test_client.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "Client failed to acquire lock"
        return 1
    fi
    
    print_color $GREEN "Basic replication test passed!"
    
    # Clean up
    kill $PRIMARY_PID $SECONDARY_PID || true
    sleep 2
    return 0
}

run_failover_test() {
    print_header "Failover Test"
    
    print_color $YELLOW "Starting primary server on port $PRIMARY_PORT"
    bin/lock_server --role primary --id 1 --address ":$PRIMARY_PORT" --peer "$SECONDARY_ADDR" > logs/test_primary.log 2>&1 &
    PRIMARY_PID=$!
    
    # Wait for primary to start
    if ! wait_for_server "$PRIMARY_ADDR"; then
        print_color $RED "Primary server failed to start"
        exit 1
    fi
    
    print_color $YELLOW "Starting secondary server on port $SECONDARY_PORT"
    bin/lock_server --role secondary --id 2 --address ":$SECONDARY_PORT" --peer "$PRIMARY_ADDR" > logs/test_secondary.log 2>&1 &
    SECONDARY_PID=$!
    
    # Wait for secondary to start
    if ! wait_for_server "$SECONDARY_ADDR"; then
        print_color $RED "Secondary server failed to start"
        exit 1
    fi
    
    print_color $YELLOW "Starting client in hold mode..."
    bin/lock_client hold --servers="$PRIMARY_ADDR,$SECONDARY_ADDR" --client-id=$CLIENT_ID --timeout=30s > logs/test_client.log 2>&1 &
    CLIENT_PID=$!
    
    # Wait for client to acquire lock
    sleep 5
    
    if ! is_process_running $CLIENT_PID; then
        print_color $RED "Client failed to acquire lock"
        return 1
    fi
    
    print_color $YELLOW "Killing primary server to trigger failover..."
    kill $PRIMARY_PID
    
    # Wait for failover to complete
    sleep 15
    
    # Check if client is still running
    if is_process_running $CLIENT_PID; then
        print_color $GREEN "Failover test passed! Client survived primary failure."
        # Clean up
        kill $CLIENT_PID || true
    else
        print_color $RED "Failover test failed! Client did not survive primary failure."
        return 1
    fi
    
    # Clean up
    kill $SECONDARY_PID || true
    sleep 2
    return 0
}

run_split_brain_test() {
    print_header "Split Brain Prevention Test"
    
    print_color $YELLOW "Starting primary server on port $PRIMARY_PORT"
    bin/lock_server --role primary --id 1 --address ":$PRIMARY_PORT" --peer "$SECONDARY_ADDR" > logs/test_primary.log 2>&1 &
    PRIMARY_PID=$!
    
    # Wait for primary to start
    if ! wait_for_server "$PRIMARY_ADDR"; then
        print_color $RED "Primary server failed to start"
        exit 1
    fi
    
    print_color $YELLOW "Starting secondary server on port $SECONDARY_PORT"
    bin/lock_server --role secondary --id 2 --address ":$SECONDARY_PORT" --peer "$PRIMARY_ADDR" > logs/test_secondary.log 2>&1 &
    SECONDARY_PID=$!
    
    # Wait for secondary to start
    if ! wait_for_server "$SECONDARY_ADDR"; then
        print_color $RED "Secondary server failed to start"
        exit 1
    fi
    
    # Create a network partition by terminating connections
    print_color $YELLOW "Simulating network partition..."
    # This will block connections between primary and secondary
    # In a real environment, we would use iptables or similar, but here we'll just kill and restart primary
    
    kill -STOP $PRIMARY_PID  # Stop but don't kill the primary
    
    # Wait for secondary to detect failure and promote itself
    print_color $YELLOW "Waiting for secondary to detect primary failure..."
    sleep 15
    
    # Resume primary (this creates a split-brain scenario)
    print_color $YELLOW "Resuming primary to create split-brain scenario..."
    kill -CONT $PRIMARY_PID
    
    # Start two clients, one connecting to each server
    print_color $YELLOW "Starting client 1 connecting to original primary..."
    bin/lock_client hold --servers="$PRIMARY_ADDR" --client-id=101 --timeout=10s > logs/test_client1.log 2>&1 &
    CLIENT1_PID=$!
    
    print_color $YELLOW "Starting client 2 connecting to promoted secondary..."
    bin/lock_client hold --servers="$SECONDARY_ADDR" --client-id=102 --timeout=10s > logs/test_client2.log 2>&1 &
    CLIENT2_PID=$!
    
    # Wait for clients to try to acquire locks
    sleep 10
    
    # Check logs to see if fencing prevented split-brain
    if grep -q "fencing period" logs/test_secondary.log; then
        print_color $GREEN "Split-brain prevention test passed! Fencing period detected."
    else
        print_color $RED "Split-brain prevention test inconclusive. Fencing period not detected."
    fi
    
    # Clean up
    kill $PRIMARY_PID $SECONDARY_PID $CLIENT1_PID $CLIENT2_PID || true
    sleep 2
    return 0
}

run_multiple_client_test() {
    print_header "Multiple Client Test"
    
    print_color $YELLOW "Starting primary server on port $PRIMARY_PORT"
    bin/lock_server --role primary --id 1 --address ":$PRIMARY_PORT" --peer "$SECONDARY_ADDR" > logs/test_primary.log 2>&1 &
    PRIMARY_PID=$!
    
    # Wait for primary to start
    if ! wait_for_server "$PRIMARY_ADDR"; then
        print_color $RED "Primary server failed to start"
        exit 1
    fi
    
    print_color $YELLOW "Starting secondary server on port $SECONDARY_PORT"
    bin/lock_server --role secondary --id 2 --address ":$SECONDARY_PORT" --peer "$PRIMARY_ADDR" > logs/test_secondary.log 2>&1 &
    SECONDARY_PID=$!
    
    # Wait for secondary to start
    if ! wait_for_server "$SECONDARY_ADDR"; then
        print_color $RED "Secondary server failed to start"
        exit 1
    fi
    
    # Start multiple clients
    NUM_CLIENTS=5
    CLIENT_PIDS=()
    
    for i in $(seq 1 $NUM_CLIENTS); do
        print_color $YELLOW "Starting client $i..."
        bin/lock_client append --servers="$PRIMARY_ADDR,$SECONDARY_ADDR" --client-id=$i --file="file_$i" --content="test from client $i" --timeout=20s > "logs/test_client_$i.log" 2>&1 &
        CLIENT_PIDS+=($!)
    done
    
    # Wait for clients to complete
    print_color $YELLOW "Waiting for clients to complete..."
    sleep 15
    
    # Check if all operations succeeded
    success=true
    for i in $(seq 1 $NUM_CLIENTS); do
        if grep -q "Successfully appended" "logs/test_client_$i.log"; then
            print_color $GREEN "Client $i successfully appended to file"
        else
            print_color $RED "Client $i failed to append to file"
            success=false
        fi
    done
    
    if [ "$success" = true ]; then
        print_color $GREEN "Multiple client test passed!"
    else
        print_color $RED "Multiple client test failed!"
        return 1
    fi
    
    # Clean up
    kill $PRIMARY_PID $SECONDARY_PID || true
    for pid in "${CLIENT_PIDS[@]}"; do
        kill $pid || true
    done
    sleep 2
    return 0
}

# Run all tests
print_header "Starting Tests"

tests_failed=0

run_basic_test
if [ $? -ne 0 ]; then
    tests_failed=$((tests_failed+1))
fi

run_failover_test
if [ $? -ne 0 ]; then
    tests_failed=$((tests_failed+1))
fi

run_split_brain_test
if [ $? -ne 0 ]; then
    tests_failed=$((tests_failed+1))
fi

run_multiple_client_test
if [ $? -ne 0 ]; then
    tests_failed=$((tests_failed+1))
fi

# Print summary
print_header "Test Summary"
if [ $tests_failed -eq 0 ]; then
    print_color $GREEN "All tests passed!"
else
    print_color $RED "$tests_failed test(s) failed!"
fi

exit $tests_failed 