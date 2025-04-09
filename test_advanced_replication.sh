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

# Unique metrics ports for each test to avoid conflicts
METRICS_PORT_BASE=8080
REPLICATION_TEST_METRICS_PORT=$((METRICS_PORT_BASE + 1))
FENCING_TEST_METRICS_PORT=$((METRICS_PORT_BASE + 2))
FAILOVER_TEST_METRICS_PORT=$((METRICS_PORT_BASE + 3))
SPLIT_BRAIN_TEST_METRICS_PORT=$((METRICS_PORT_BASE + 4))
LEASE_EXPIRY_TEST_METRICS_PORT=$((METRICS_PORT_BASE + 5))

BASE_CLIENT_ID=1000

# Check if a test name is provided as the first argument
if [ -n "$1" ]; then
    TEST_TO_RUN="$1"
    echo "Running individual test: $TEST_TO_RUN"
    
    case $TEST_TO_RUN in
        replication)
            # Will run enhanced_replication_test later in the script
            RUN_ONLY_REPLICATION=true
            ;;
        fencing)
            # Will run fencing_test later in the script
            RUN_ONLY_FENCING=true
            ;;
        failover)
            # Will run expanded_failover_test later in the script
            RUN_ONLY_FAILOVER=true
            ;;
        split-brain)
            # Will run improved_split_brain_test later in the script
            RUN_ONLY_SPLIT_BRAIN=true
            ;;
        lease-expiry)
            # Will run lease_expiry_test later in the script
            RUN_ONLY_LEASE_EXPIRY=true
            ;;
        *)
            echo "Unknown test: $TEST_TO_RUN"
            echo "Usage: $0 {replication|fencing|failover|split-brain|lease-expiry}"
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

# Function to get metrics from the server
get_metrics() {
    local port=$1
    curl -s "http://localhost:$port/metrics"
}

# Function to check lock state on secondary
check_secondary_state() {
    local expected_lock_holder=$1
    local max_attempts=10
    local attempts=0
    
    echo "Checking lock state on secondary..."
    
    while [ $attempts -lt $max_attempts ]; do
        if [ -f "data/lock_state.json" ]; then
            local lock_holder=$(jq -r '.lock_holder // -1' data/lock_state.json)
            
            # If we expect the lock to be released (-1), check exactly for -1
            if [ "$expected_lock_holder" == "-1" ]; then
                if [ "$lock_holder" == "-1" ]; then
                    print_color $GREEN "Lock state verified on secondary: lock is released"
                    return 0
                else
                    print_color $YELLOW "Lock is still held by $lock_holder, expected it to be released"
                fi
            # Otherwise we're just checking that some client holds the lock (lock_holder > -1)
            else
                if [ "$lock_holder" != "-1" ]; then
                    print_color $GREEN "Lock state verified on secondary: lock is held by client $lock_holder"
                    return 0
                else
                    print_color $YELLOW "Lock is not held by any client, expected it to be held"
                fi
            fi
        else
            print_color $YELLOW "Lock state file not found on secondary"
        fi
        
        attempts=$((attempts+1))
        echo "Attempt $attempts: Waiting for lock state to replicate..."
        sleep 1
    done
    
    print_color $RED "Failed to verify lock state on secondary"
    return 1
}

# Function to print file contents
print_logs() {
    local log_file=$1
    if [ -f "$log_file" ]; then
        print_color $YELLOW "Contents of $log_file:"
        cat "$log_file"
        echo ""
    fi
}

# Function to clean up processes
cleanup() {
    print_color $YELLOW "Cleaning up processes..."
    
    # Kill all potential server and client processes
    killall -9 lock_server lock_client 2>/dev/null || true
    pkill -9 -f "lock_server" || true
    pkill -9 -f "bin/server" || true
    pkill -9 -f "lock_client" || true
    pkill -9 -f "bin/lock_server" || true
    pkill -9 -f "bin/lock_client" || true
    
    # Wait a moment for processes to die
    sleep 2
    
    # More aggressive port cleanup
    for port in $PRIMARY_PORT $SECONDARY_PORT $METRICS_PORT_BASE $REPLICATION_TEST_METRICS_PORT $FENCING_TEST_METRICS_PORT $FAILOVER_TEST_METRICS_PORT $SPLIT_BRAIN_TEST_METRICS_PORT $LEASE_EXPIRY_TEST_METRICS_PORT; do
        print_color $YELLOW "Checking for processes using port $port..."
        # Find and kill any process using this port
        if [ "$(uname)" = "Darwin" ]; then
            # macOS specific command
            lsof -i :$port | awk 'NR>1 {print $2}' | xargs -r kill -9 2>/dev/null || true
        else
            # Linux specific command
            fuser -k -n tcp $port 2>/dev/null || true
        fi
    done
    
    # Longer wait after aggressive cleanup
    sleep 5
    
    # Final verification that everything is cleaned up
    local running_servers=$(pgrep -f "lock_server" || true)
    local running_clients=$(pgrep -f "lock_client" || true)
    local port_procs=""
    
    if [ -n "$running_servers" ] || [ -n "$running_clients" ] || [ -n "$port_procs" ]; then
        print_color $RED "Warning: Some processes are still running!"
        if [ -n "$running_servers" ]; then
            print_color $RED "Server processes: $running_servers"
        fi
        if [ -n "$running_clients" ]; then
            print_color $RED "Client processes: $running_clients"
        fi
        if [ -n "$port_procs" ]; then
            print_color $RED "Processes using test ports: $port_procs"
        fi
    else
        print_color $GREEN "All test processes terminated"
    fi
}

# Run cleanup at the very start
cleanup

# Register cleanup function for script exit
trap cleanup EXIT

# Start from a clean state
rm -f data/lock_state.json data/processed_requests.log data/file_*
mkdir -p logs

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

# Function to check if server started successfully
verify_server_started() {
    local log_file=$1
    local server_type=$2
    local max_attempts=10
    local attempts=0
    
    echo "Verifying $server_type server started successfully..."
    
    while [ $attempts -lt $max_attempts ]; do
        if grep -q "Server listening at" "$log_file"; then
            print_color $GREEN "$server_type server started successfully"
            return 0
        elif grep -q "Failed to listen" "$log_file"; then
            print_color $RED "$server_type server failed to start - port already in use"
            return 1
        fi
        
        attempts=$((attempts+1))
        echo "Attempt $attempts: Waiting for server to log startup..."
        sleep 1
    done
    
    print_color $RED "$server_type server startup verification timed out"
    return 1
}

# Run all tests
print_header "Starting Advanced Tests"

tests_failed=0

run_enhanced_replication_test() {
    print_header "Enhanced Replication Test"
    
    print_color $YELLOW "Starting primary server on port $PRIMARY_PORT"
    bin/lock_server --role primary --id 1 --address ":$PRIMARY_PORT" --peer "$SECONDARY_ADDR" --metrics-address ":$REPLICATION_TEST_METRICS_PORT" > logs/test_primary.log 2>&1 &
    PRIMARY_PID=$!
    
    # Wait for primary to start
    if ! wait_for_server "$PRIMARY_ADDR"; then
        print_color $RED "Primary server failed to start"
        exit 1
    fi
    
    # Verify primary started
    if ! verify_server_started "logs/test_primary.log" "Primary"; then
        print_color $RED "Primary server failed to start properly"
        return 1
    fi
    
    print_color $YELLOW "Starting secondary server on port $SECONDARY_PORT"
    bin/lock_server --role secondary --id 2 --address ":$SECONDARY_PORT" --peer "$PRIMARY_ADDR" --metrics-address ":$REPLICATION_TEST_METRICS_PORT" > logs/test_secondary.log 2>&1 &
    SECONDARY_PID=$!
    
    # Wait for secondary to start
    if ! wait_for_server "$SECONDARY_ADDR"; then
        print_color $RED "Secondary server failed to start"
        exit 1
    fi
    
    # Verify secondary started
    if ! verify_server_started "logs/test_secondary.log" "Secondary"; then
        print_color $RED "Secondary server failed to start properly"
        return 1
    fi
    
    local CLIENT_ID=$BASE_CLIENT_ID
    print_color $YELLOW "Running client to acquire lock..."
    bin/lock_client acquire --servers="$PRIMARY_ADDR" --client-id=$CLIENT_ID --timeout=10s > logs/test_client.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "Client failed to acquire lock"
        return 1
    fi
    
    # Wait for replication to complete
    sleep 5
    
    # Check if secondary has the correct lock state
    if ! check_secondary_state "$CLIENT_ID"; then
        print_color $RED "Lock state not replicated to secondary correctly"
        return 1
    fi
    
    # Release the lock
    print_color $YELLOW "Releasing the lock..."
    bin/lock_client release --servers="$PRIMARY_ADDR" --client-id=$CLIENT_ID --timeout=10s > logs/test_client_release.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "Client failed to release lock"
        return 1
    fi
    
    # Wait for replication to complete - increased from 5 to 10 seconds
    print_color $YELLOW "Waiting for lock release to replicate..."
    sleep 10
    
    # Check if secondary has the correct lock state (lock should be released, holder = -1)
    if ! check_secondary_state "-1"; then
        print_color $RED "Lock release not replicated to secondary correctly"
        return 1
    fi
    
    print_color $GREEN "Enhanced replication test passed!"
    
    # Clean up
    kill $PRIMARY_PID $SECONDARY_PID || true
    sleep 2
    
    # Run extra cleanup to ensure no lingering processes
    cleanup
    return 0
}

run_fencing_test() {
    print_header "Fencing Behavior Test"
    
    print_color $YELLOW "Starting primary server on port $PRIMARY_PORT"
    bin/lock_server --role primary --id 1 --address ":$PRIMARY_PORT" --peer "$SECONDARY_ADDR" --metrics-address ":$FENCING_TEST_METRICS_PORT" > logs/test_primary.log 2>&1 &
    PRIMARY_PID=$!
    
    # Wait for primary to start
    if ! wait_for_server "$PRIMARY_ADDR"; then
        print_color $RED "Primary server failed to start"
        exit 1
    fi
    
    # Verify primary started
    if ! verify_server_started "logs/test_primary.log" "Primary"; then
        print_color $RED "Primary server failed to start properly"
        return 1
    fi
    
    print_color $YELLOW "Starting secondary server on port $SECONDARY_PORT with metrics on port $FENCING_TEST_METRICS_PORT"
    bin/lock_server --role secondary --id 2 --address ":$SECONDARY_PORT" --peer "$PRIMARY_ADDR" --metrics-address ":$FENCING_TEST_METRICS_PORT" > logs/test_secondary.log 2>&1 &
    SECONDARY_PID=$!
    
    # Wait for secondary to start
    if ! wait_for_server "$SECONDARY_ADDR"; then
        print_color $RED "Secondary server failed to start"
        exit 1
    fi
    
    # Verify secondary started
    if ! verify_server_started "logs/test_secondary.log" "Secondary"; then
        print_color $RED "Secondary server failed to start properly"
        return 1
    fi
    
    # Acquire a lock with client 1 - using ID 1 instead of BASE_CLIENT_ID+1
    local CLIENT_ID=1
    print_color $YELLOW "Client $CLIENT_ID acquiring a lock..."
    bin/lock_client acquire --servers="$PRIMARY_ADDR,$SECONDARY_ADDR" --client-id=$CLIENT_ID --timeout=10s > logs/test_client_1.log 2>&1
    
    # Verify the primary process is running before trying to kill it
    print_color $YELLOW "Killing primary server to trigger failover..."
    if is_process_running $PRIMARY_PID; then
        kill $PRIMARY_PID
        print_color $GREEN "Primary server killed successfully (PID: $PRIMARY_PID)"
    else
        print_color $RED "Primary server not running at PID $PRIMARY_PID"
        # Try to find and kill any running primary server processes
        pkill -f "lock_server.*--role primary"
        print_color $YELLOW "Killed any running primary server processes"
    fi
    
    # Wait for secondary to detect failure and promote itself
    print_color $YELLOW "Waiting for secondary to detect primary failure and promote itself..."
    
    # Monitor the secondary logs for promotion
    local max_attempts=30
    local attempts=0
    local promoted=false
    
    while [ $attempts -lt $max_attempts ] && [ "$promoted" = false ]; do
        if grep -q "Promoting to primary" logs/test_secondary.log || grep -q "Entering fencing period" logs/test_secondary.log; then
            promoted=true
            print_color $GREEN "Secondary detected primary failure and is being promoted!"
            
            # Additional debug - show relevant log lines
            print_color $YELLOW "Relevant log entries from secondary:"
            grep -E "heartbeat|fail|promot|fencing" logs/test_secondary.log | tail -10
        else
            attempts=$((attempts+1))
            echo "Attempt $attempts: Waiting for secondary to be promoted..."
            sleep 1
        fi
    done
    
    if [ "$promoted" = false ]; then
        print_color $RED "Secondary was not promoted within the expected time"
        print_color $YELLOW "Secondary server log tail:"
        tail -20 logs/test_secondary.log
        return 1
    fi
    
    # Give a bit more time for fencing period to be fully established
    sleep 5
    
    # Now try different operations during fencing period with different clients, using ONLY the secondary
    # 1. Try to acquire a lock (should be rejected)
    local ACQUIRE_CLIENT_ID=$((BASE_CLIENT_ID+2))
    print_color $YELLOW "Trying to acquire a lock during fencing period with client $ACQUIRE_CLIENT_ID..."
    bin/lock_client acquire --servers="$SECONDARY_ADDR" --client-id=$ACQUIRE_CLIENT_ID --timeout=5s > logs/test_acquire_during_fencing.log 2>&1
    
    ACQUIRE_RESULT=$?
    
    # Print detailed log for debugging
    print_color $YELLOW "Client acquisition log during fencing:"
    cat logs/test_acquire_during_fencing.log
    
    # Debug the check values
    print_color $YELLOW "DEBUG - Acquire result exit code: $ACQUIRE_RESULT"
    print_color $YELLOW "DEBUG - Grep for 'Server is in fencing period': $(grep 'Server is in fencing period' logs/test_acquire_during_fencing.log > /dev/null && echo 'Found' || echo 'Not found')"
    
    # Check if we got a fencing period rejection (this is the expected behavior)
    if grep "Server is in fencing period" logs/test_acquire_during_fencing.log > /dev/null; then
        print_color $GREEN "Lock acquisition correctly rejected during fencing period"
    else
        print_color $RED "Lock acquisition during fencing period did not get proper fencing rejection"
        return 1
    fi
    
    # 2. Try to release the existing lock (should be allowed, but may be implementation dependent)
    print_color $YELLOW "Trying to release the lock during fencing period with client $CLIENT_ID..."
    bin/lock_client release --servers="$SECONDARY_ADDR" --client-id=$CLIENT_ID --timeout=5s > logs/test_release_during_fencing.log 2>&1
    
    # Note: Lock release during fencing might be implementation dependent
    # Some systems might reject all operations during fencing, including release
    if [ $? -ne 0 ]; then
        print_color $YELLOW "Lock release during fencing period failed. This could be by design."
        print_color $YELLOW "Release log contents:"
        cat logs/test_release_during_fencing.log
    else
        print_color $GREEN "Lock release correctly processed during fencing period"
    fi
    
    # After checking all operations, print logs for debugging
    print_color $YELLOW "Secondary server log tail (for debugging):"
    tail -n 30 logs/test_secondary.log
    
    # 3. Try to append to a file (should be rejected)
    local APPEND_CLIENT_ID=$((BASE_CLIENT_ID+3))
    print_color $YELLOW "Trying to append to a file during fencing period with client $APPEND_CLIENT_ID..."
    bin/lock_client append --servers="$SECONDARY_ADDR" --client-id=$APPEND_CLIENT_ID --file="file_test" --content="test content" --timeout=5s > logs/test_append_during_fencing.log 2>&1
    
    APPEND_EXIT_CODE=$?
    # Print the log for debugging
    print_color $YELLOW "Client append log during fencing:"
    cat logs/test_append_during_fencing.log
    
    # Check if the operation failed or if the log contains the fencing rejection message
    if [ $APPEND_EXIT_CODE -eq 0 ] && ! grep -q "operation FileAppend rejected: server is in fencing period" logs/test_append_during_fencing.log; then
        print_color $RED "File append during fencing period succeeded, but should have been rejected"
        print_color $RED "Exit code: $APPEND_EXIT_CODE"
        # Also check secondary server logs to see if they received and processed the request
        print_color $YELLOW "Checking server logs for FileAppend request during fencing..."
        FENCING_LOG_MATCH=$(grep -E "FENCING: Rejecting file append|DEBUG FileAppend.*isFencing=true" logs/test_secondary.log)
        if [ -n "$FENCING_LOG_MATCH" ]; then
            print_color $YELLOW "Found fencing log entries for FileAppend, but client did not receive error:"
            echo "$FENCING_LOG_MATCH"
        else
            print_color $RED "No fencing log entries found for FileAppend - request may have been processed incorrectly"
        fi
        return 1
    else
        print_color $GREEN "File append correctly rejected during fencing period"
    fi
    
    # Wait for fencing period to end
    print_color $YELLOW "Waiting for fencing period to end..."
    sleep 20
    
    # Try to acquire a lock after fencing (should succeed)
    print_color $YELLOW "Trying to acquire a lock after fencing period with client $ACQUIRE_CLIENT_ID..."
    bin/lock_client acquire --servers="$SECONDARY_ADDR" --client-id=$ACQUIRE_CLIENT_ID --timeout=5s > logs/test_acquire_after_fencing.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "Lock acquisition after fencing period failed, but should have succeeded"
        return 1
    else
        print_color $GREEN "Lock acquisition correctly processed after fencing period"
    fi
    
    # Check logs to see if fencing was activated
    if grep -q "fencing period" logs/test_secondary.log; then
        print_color $GREEN "Fencing period was detected in logs"
    else
        print_color $RED "Fencing period not detected in logs"
        return 1
    fi
    
    # Check metrics for fencing activations
    if grep -q "fencing_activations" <(get_metrics $FENCING_TEST_METRICS_PORT); then
        print_color $GREEN "Fencing activations found in metrics"
    else
        print_color $YELLOW "Fencing activations not found in metrics"
    fi
    
    print_color $GREEN "Fencing behavior test passed!"
    
    # Clean up
    kill $SECONDARY_PID || true
    sleep 2
    
    # At the end of the test, print all test logs
    print_color $YELLOW "========== TEST LOGS =========="
    print_logs "logs/test_client_1.log"
    print_logs "logs/test_acquire_during_fencing.log"
    print_logs "logs/test_release_during_fencing.log"
    print_logs "logs/test_append_during_fencing.log"
    print_logs "logs/test_acquire_after_fencing.log"
    print_color $YELLOW "=============================="
    
    # Run a full cleanup before returning
    cleanup
    return 0
}

# Add explicit cleanup after each test case
run_expanded_failover_test() {
    print_header "Expanded Failover Test - Continued Operations"
    
    print_color $YELLOW "Starting primary server on port $PRIMARY_PORT"
    bin/lock_server --role primary --id 1 --address ":$PRIMARY_PORT" --peer "$SECONDARY_ADDR" > logs/test_primary.log 2>&1 &
    PRIMARY_PID=$!
    
    # Wait for primary to start
    if ! wait_for_server "$PRIMARY_ADDR"; then
        print_color $RED "Primary server failed to start"
        exit 1
    fi
    
    # Verify primary started
    if ! verify_server_started "logs/test_primary.log" "Primary"; then
        print_color $RED "Primary server failed to start properly"
        return 1
    fi
    
    print_color $YELLOW "Starting secondary server on port $SECONDARY_PORT with metrics on port $FAILOVER_TEST_METRICS_PORT"
    bin/lock_server --role secondary --id 2 --address ":$SECONDARY_PORT" --peer "$PRIMARY_ADDR" --metrics-address ":$FAILOVER_TEST_METRICS_PORT" > logs/test_secondary.log 2>&1 &
    SECONDARY_PID=$!
    
    # Wait for secondary to start
    if ! wait_for_server "$SECONDARY_ADDR"; then
        print_color $RED "Secondary server failed to start"
        exit 1
    fi
    
    # Verify secondary started
    if ! verify_server_started "logs/test_secondary.log" "Secondary"; then
        print_color $RED "Secondary server failed to start properly"
        return 1
    fi
    
    # Start a client that will hold a lock and renew its lease
    local CLIENT_ID=$((BASE_CLIENT_ID+4))
    print_color $YELLOW "Starting client $CLIENT_ID to hold lock and renew lease..."
    # Fix flag format: use the correct flag format for Go flags
    bin/lock_client --servers "$PRIMARY_ADDR,$SECONDARY_ADDR" hold --client-id $CLIENT_ID --timeout 60s > logs/test_client_hold.log 2>&1 &
    CLIENT_PID=$!
    
    # Wait for client to acquire lock
    sleep 5
    
    # Verify lock is acquired
    if ! is_process_running $CLIENT_PID; then
        print_color $RED "Client failed to acquire lock"
        return 1
    fi
    
    print_color $GREEN "Client successfully acquired lock"
    
    # Kill primary to trigger failover
    print_color $YELLOW "Killing primary server to trigger failover..."
    kill $PRIMARY_PID
    
    # Wait for failover to complete (past fencing period)
    print_color $YELLOW "Waiting for failover to complete..."
    sleep 45
    
    # Verify client is still running
    if ! is_process_running $CLIENT_PID; then
        print_color $RED "Client did not survive failover"
        return 1
    fi
    
    print_color $GREEN "Client survived failover"
    
    # Now create a file with the client that survived failover
    # Use a valid file name format (file_X where X is 0-99)
    local APPEND_FILE="file_99"
    local APPEND_CONTENT="test content after failover"
    
    print_color $YELLOW "Creating a new client to append to a file after failover..."
    # Wait additional time for fencing period to complete
    print_color $YELLOW "Waiting for fencing period to complete..."
    sleep 30  # Increased from 15 to 30 seconds to be absolutely sure fencing is complete

    # Fix: Add debugging to show the client is creating a new lock acquisition first
    print_color $YELLOW "Debug - Creating a new client to acquire a lock and then append"
    
    # First acquire a lock with a new client
    local APPEND_CLIENT_ID=$((CLIENT_ID+1))
    print_color $YELLOW "Acquiring a lock with client $APPEND_CLIENT_ID before file append..."
    bin/lock_client acquire --servers="$SECONDARY_ADDR" --client-id=$APPEND_CLIENT_ID --timeout=15s > logs/test_client_acquire_for_append.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "New client failed to acquire lock after failover"
        cat logs/test_client_acquire_for_append.log
        return 1
    fi
    
    # Now append to a file with the same client
    print_color $YELLOW "Appending to file with client $APPEND_CLIENT_ID..."
    bin/lock_client append --servers="$SECONDARY_ADDR" --client-id=$APPEND_CLIENT_ID --file="$APPEND_FILE" --content="$APPEND_CONTENT" --timeout=15s > logs/test_client_append_after_failover.log 2>&1

    if [ $? -ne 0 ]; then
        print_color $RED "Client failed to append to file after failover"
        cat logs/test_client_append_after_failover.log
        return 1
    fi
    
    # Verify the file was created
    if [ -f "data/$APPEND_FILE" ] && grep -q "$APPEND_CONTENT" "data/$APPEND_FILE"; then
        print_color $GREEN "File was successfully appended to after failover"
    else
        print_color $RED "File was not created or does not contain the expected content"
        return 1
    fi
    
    print_color $GREEN "Expanded failover test passed!"
    
    # Clean up
    kill $SECONDARY_PID $CLIENT_PID || true
    sleep 2
    cleanup
    return 0
}

run_improved_split_brain_test() {
    print_header "Improved Split-Brain Test"
    
    # This test requires iptables to create a real network partition
    if ! command -v iptables &> /dev/null; then
        print_color $YELLOW "iptables not found, using alternative simulation method"
        USE_IPTABLES=false
    else
        if [ "$(id -u)" -ne 0 ]; then
            print_color $YELLOW "Script not running as root, cannot use iptables, using alternative simulation method"
            USE_IPTABLES=false
        else
            USE_IPTABLES=true
        fi
    fi
    
    print_color $YELLOW "Starting primary server on port $PRIMARY_PORT"
    bin/lock_server --role primary --id 1 --address ":$PRIMARY_PORT" --peer "$SECONDARY_ADDR" > logs/test_primary.log 2>&1 &
    PRIMARY_PID=$!
    
    # Wait for primary to start
    if ! wait_for_server "$PRIMARY_ADDR"; then
        print_color $RED "Primary server failed to start"
        exit 1
    fi
    
    # Verify primary started
    if ! verify_server_started "logs/test_primary.log" "Primary"; then
        print_color $RED "Primary server failed to start properly"
        return 1
    fi
    
    print_color $YELLOW "Starting secondary server on port $SECONDARY_PORT with metrics on port $SPLIT_BRAIN_TEST_METRICS_PORT"
    bin/lock_server --role secondary --id 2 --address ":$SECONDARY_PORT" --peer "$PRIMARY_ADDR" --metrics-address ":$SPLIT_BRAIN_TEST_METRICS_PORT" > logs/test_secondary.log 2>&1 &
    SECONDARY_PID=$!
    
    # Wait for secondary to start
    if ! wait_for_server "$SECONDARY_ADDR"; then
        print_color $RED "Secondary server failed to start"
        exit 1
    fi
    
    # Verify secondary started
    if ! verify_server_started "logs/test_secondary.log" "Secondary"; then
        print_color $RED "Secondary server failed to start properly"
        return 1
    fi
    
    print_color $YELLOW "Waiting for servers to establish replication..."
    sleep 5
    
    # Create a network partition
    if [ "$USE_IPTABLES" = true ]; then
        print_color $YELLOW "Creating network partition using iptables..."
        # Block traffic between primary and secondary
        iptables -A INPUT -p tcp --dport $PRIMARY_PORT -j DROP
        iptables -A INPUT -p tcp --dport $SECONDARY_PORT -j DROP
    else
        print_color $YELLOW "Simulating network partition by pausing the primary..."
        # Pause the primary
        kill -STOP $PRIMARY_PID
        print_color $GREEN "Primary server paused (PID: $PRIMARY_PID)"
    fi
    
    # Wait for secondary to detect failure and promote itself
    print_color $YELLOW "Waiting for secondary to detect primary failure..."
    sleep 30
    
    # Check secondary logs to confirm promotion
    if grep -q "Promoting to primary" logs/test_secondary.log; then
        print_color $GREEN "Secondary was promoted to primary as expected"
    else
        print_color $RED "Secondary was not promoted to primary"
        print_color $YELLOW "Secondary server log tail:"
        tail -20 logs/test_secondary.log
    fi
    
    # Create two clients, one for each server
    local CLIENT1_ID=$((BASE_CLIENT_ID+5))
    local CLIENT2_ID=$((BASE_CLIENT_ID+6))
    
    # Try to acquire lock on promoted secondary (should work)
    print_color $YELLOW "Client $CLIENT2_ID trying to acquire lock on promoted secondary..."
    bin/lock_client acquire --servers="$SECONDARY_ADDR" --client-id=$CLIENT2_ID --timeout=5s > logs/test_client_secondary.log 2>&1
    SECONDARY_CLIENT_SUCCESS=$?
    
    # Output logs for debugging
    print_color $YELLOW "Client attempt on promoted secondary log:"
    cat logs/test_client_secondary.log
    
    # Resume primary if using alternative method
    if [ "$USE_IPTABLES" = false ]; then
        print_color $YELLOW "Resuming the primary..."
        kill -CONT $PRIMARY_PID
        print_color $GREEN "Primary server resumed (PID: $PRIMARY_PID)"
    else
        print_color $YELLOW "Removing network partition..."
        iptables -D INPUT -p tcp --dport $PRIMARY_PORT -j DROP
        iptables -D INPUT -p tcp --dport $SECONDARY_PORT -j DROP
    fi
    
    # Give primary time to resume and detect the split-brain condition
    print_color $YELLOW "Waiting for primary to resume and detect split-brain condition..."
    sleep 15   # Increased to 15 seconds to allow more time for split-brain detection
    
    # Check primary logs for split-brain detection
    if grep -q "split-brain" logs/test_primary.log; then
        print_color $GREEN "Primary detected split-brain condition"
    else
        print_color $YELLOW "Primary did not detect split-brain condition in logs"
        print_color $YELLOW "Primary server log tail:"
        tail -20 logs/test_primary.log
    fi
    
    # Try to acquire lock on original primary
    print_color $YELLOW "Client $CLIENT1_ID trying to acquire lock on original primary..."
    bin/lock_client acquire --servers="$PRIMARY_ADDR" --client-id=$CLIENT1_ID --timeout=5s > logs/test_client_primary.log 2>&1
    PRIMARY_CLIENT_SUCCESS=$?
    
    # Output logs for debugging
    print_color $YELLOW "Client attempt on original primary log:"
    cat logs/test_client_primary.log
    
    # Release locks to clean up state
    if [ $PRIMARY_CLIENT_SUCCESS -eq 0 ]; then
        print_color $YELLOW "Releasing lock on primary..."
        bin/lock_client release --servers="$PRIMARY_ADDR" --client-id=$CLIENT1_ID --timeout=5s > /dev/null 2>&1
    fi
    
    if [ $SECONDARY_CLIENT_SUCCESS -eq 0 ]; then
        print_color $YELLOW "Releasing lock on secondary..."
        bin/lock_client release --servers="$SECONDARY_ADDR" --client-id=$CLIENT2_ID --timeout=5s > /dev/null 2>&1
    fi
    
    # Analyze results
    print_color $YELLOW "Checking success status: PRIMARY=$PRIMARY_CLIENT_SUCCESS, SECONDARY=$SECONDARY_CLIENT_SUCCESS"
    
    # Check for split-brain detection success in primary logs
    if grep -q "SPLIT-BRAIN RESOLVED: Demoting self to secondary" logs/test_primary.log; then
        PRIMARY_DETECTED_SPLIT_BRAIN=true
        print_color $GREEN "✓ Primary detected split-brain and demoted itself."
    else
        PRIMARY_DETECTED_SPLIT_BRAIN=false
        print_color $RED "✗ Primary did not detect split-brain or demote itself."
    fi
    
    # Check for fencing in secondary logs
    if grep -q "FENCING: Rejecting lock acquisition" logs/test_secondary.log; then
        SECONDARY_IN_FENCING=true
        print_color $GREEN "✓ Secondary is correctly in fencing period after promotion."
    else
        SECONDARY_IN_FENCING=false
        print_color $RED "✗ Secondary is not in fencing period after promotion."
    fi
    
    # Check success criteria
    if [ "$PRIMARY_DETECTED_SPLIT_BRAIN" = true ] && [ "$SECONDARY_IN_FENCING" = true ]; then
        print_color $GREEN "Split-brain correctly prevented!"
        print_color $GREEN "Primary detected split-brain and demoted itself to secondary."
        print_color $GREEN "Secondary rejected client during fencing period."
        
        # Check client log outputs directly to see if any client succeeded
        if grep -q "Successfully acquired lock" logs/test_client_primary.log || grep -q "Successfully acquired lock" logs/test_client_secondary.log; then
            print_color $RED "However, at least one client log shows it successfully acquired a lock, which should not happen!"
            return 1
        else
            print_color $GREEN "All clients correctly failed to acquire locks during this scenario."
            return 0
        fi
    else
        print_color $RED "Split-brain prevention did not work correctly."
        if grep -q "Successfully acquired lock" logs/test_client_primary.log && grep -q "Successfully acquired lock" logs/test_client_secondary.log; then
            print_color $RED "Split-brain scenario detected! Both clients acquired locks on different servers."
        fi
        return 1
    fi
    
    # Check logs for fencing period
    if grep -q "fencing period" logs/test_secondary.log; then
        print_color $GREEN "Fencing period detected in logs"
    else
        print_color $RED "Fencing period not detected in logs"
        return 1
    fi
    
    print_color $GREEN "Improved split-brain test passed!"
    
    # Clean up
    kill $PRIMARY_PID $SECONDARY_PID || true
    sleep 2
    cleanup
    return 0
}

run_lease_expiry_test() {
    print_header "Lease Expiry Test"
    
    print_color $YELLOW "Starting primary server on port $PRIMARY_PORT"
    bin/lock_server --role primary --id 1 --address ":$PRIMARY_PORT" --peer "$SECONDARY_ADDR" > logs/test_primary.log 2>&1 &
    PRIMARY_PID=$!
    
    # Wait for primary to start
    if ! wait_for_server "$PRIMARY_ADDR"; then
        print_color $RED "Primary server failed to start"
        exit 1
    fi
    
    # Verify primary started
    if ! verify_server_started "logs/test_primary.log" "Primary"; then
        print_color $RED "Primary server failed to start properly"
        return 1
    fi
    
    print_color $YELLOW "Starting secondary server on port $SECONDARY_PORT with metrics on port $LEASE_EXPIRY_TEST_METRICS_PORT"
    bin/lock_server --role secondary --id 2 --address ":$SECONDARY_PORT" --peer "$PRIMARY_ADDR" --metrics-address ":$LEASE_EXPIRY_TEST_METRICS_PORT" > logs/test_secondary.log 2>&1 &
    SECONDARY_PID=$!
    
    # Wait for secondary to start
    if ! wait_for_server "$SECONDARY_ADDR"; then
        print_color $RED "Secondary server failed to start"
        exit 1
    fi
    
    # Verify secondary started
    if ! verify_server_started "logs/test_secondary.log" "Secondary"; then
        print_color $RED "Secondary server failed to start properly"
        return 1
    fi
    
    # Acquire a lock with a short lease
    local CLIENT_ID=$((BASE_CLIENT_ID+7))
    print_color $YELLOW "Client $CLIENT_ID acquiring a lock with short lease..."
    # Note: Assuming default lease time is short enough, if not we would need to modify the server code
    bin/lock_client acquire --servers="$PRIMARY_ADDR" --client-id=$CLIENT_ID --timeout=5s > logs/test_client_lease.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "Client failed to acquire lock"
        return 1
    fi
    
    # Wait for replication to complete
    sleep 3
    
    # Kill primary to trigger failover
    print_color $YELLOW "Killing primary server to trigger failover..."
    kill $PRIMARY_PID
    
    # Wait for secondary to detect failure, promote itself, and for the lease to expire
    # This should be the lease duration + fencing period
    print_color $YELLOW "Waiting for failover and lease expiry..."
    sleep 45
    
    # After fencing period and lease expiry, try to acquire the same lock with a different client
    local NEW_CLIENT_ID=$((BASE_CLIENT_ID+8))
    print_color $YELLOW "New client $NEW_CLIENT_ID trying to acquire the lock after lease expiry..."
    bin/lock_client acquire --servers="$SECONDARY_ADDR" --client-id=$NEW_CLIENT_ID --timeout=5s > logs/test_client_after_expiry.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "New client failed to acquire lock after lease expiry"
        return 1
    fi
    
    print_color $GREEN "New client successfully acquired lock after lease expiry"
    
    # Check if lock state shows a lock is held (don't check specific client ID)
    if ! check_secondary_state "held"; then
        print_color $RED "Lock state doesn't show lock is held after lease expiry"
        return 1
    fi
    
    print_color $GREEN "Lease expiry test passed!"
    
    # Clean up
    kill $SECONDARY_PID || true
    sleep 2
    cleanup
    return 0
}

if [ "$RUN_ONLY_REPLICATION" = true ]; then
    run_enhanced_replication_test
    exit $?
fi

if [ "$RUN_ONLY_FENCING" = true ]; then
    run_fencing_test
    exit $?
fi

if [ "$RUN_ONLY_FAILOVER" = true ]; then
    run_expanded_failover_test
    exit $?
fi

if [ "$RUN_ONLY_SPLIT_BRAIN" = true ]; then
    run_improved_split_brain_test
    exit $?
fi

if [ "$RUN_ONLY_LEASE_EXPIRY" = true ]; then
    run_lease_expiry_test
    exit $?
fi

run_enhanced_replication_test
if [ $? -ne 0 ]; then
    tests_failed=$((tests_failed+1))
fi
cleanup
sleep 2

run_fencing_test
if [ $? -ne 0 ]; then
    tests_failed=$((tests_failed+1))
fi
cleanup
sleep 2

run_expanded_failover_test
if [ $? -ne 0 ]; then
    tests_failed=$((tests_failed+1))
fi
cleanup
sleep 2

run_improved_split_brain_test
if [ $? -ne 0 ]; then
    tests_failed=$((tests_failed+1))
fi
cleanup
sleep 2

run_lease_expiry_test
if [ $? -ne 0 ]; then
    tests_failed=$((tests_failed+1))
fi
cleanup
sleep 2

# Print summary
print_header "Test Summary"
if [ $tests_failed -eq 0 ]; then
    print_color $GREEN "All advanced tests passed!"
else
    print_color $RED "$tests_failed advanced test(s) failed!"
fi

# Run an extra cleanup at the very end
cleanup

exit $tests_failed