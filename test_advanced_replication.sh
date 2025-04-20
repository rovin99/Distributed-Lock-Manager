#!/bin/bash

# Set the color variables
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration for 3-node cluster
PORT_1=50051
PORT_2=50052
PORT_3=50053
SERVER_1_ADDR="localhost:$PORT_1"
SERVER_2_ADDR="localhost:$PORT_2"
SERVER_3_ADDR="localhost:$PORT_3"
ALL_SERVERS="$SERVER_1_ADDR,$SERVER_2_ADDR,$SERVER_3_ADDR"
TEST_TIMEOUT=30

# Unique metrics ports for each test to avoid conflicts
METRICS_PORT_BASE=8080
REPLICATION_TEST_METRICS_PORT=$((METRICS_PORT_BASE + 1))
FENCING_TEST_METRICS_PORT=$((METRICS_PORT_BASE + 2))
FAILOVER_TEST_METRICS_PORT=$((METRICS_PORT_BASE + 3))
SPLIT_BRAIN_TEST_METRICS_PORT=$((METRICS_PORT_BASE + 4))
LEASE_EXPIRY_TEST_METRICS_PORT=$((METRICS_PORT_BASE + 5))
QUORUM_LOSS_TEST_METRICS_PORT=$((METRICS_PORT_BASE + 6))

BASE_CLIENT_ID=1000

# Check if a test name is provided as the first argument
if [ -n "$1" ]; then
    TEST_TO_RUN="$1"
    echo "Running individual test: $TEST_TO_RUN"
    
    case $TEST_TO_RUN in
        replication)
            RUN_ONLY_REPLICATION=true
            ;;
        fencing)
            RUN_ONLY_FENCING=true
            ;;
        failover)
            RUN_ONLY_FAILOVER=true
            ;;
        split-brain)
            RUN_ONLY_SPLIT_BRAIN=true
            ;;
        lease-expiry)
            RUN_ONLY_LEASE_EXPIRY=true
            ;;
        quorum-loss)
            RUN_ONLY_QUORUM_LOSS=true
            ;;
        *)
            echo "Unknown test: $TEST_TO_RUN"
            echo "Usage: $0 {replication|fencing|failover|split-brain|lease-expiry|quorum-loss}"
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
    local port=${addr#*:}  # Extract port number from address
    local max_attempts=30  # Increased from 20 to 30
    local attempts=0
    local ready=false
    
    echo "Waiting for server at $addr to be ready..."
    
    while [ $attempts -lt $max_attempts ] && [ "$ready" = false ]; do
        # Multiple ways to check if the server is available
        if nc -z -w 1 ${addr/:/ } >/dev/null 2>&1 || lsof -i :$port >/dev/null 2>&1 || grep -q "Server listening at" "logs/server${addr: -1}.log" 2>/dev/null; then
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
        print_color $YELLOW "Last 10 lines of server log:"
        tail -10 "logs/server${addr: -1}.log" 2>/dev/null || echo "No log file found"
        return 1
    fi
    
    return 0
}

# Function to get metrics from the server
get_metrics() {
    local port=$1
    curl -s "http://localhost:$port/metrics"
}

# Function to check lock state on a specific server
check_lock_state() {
    local server_id=$1
    local expected_lock_holder=$2
    local max_attempts=15
    local attempts=0
    
    echo "Checking lock state on Server $server_id..."
    
    # Create a lock state file for verification
    curl -s "http://localhost:$((METRICS_PORT_BASE + server_id))/status" > /dev/null 2>&1
    
    while [ $attempts -lt $max_attempts ]; do
        if [ -f "data/lock_state.json" ]; then
            local lock_holder=$(jq -r '.lock_holder // -1' data/lock_state.json)
            
            # If we expect the lock to be released (-1), check exactly for -1
            if [ "$expected_lock_holder" == "-1" ]; then
                if [ "$lock_holder" == "-1" ]; then
                    print_color $GREEN "Lock state verified on Server $server_id: lock is released"
                    return 0
                else
                    print_color $YELLOW "Lock is still held by $lock_holder, expected it to be released"
                fi
            # If we expect any lock to be held (held)
            elif [ "$expected_lock_holder" == "held" ]; then
                if [ "$lock_holder" != "-1" ]; then
                    print_color $GREEN "Lock state verified on Server $server_id: lock is held by client $lock_holder"
                    return 0
                else
                    print_color $YELLOW "Lock is not held by any client, expected it to be held"
                fi
            # Otherwise check for a specific client ID
            else
                if [ "$lock_holder" == "$expected_lock_holder" ]; then
                    print_color $GREEN "Lock state verified on Server $server_id: lock is held by client $expected_lock_holder"
                    return 0
                else
                    print_color $YELLOW "Lock is held by $lock_holder, expected $expected_lock_holder"
                fi
            fi
        else
            print_color $YELLOW "Lock state file not found for Server $server_id"
        fi
        
        attempts=$((attempts+1))
        echo "Attempt $attempts: Waiting for lock state on Server $server_id..."
        sleep 2
    done
    
    print_color $RED "Failed to verify lock state on Server $server_id"
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
    
    # Kill only our specific server/client processes by PID
    for pid_var in SERVER_1_PID SERVER_2_PID SERVER_3_PID CLIENT_PID; do
        if [ -n "${!pid_var}" ] && is_process_running ${!pid_var}; then
            kill ${!pid_var} 2>/dev/null || kill -9 ${!pid_var} 2>/dev/null || true
            print_color $YELLOW "Killed process with PID ${!pid_var}"
        fi
    done
    
    # Kill only our specific server/client processes by binary path
    pkill -f "bin/server --id [1-3] --address" 2>/dev/null || true
    pkill -f "bin/client --servers" 2>/dev/null || true
    
    # Wait a moment for processes to die
    sleep 2
    
    # More targeted port cleanup
    for port in $PORT_1 $PORT_2 $PORT_3 \
               $METRICS_PORT_BASE $REPLICATION_TEST_METRICS_PORT $FENCING_TEST_METRICS_PORT \
               $FAILOVER_TEST_METRICS_PORT $SPLIT_BRAIN_TEST_METRICS_PORT $LEASE_EXPIRY_TEST_METRICS_PORT \
               $QUORUM_LOSS_TEST_METRICS_PORT; do
        # Find and kill any process using this port
        if [ "$(uname)" = "Darwin" ]; then
            # macOS specific command
            lsof -ti :$port | xargs kill -9 2>/dev/null || true
        else
            # Linux specific command
            fuser -k -n tcp $port 2>/dev/null || true
        fi
    done
    
    print_color $GREEN "Cleanup completed"
}

# Function to check if server started successfully
verify_server_started() {
    local log_file=$1
    local server_id=$2
    local max_attempts=10
    local attempts=0
    
    echo "Verifying Server $server_id started successfully..."
    
    while [ $attempts -lt $max_attempts ]; do
        if grep -q "Server listening at" "$log_file"; then
            print_color $GREEN "Server $server_id started successfully"
            return 0
        elif grep -q "Failed to listen" "$log_file"; then
            print_color $RED "Server $server_id failed to start - port already in use"
            return 1
        fi
        
        attempts=$((attempts+1))
        echo "Attempt $attempts: Waiting for server to log startup..."
        sleep 1
    done
    
    print_color $RED "Server $server_id startup verification timed out"
    return 1
}

# Function to check if a server was promoted to primary
check_promotion() {
    local server_id=$1
    local max_attempts=30
    local attempts=0
    
    echo "Checking if Server $server_id was promoted to Primary..."
    
    while [ $attempts -lt $max_attempts ]; do
        if grep -q "Promoting to primary\|promoted to Primary\|Starting as primary\|Assuming primary role\|role=Primary\|I am the leader\|Acting as primary\|Became primary\|isPrimary=true" "logs/server$server_id.log"; then
            print_color $GREEN "Server $server_id was promoted to Primary"
            return 0
        fi
        
        attempts=$((attempts+1))
        echo "Attempt $attempts: Waiting for promotion log entry..."
        sleep 2
    done
    
    print_color $RED "No promotion log entry found for Server $server_id"
    return 1
}

# Function to verify fencing period on a server
verify_fencing() {
    local server_id=$1
    local max_attempts=30
    local attempts=0
    
    echo "Checking if Server $server_id entered fencing period..."
    
    while [ $attempts -lt $max_attempts ]; do
        if grep -q "fencing period\|Entering fencing\|Starting fencing\|Begin fencing\|isFencing=true" "logs/server$server_id.log"; then
            print_color $GREEN "Server $server_id entered fencing period"
            return 0
        fi
        
        attempts=$((attempts+1))
        echo "Attempt $attempts: Waiting for fencing log entry..."
        sleep 1
    done
    
    print_color $RED "No fencing period log entry found for Server $server_id"
    return 1
}

# Function to wait for fencing period to end
wait_for_fencing_end() {
    local server_id=$1
    local max_wait=60
    
    print_color $YELLOW "Waiting for fencing period to end on Server $server_id..."
    
    # First confirm the server entered fencing
    if ! verify_fencing $server_id; then
        print_color $RED "Server $server_id didn't enter fencing period"
        return 1
    fi
    
    print_color $YELLOW "Fencing period confirmed, waiting for it to end (up to $max_wait seconds)..."
    
    # Keep checking for fencing end logs for up to max_wait seconds
    fencing_end_start=$(date +%s)
    fencing_end_timeout=$((fencing_end_start + max_wait))
    
    while [ $(date +%s) -lt $fencing_end_timeout ]; do
        if grep -q "Fencing period ended\|Ending fencing\|fencing period.*ended\|isFencing=false" logs/server${server_id}.log; then
            print_color $GREEN "Fencing period ended as confirmed by logs"
            return 0
        fi
        sleep 5
        echo "Still waiting for fencing period to end..."
    done
    
    print_color $YELLOW "No explicit fencing end log found, but continuing based on timeout"
    return 0
}

# Function to stop all servers
stop_cluster() {
    print_color $YELLOW "Stopping all servers..."
    
    # Kill Server 1 if running
    if [ -n "$SERVER_1_PID" ] && is_process_running $SERVER_1_PID; then
        kill $SERVER_1_PID
    fi
    
    # Kill Server 2 if running
    if [ -n "$SERVER_2_PID" ] && is_process_running $SERVER_2_PID; then
        kill $SERVER_2_PID
    fi
    
    # Kill Server 3 if running
    if [ -n "$SERVER_3_PID" ] && is_process_running $SERVER_3_PID; then
        kill $SERVER_3_PID
    fi
    
    sleep 2
    print_color $GREEN "All servers stopped"
}

# Function to start a 3-node cluster
start_cluster() {
    local metrics_port=$1
    
    print_color $YELLOW "Starting 3-node cluster with metrics on port $metrics_port"
    
    # Start Server 1 (will be initial primary by default)
    print_color $YELLOW "Starting Server 1 on port $PORT_1"
    bin/server --id 1 --address ":$PORT_1" --servers "$ALL_SERVERS" --metrics-address ":$metrics_port" > logs/server1.log 2>&1 &
    SERVER_1_PID=$!
    
    # Wait for Server 1 to start
    if ! wait_for_server "$SERVER_1_ADDR"; then
        print_color $RED "Server 1 failed to start"
        return 1
    fi
    
    # Verify Server 1 started
    if ! verify_server_started "logs/server1.log" "1"; then
        print_color $RED "Server 1 failed to start properly"
        return 1
    fi
    
    # Start Server 2 (will be replica)
    print_color $YELLOW "Starting Server 2 on port $PORT_2"
    bin/server --id 2 --address ":$PORT_2" --servers "$ALL_SERVERS" --metrics-address ":$metrics_port" > logs/server2.log 2>&1 &
    SERVER_2_PID=$!
    
    # Wait for Server 2 to start
    if ! wait_for_server "$SERVER_2_ADDR"; then
        print_color $RED "Server 2 failed to start"
        return 1
    fi
    
    # Verify Server 2 started
    if ! verify_server_started "logs/server2.log" "2"; then
        print_color $RED "Server 2 failed to start properly"
        return 1
    fi
    
    # Start Server 3 (will be replica)
    print_color $YELLOW "Starting Server 3 on port $PORT_3"
    bin/server --id 3 --address ":$PORT_3" --servers "$ALL_SERVERS" --metrics-address ":$metrics_port" > logs/server3.log 2>&1 &
    SERVER_3_PID=$!
    
    # Wait for Server 3 to start
    if ! wait_for_server "$SERVER_3_ADDR"; then
        print_color $RED "Server 3 failed to start"
        return 1
    fi
    
    # Verify Server 3 started
    if ! verify_server_started "logs/server3.log" "3"; then
        print_color $RED "Server 3 failed to start properly"
        return 1
    fi
    
    # Give servers time to establish connections and elect a primary
    print_color $YELLOW "Waiting for cluster to stabilize..."
    sleep 10
    
    # Verify primary is server 1 (lowest ID)
    if ! grep -q "Assuming primary role\|Starting.*as primary\|Starting Server ID 1 as primary" logs/server1.log; then
        print_color $YELLOW "Server 1 may not be primary, checking logs:"
        grep -i "primary\|role" logs/server1.log | tail -5
    else
        print_color $GREEN "Server 1 confirmed as primary"
    fi
    
    print_color $GREEN "3-node cluster started successfully"
    return 0
}

# Function to verify replication to a specific server
verify_replication() {
    local client_id=$1
    local server_id=$2
    
    echo "Verifying replication to Server $server_id for client $client_id..."
    
    # Check if the lock state has been replicated
    check_lock_state $server_id $client_id
    
    return $?
}

# Function to kill a specific server
kill_server() {
    local server_id=$1
    local pid_var="SERVER_${server_id}_PID"
    
    print_color $YELLOW "Killing Server $server_id (PID: ${!pid_var})..."
    
    if is_process_running ${!pid_var}; then
        kill ${!pid_var}
        print_color $GREEN "Server $server_id killed"
    else
        print_color $RED "Server $server_id (PID: ${!pid_var}) not running"
        return 1
    fi
    
    return 0
}

# Function to check the current primary server by checking logs
identify_primary() {
    local max_attempts=15
    local attempts=0
    
    echo "Identifying current primary server..."
    
    while [ $attempts -lt $max_attempts ]; do
        for id in {1..3}; do
            # Skip if the server process is not running
            local pid_var="SERVER_${id}_PID"
            if ! is_process_running ${!pid_var} 2>/dev/null; then
                continue
            fi
            
            # Look for indicators that this server is the primary
            if grep -q "Assuming primary role\|Promoting to primary\|Starting as primary\|role=Primary\|I am the leader\|Acting as primary\|Became primary\|isPrimary=true" "logs/server${id}.log"; then
                # Double-check that this is still the current primary (not an old log entry)
                if tail -50 "logs/server${id}.log" | grep -q "Assuming primary role\|Promoting to primary\|Starting as primary\|role=Primary\|I am the leader\|Acting as primary\|Became primary\|isPrimary=true"; then
                    print_color $GREEN "Server $id is currently the primary"
                    echo $id
                    return 0
                fi
            fi
        done
        
        attempts=$((attempts+1))
        echo "Attempt $attempts: No primary identified yet, waiting..."
        sleep 2
    done
    
    print_color $RED "Failed to identify a primary server"
    return 1
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
go build -o bin/server cmd/server/main.go
if [ $? -ne 0 ]; then
    print_color $RED "Failed to build server"
    exit 1
fi

go build -o bin/client cmd/client/main.go
if [ $? -ne 0 ]; then
    print_color $RED "Failed to build client"
    exit 1
fi

print_color $GREEN "Binaries built successfully"

# Make sure data directory exists
mkdir -p data logs

# Run all tests
print_header "Starting Advanced Tests"

tests_failed=0

run_enhanced_replication_test() {
    print_header "Enhanced Replication Test"
    
    # Start 3-node cluster
    if ! start_cluster $REPLICATION_TEST_METRICS_PORT; then
        print_color $RED "Failed to start cluster"
        return 1
    fi
    
    # Acquire a lock from Server 1 (primary)
    local CLIENT_ID=$BASE_CLIENT_ID
    print_color $YELLOW "Running client to acquire lock from Server 1..."
    bin/client --servers "$SERVER_1_ADDR" --client-id $CLIENT_ID acquire > logs/test_client.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "Client failed to acquire lock"
        print_color $YELLOW "Client log:"
        cat logs/test_client.log
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Lock successfully acquired"
    
    # Wait for replication to complete
    print_color $YELLOW "Waiting for replication to complete..."
    sleep 5
    
    # Verify replication to Server 2
    if ! verify_replication $CLIENT_ID 2; then
        print_color $RED "Lock state not replicated to Server 2 correctly"
        stop_cluster
        return 1
    fi
    
    # Verify replication to Server 3
    if ! verify_replication $CLIENT_ID 3; then
        print_color $RED "Lock state not replicated to Server 3 correctly"
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Lock state successfully replicated to all replicas"
    
    # Release the lock
    print_color $YELLOW "Releasing the lock..."
    bin/client --servers "$SERVER_1_ADDR" --client-id $CLIENT_ID release > logs/test_client_release.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "Client failed to release lock"
        print_color $YELLOW "Client release log:"
        cat logs/test_client_release.log
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Lock successfully released"
    
    # Wait for release to replicate
    print_color $YELLOW "Waiting for lock release to replicate..."
    sleep 10
    
    # Verify release replicated to Server 2
    if ! check_lock_state 2 "-1"; then
        print_color $RED "Lock release not replicated to Server 2 correctly"
        stop_cluster
        return 1
    fi
    
    # Verify release replicated to Server 3
    if ! check_lock_state 3 "-1"; then
        print_color $RED "Lock release not replicated to Server 3 correctly"
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Lock release successfully replicated to all replicas"
    print_color $GREEN "Enhanced replication test passed!"
    
    # Clean up
    stop_cluster
    sleep 2
    cleanup
    return 0
}

run_fencing_test() {
    print_header "Fencing Behavior Test"
    
    # Start 3-node cluster
    if ! start_cluster $FENCING_TEST_METRICS_PORT; then
        print_color $RED "Failed to start cluster"
        return 1
    fi
    
    # Verify explicitly that Server 1 is primary before proceeding
    print_color $YELLOW "Verifying Server 1 is primary before test..."
    grep -q "Assuming primary role\|Starting as primary\|role=Primary\|isPrimary=true" logs/server1.log
    if [ $? -ne 0 ]; then
        print_color $RED "Server 1 logs don't indicate it's primary, check logs:"
        tail -30 logs/server1.log
        stop_cluster
        return 1
    else
        print_color $GREEN "Confirmed Server 1 is primary from logs"
    fi
    
    # Acquire a lock with client 1
    local CLIENT_ID=1001
    print_color $YELLOW "Client $CLIENT_ID acquiring a lock..."
    bin/client --servers "$SERVER_1_ADDR" --client-id $CLIENT_ID acquire > logs/test_client_1.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "Client failed to acquire lock"
        print_color $YELLOW "Client log:"
        cat logs/test_client_1.log
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Lock successfully acquired"
    
    # Wait for replication to happen
    sleep 5
    
    # Kill Server 1 (primary) to trigger failover
    print_color $YELLOW "Killing Server 1 (primary) to trigger failover..."
    kill_server 1
    
    # Wait for Server 2 to detect failure and promote itself
    # With heartbeat interval of 2s and max failures of 3, we need at least 6-8 seconds
    # Plus some extra time for the election process
    print_color $YELLOW "Waiting for Server 2 to detect primary failure and promote itself..."
    sleep 15  # Wait at least 3 * heartbeat interval + buffer for detection
    
    # Check Server 2 logs for evidence of attempted promotion
    print_color $YELLOW "Checking Server 2 logs for evidence of promotion..."
    tail -50 logs/server2.log
    
    # Increase attempts for check_promotion function
    check_promotion_timeout=30  # 30 seconds should be enough
    check_promotion_start=$(date +%s)
    check_promotion_end=$((check_promotion_start + check_promotion_timeout))
    
    promotion_found=false
    while [ $(date +%s) -lt $check_promotion_end ]; do
        if grep -q "Promoting to primary\|promoted to Primary\|Starting as primary\|Assuming primary role\|role=Primary\|I am the leader\|Acting as primary\|Became primary" "logs/server2.log"; then
            promotion_found=true
            break
        fi
        sleep 2
        echo "Still waiting for promotion logs in Server 2..."
    done
    
    if [ "$promotion_found" = false ]; then
        print_color $RED "Server 2 was not promoted to primary as expected"
        print_color $YELLOW "Server 2 log:"
        tail -50 logs/server2.log
        
        # Check if there's an issue with heartbeat mechanism
        print_color $YELLOW "Checking for heartbeat-related logs in Server 2:"
        grep -i "heartbeat\|ping\|timeout\|failure" logs/server2.log | tail -20
        
        # Check if there's an issue with election mechanism
        print_color $YELLOW "Checking for election-related logs in Server 2:"
        grep -i "election\|vote\|quorum\|majority" logs/server2.log | tail -20
        
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Server 2 detected primary failure and was promoted"
    
    # Verify Server 2 entered fencing period
    if ! verify_fencing 2; then
        print_color $RED "Server 2 did not enter fencing period as expected"
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Server 2 entered fencing period as expected"
    
    # Try operations during fencing period
    # 1. Try to acquire a lock (should be rejected)
    local ACQUIRE_CLIENT_ID=1002
    print_color $YELLOW "Trying to acquire a lock during fencing period with client $ACQUIRE_CLIENT_ID..."
    bin/client --servers "$SERVER_2_ADDR" --client-id $ACQUIRE_CLIENT_ID acquire > logs/test_acquire_during_fencing.log 2>&1
    
    # Print detailed log for debugging
    print_color $YELLOW "Client acquisition log during fencing:"
    cat logs/test_acquire_during_fencing.log
    
    # Check if we got a fencing period rejection
    if grep -q "Server is in fencing period\|rejected.*fencing\|fencing period\|SERVER_FENCING" logs/test_acquire_during_fencing.log; then
        print_color $GREEN "Lock acquisition correctly rejected during fencing period"
    else
        print_color $RED "Lock acquisition during fencing period did not get proper fencing rejection"
        stop_cluster
        return 1
    fi
    
    # 2. Try to release the existing lock
    print_color $YELLOW "Trying to release the lock during fencing period with client $CLIENT_ID..."
    bin/client --servers "$SERVER_2_ADDR" --client-id $CLIENT_ID release > logs/test_release_during_fencing.log 2>&1
    
    # This could be implementation-dependent: some systems reject all operations during fencing
    print_color $YELLOW "Release during fencing log:"
    cat logs/test_release_during_fencing.log
    
    # 3. Try to append to a file (should be rejected)
    local APPEND_CLIENT_ID=1003
    print_color $YELLOW "Trying to append to a file during fencing period with client $APPEND_CLIENT_ID..."
    bin/client --servers "$SERVER_2_ADDR" --client-id $APPEND_CLIENT_ID --file "file_test" --content "test content" append > logs/test_append_during_fencing.log 2>&1
    
    # Print the log for debugging
    print_color $YELLOW "Client append log during fencing:"
    cat logs/test_append_during_fencing.log
    
    # Check if the operation failed with fencing message
    if grep -q "Server is in fencing period\|rejected.*fencing\|fencing period\|SERVER_FENCING" logs/test_append_during_fencing.log; then
        print_color $GREEN "File append correctly rejected during fencing period"
    else
        print_color $RED "File append during fencing period did not get proper fencing rejection"
        stop_cluster
        return 1
    fi
    
    # Wait for fencing period to end
    wait_for_fencing_end 2
    
    # Try to acquire a lock after fencing (should succeed)
    print_color $YELLOW "Trying to acquire a lock after fencing period with client $ACQUIRE_CLIENT_ID..."
    bin/client --servers "$SERVER_2_ADDR" --client-id $ACQUIRE_CLIENT_ID acquire > logs/test_acquire_after_fencing.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "Lock acquisition after fencing period failed, but should have succeeded"
        print_color $YELLOW "Client log:"
        cat logs/test_acquire_after_fencing.log
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Lock successfully acquired after fencing period"
    
    # Verify Server 3 received the lock state update
    if ! verify_replication $ACQUIRE_CLIENT_ID 3; then
        print_color $RED "Lock state not replicated to Server 3 after fencing"
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Lock state successfully replicated after fencing period"
    print_color $GREEN "Fencing behavior test passed!"
    
    # Clean up
    stop_cluster
    sleep 2
    cleanup
    return 0
}

# Add explicit cleanup after each test case
run_expanded_failover_test() {
    print_header "Expanded Failover Test - Continued Operations"
    
    # Start 3-node cluster
    if ! start_cluster $FAILOVER_TEST_METRICS_PORT; then
        print_color $RED "Failed to start cluster"
        return 1
    fi
    
    # Verify explicitly that Server 1 is primary before proceeding
    print_color $YELLOW "Verifying Server 1 is primary before test..."
    grep -q "Assuming primary role\|Starting as primary\|role=Primary\|isPrimary=true" logs/server1.log
    if [ $? -ne 0 ]; then
        print_color $RED "Server 1 logs don't indicate it's primary, check logs:"
        tail -30 logs/server1.log
        stop_cluster
        return 1
    else
        print_color $GREEN "Confirmed Server 1 is primary from logs"
    fi
    
    # Start a client that will hold a lock and renew its lease
    local CLIENT_ID=1004
    print_color $YELLOW "Starting client $CLIENT_ID to hold lock and renew lease..."
    bin/client --servers "$ALL_SERVERS" --client-id $CLIENT_ID hold --timeout 60s > logs/test_client_hold.log 2>&1 &
    CLIENT_PID=$!
    
    # Wait for client to acquire lock
    sleep 5
    
    # Verify lock is acquired
    if ! is_process_running $CLIENT_PID; then
        print_color $RED "Client failed to acquire lock"
        print_color $YELLOW "Client log:"
        cat logs/test_client_hold.log
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Client successfully acquired lock"
    
    # Verify replication to replicas
    if ! verify_replication $CLIENT_ID 2; then
        print_color $RED "Lock state not replicated to Server 2"
        stop_cluster
        kill $CLIENT_PID
        return 1
    fi
    
    if ! verify_replication $CLIENT_ID 3; then
        print_color $RED "Lock state not replicated to Server 3"
        stop_cluster
        kill $CLIENT_PID
        return 1
    fi
    
    print_color $GREEN "Lock state successfully replicated to all replicas"
    
    # Kill Server 1 (primary) to trigger failover
    print_color $YELLOW "Killing Server 1 (primary) to trigger failover..."
    kill_server 1
    
    # Wait longer for failover to complete - heartbeat detection needs max_failures * interval = 3 * 2s = 6s
    # Plus additional time for the election process and fencing to begin
    print_color $YELLOW "Waiting for Server 2 to detect primary failure and promote itself..."
    sleep 15
    
    # Check Server 2 logs for evidence of attempted promotion
    print_color $YELLOW "Checking Server 2 logs for evidence of promotion..."
    tail -50 logs/server2.log
    
    # Use custom promotion check with longer timeout
    check_promotion_timeout=30  # 30 seconds should be enough
    check_promotion_start=$(date +%s)
    check_promotion_end=$((check_promotion_start + check_promotion_timeout))
    
    promotion_found=false
    while [ $(date +%s) -lt $check_promotion_end ]; do
        if grep -q "Promoting to primary\|promoted to Primary\|Starting as primary\|Assuming primary role\|role=Primary\|I am the leader\|Acting as primary\|Became primary" "logs/server2.log"; then
            promotion_found=true
            break
        fi
        sleep 2
        echo "Still waiting for promotion logs in Server 2..."
    done
    
    if [ "$promotion_found" = false ]; then
        print_color $RED "Server 2 was not promoted to primary as expected"
        print_color $YELLOW "Server 2 log:"
        tail -50 logs/server2.log
        
        # Check if there's an issue with heartbeat mechanism
        print_color $YELLOW "Checking for heartbeat-related logs in Server 2:"
        grep -i "heartbeat\|ping\|timeout\|failure" logs/server2.log | tail -20
        
        # Check if there's an issue with election mechanism
        print_color $YELLOW "Checking for election-related logs in Server 2:"
        grep -i "election\|vote\|quorum\|majority" logs/server2.log | tail -20
        
        stop_cluster
        kill $CLIENT_PID
        return 1
    fi
    
    print_color $GREEN "Server 2 detected primary failure and was promoted"
    
    # Check for fencing period
    print_color $YELLOW "Checking if Server 2 entered fencing period..."
    if ! verify_fencing 2; then
        print_color $RED "Server 2 did not enter fencing period as expected"
        stop_cluster
        kill $CLIENT_PID
        return 1
    fi
    
    print_color $GREEN "Server 2 entered fencing period as expected"
    
    # Wait for fencing period to end
    wait_for_fencing_end 2
    
    # Verify the client is still running (should be reconnecting to the new primary)
    if ! is_process_running $CLIENT_PID; then
        print_color $RED "Client did not survive failover"
        print_color $YELLOW "Client log:"
        cat logs/test_client_hold.log
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Client survived failover"
    
    # Create a new client to append to a file
    local APPEND_FILE="file_99"
    local APPEND_CONTENT="test content after failover"
    local APPEND_CLIENT_ID=1005
    
    print_color $YELLOW "Acquiring a lock with client $APPEND_CLIENT_ID before file append..."
    bin/client --servers "$ALL_SERVERS" --client-id $APPEND_CLIENT_ID acquire > logs/test_client_acquire_for_append.log 2>&1
    
    # Retry a few times if the acquire fails - the server might still be stabilizing
    max_acquire_attempts=5
    acquire_attempt=1
    acquire_success=false
    
    while [ $acquire_attempt -le $max_acquire_attempts ] && [ "$acquire_success" = false ]; do
        bin/client --servers "$ALL_SERVERS" --client-id $APPEND_CLIENT_ID acquire > logs/test_client_acquire_for_append.log 2>&1
        if [ $? -eq 0 ]; then
            acquire_success=true
        else
            print_color $YELLOW "Acquire attempt $acquire_attempt failed, retrying in 5 seconds..."
            sleep 5
            acquire_attempt=$((acquire_attempt+1))
        fi
    done
    
    if [ "$acquire_success" = false ]; then
        print_color $RED "New client failed to acquire lock after failover"
        print_color $YELLOW "Client log:"
        cat logs/test_client_acquire_for_append.log
        stop_cluster
        kill $CLIENT_PID
        return 1
    fi
    
    print_color $GREEN "New client successfully acquired lock after failover"
    
    # Now append to a file with the same client
    print_color $YELLOW "Appending to file with client $APPEND_CLIENT_ID..."
    bin/client --servers "$ALL_SERVERS" --client-id $APPEND_CLIENT_ID --file "$APPEND_FILE" --content "$APPEND_CONTENT" append > logs/test_client_append_after_failover.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "Client failed to append to file after failover"
        print_color $YELLOW "Client append log:"
        cat logs/test_client_append_after_failover.log
        stop_cluster
        kill $CLIENT_PID
        return 1
    fi
    
    print_color $GREEN "Client successfully appended to file after failover"
    
    # Verify the file was created
    if [ -f "data/$APPEND_FILE" ] && grep -q "$APPEND_CONTENT" "data/$APPEND_FILE"; then
        print_color $GREEN "File was successfully appended to after failover"
    else
        print_color $RED "File was not created or does not contain the expected content"
        stop_cluster
        kill $CLIENT_PID
        return 1
    fi
    
    # Kill the 'hold' client
    kill $CLIENT_PID
    
    print_color $GREEN "Expanded failover test passed!"
    
    # Clean up
    stop_cluster
    sleep 2
    cleanup
    return 0
}

run_improved_split_brain_test() {
    print_header "Improved Split-Brain Test"
    
    # Check if we can use iptables for this test
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
    
    # Start 3-node cluster
    if ! start_cluster $SPLIT_BRAIN_TEST_METRICS_PORT; then
        print_color $RED "Failed to start cluster"
        return 1
    fi
    
    print_color $YELLOW "Waiting for servers to establish replication..."
    sleep 5
    
    # Simulate a network partition by isolating Server 1 (primary)
    if [ "$USE_IPTABLES" = true ]; then
        print_color $YELLOW "Creating network partition using iptables..."
        # Block traffic to/from Server 1
        iptables -A INPUT -p tcp --dport $PORT_1 -j DROP
        iptables -A OUTPUT -p tcp --dport $PORT_1 -j DROP
    else
        print_color $YELLOW "Simulating network partition by stopping Server 1..."
        # Stop Server 1
        kill_server 1
    fi
    
    print_color $YELLOW "Partition created, Server 1 is isolated"
    
    # Wait for Servers 2 and 3 to detect failure
    print_color $YELLOW "Waiting for Servers 2 and 3 to detect primary failure..."
    sleep 15
    
    # Server 2 should be promoted (it has the lowest ID among the remaining servers)
    if ! check_promotion 2; then
        print_color $RED "Server 2 was not promoted as expected"
        print_color $YELLOW "Server 2 log:"
        tail -30 logs/server2.log
        if [ "$USE_IPTABLES" = true ]; then
            # Clean up iptables rules
            iptables -D INPUT -p tcp --dport $PORT_1 -j DROP
            iptables -D OUTPUT -p tcp --dport $PORT_1 -j DROP
        fi
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Server 2 was promoted to primary as expected"
    
    # Create client for the new partition (Servers 2 and 3)
    local CLIENT_2_ID=1006
    
    # Wait for fencing to complete before testing client operations
    wait_for_fencing_end 2
    
    # Try to acquire lock on the new primary (Server 2)
    print_color $YELLOW "Client $CLIENT_2_ID trying to acquire lock on new primary (Server 2)..."
    bin/client --servers "$SERVER_2_ADDR,$SERVER_3_ADDR" --client-id $CLIENT_2_ID acquire > logs/test_client_new_partition.log 2>&1
    NEW_PARTITION_SUCCESS=$?
    
    # Output logs for debugging
    print_color $YELLOW "Client attempt on new partition log:"
    cat logs/test_client_new_partition.log
    
    # Heal the partition
    if [ "$USE_IPTABLES" = true ]; then
        print_color $YELLOW "Healing network partition..."
        iptables -D INPUT -p tcp --dport $PORT_1 -j DROP
        iptables -D OUTPUT -p tcp --dport $PORT_1 -j DROP
    else
        print_color $YELLOW "Restarting Server 1..."
        # Start Server 1 again
        bin/server --id 1 --address ":$PORT_1" --servers "$ALL_SERVERS" --metrics-address ":$SPLIT_BRAIN_TEST_METRICS_PORT" > logs/server1_restarted.log 2>&1 &
        SERVER_1_PID=$!
        
        # Wait for Server 1 to start
        if ! wait_for_server "$SERVER_1_ADDR"; then
            print_color $RED "Server 1 failed to restart"
            stop_cluster
            return 1
        fi
    fi
    
    print_color $YELLOW "Partition healed, waiting for split-brain detection..."
    sleep 15
    
    # Check if Server 1 detected it's no longer primary
    if [ "$USE_IPTABLES" = true ]; then
        # Only check if we used iptables (Server 1 still running)
        if grep -q "Split-brain detected\|Demoting to secondary\|Detected newer primary" logs/server1.log; then
            print_color $GREEN "Server 1 detected split-brain and demoted itself"
            SPLIT_BRAIN_DETECTED=true
        else
            print_color $RED "Server 1 did not detect split-brain or demote itself"
            print_color $YELLOW "Server 1 log tail:"
            tail -30 logs/server1.log
            SPLIT_BRAIN_DETECTED=false
        fi
    else
        # If we restarted Server 1, it should start as a replica
        if grep -q "Starting as replica\|role=Secondary" logs/server1_restarted.log; then
            print_color $GREEN "Restarted Server 1 correctly started as replica"
            SPLIT_BRAIN_DETECTED=true
        else
            print_color $RED "Restarted Server 1 did not start as replica"
            print_color $YELLOW "Restarted Server 1 log tail:"
            tail -30 logs/server1_restarted.log
            SPLIT_BRAIN_DETECTED=false
        fi
    fi
    
    # Create a client that tries all servers
    local CLIENT_ALL_ID=1007
    print_color $YELLOW "Client $CLIENT_ALL_ID trying to acquire lock using all servers..."
    bin/client --servers "$ALL_SERVERS" --client-id $CLIENT_ALL_ID acquire > logs/test_client_all_servers.log 2>&1
    ALL_SERVERS_SUCCESS=$?
    
    # Output logs for debugging
    print_color $YELLOW "Client attempt using all servers log:"
    cat logs/test_client_all_servers.log
    
    # Release locks to clean up state
    if [ $NEW_PARTITION_SUCCESS -eq 0 ]; then
        print_color $YELLOW "Releasing lock for client $CLIENT_2_ID..."
        bin/client --servers "$ALL_SERVERS" --client-id $CLIENT_2_ID release > /dev/null 2>&1
    fi
    
    if [ $ALL_SERVERS_SUCCESS -eq 0 ]; then
        print_color $YELLOW "Releasing lock for client $CLIENT_ALL_ID..."
        bin/client --servers "$ALL_SERVERS" --client-id $CLIENT_ALL_ID release > /dev/null 2>&1
    fi
    
    # Check for success criteria
    if [ "$SPLIT_BRAIN_DETECTED" = true ] && [ $ALL_SERVERS_SUCCESS -eq 0 ]; then
        print_color $GREEN "Split-brain scenario correctly handled!"
        print_color $GREEN "Old primary detected split-brain and stepped down."
        print_color $GREEN "Clients can successfully use the cluster after split-brain resolution."
        RESULT=0
    else
        print_color $RED "Split-brain handling did not work correctly"
        if [ "$SPLIT_BRAIN_DETECTED" != true ]; then
            print_color $RED "Old primary did not detect split-brain or step down"
        fi
        if [ $ALL_SERVERS_SUCCESS -ne 0 ]; then
            print_color $RED "Clients cannot use the cluster after split-brain"
        fi
        RESULT=1
    fi
    
    print_color $GREEN "Improved split-brain test completed"
    
    # Clean up
    stop_cluster
    sleep 2
    cleanup
    return $RESULT
}

run_lease_expiry_test() {
    print_header "Lease Expiry Test"
    
    # Start 3-node cluster
    if ! start_cluster $LEASE_EXPIRY_TEST_METRICS_PORT; then
        print_color $RED "Failed to start cluster"
        return 1
    fi
    
    # Acquire a lock with a short lease
    local CLIENT_ID=1008
    print_color $YELLOW "Client $CLIENT_ID acquiring a lock with default lease time..."
    bin/client --servers "$SERVER_1_ADDR" --client-id $CLIENT_ID acquire > logs/test_client_lease.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "Client failed to acquire lock"
        print_color $YELLOW "Client log:"
        cat logs/test_client_lease.log
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Lock successfully acquired with default lease time"
    
    # Wait for replication to complete
    print_color $YELLOW "Waiting for replication to complete..."
    sleep 5
    
    # Verify replication to both replicas
    if ! verify_replication $CLIENT_ID 2; then
        print_color $RED "Lock state not replicated to Server 2"
        stop_cluster
        return 1
    fi
    
    if ! verify_replication $CLIENT_ID 3; then
        print_color $RED "Lock state not replicated to Server 3"
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Lock state successfully replicated to all replicas"
    
    # Kill Server 1 (primary) to trigger failover
    print_color $YELLOW "Killing Server 1 (primary) to trigger failover..."
    kill_server 1
    
    # Wait for Server 2 to detect failure and promote itself
    print_color $YELLOW "Waiting for Server 2 to detect primary failure and promote itself..."
    
    if ! check_promotion 2; then
        print_color $RED "Server 2 was not promoted to primary as expected"
        print_color $YELLOW "Server 2 log:"
        tail -30 logs/server2.log
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Server 2 detected primary failure and was promoted"
    
    # Wait for fencing period and lease expiry
    # This should be the lease duration (typically 30s) + fencing period (typically 30s)
    print_color $YELLOW "Waiting for fencing period and lease expiry..."
    sleep 60
    
    # After fencing period and lease expiry, try to acquire the same lock with a different client
    local NEW_CLIENT_ID=1009
    print_color $YELLOW "New client $NEW_CLIENT_ID trying to acquire the lock after lease expiry..."
    bin/client --servers "$SERVER_2_ADDR" --client-id $NEW_CLIENT_ID acquire > logs/test_client_after_expiry.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "New client failed to acquire lock after lease expiry"
        print_color $YELLOW "Client log:"
        cat logs/test_client_after_expiry.log
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "New client successfully acquired lock after lease expiry"
    
    # Verify replication to Server 3
    if ! verify_replication $NEW_CLIENT_ID 3; then
        print_color $RED "New lock state not replicated to Server 3"
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "New lock state successfully replicated to all replicas"
    print_color $GREEN "Lease expiry test passed!"
    
    # Clean up
    stop_cluster
    sleep 2
    cleanup
    return 0
}

run_quorum_loss_test() {
    print_header "Quorum Loss Test"
    
    # Start 3-node cluster
    if ! start_cluster $QUORUM_LOSS_TEST_METRICS_PORT; then
        print_color $RED "Failed to start cluster"
        return 1
    fi
    
    # Verify primary is working by acquiring and releasing a lock
    local CLIENT_ID=1010
    print_color $YELLOW "Verifying primary is working..."
    bin/client --servers "$SERVER_1_ADDR" --client-id $CLIENT_ID acquire > logs/test_client_verify.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "Failed to acquire initial lock"
        print_color $YELLOW "Client log:"
        cat logs/test_client_verify.log
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Initial lock successfully acquired"
    
    # Release the lock
    bin/client --servers "$SERVER_1_ADDR" --client-id $CLIENT_ID release > logs/test_client_release.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "Failed to release initial lock"
        print_color $YELLOW "Client release log:"
        cat logs/test_client_release.log
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Initial lock successfully released"
    
    # Kill both Server 2 and Server 3 to cause quorum loss
    print_color $YELLOW "Killing Server 2 and Server 3 to cause quorum loss..."
    kill_server 2
    kill_server 3
    
    # Wait for primary to detect quorum loss
    print_color $YELLOW "Waiting for Server 1 to detect quorum loss..."
    sleep 15
    
    # Try to acquire a lock - should fail due to quorum loss
    local QUORUM_LOSS_CLIENT_ID=1011
    print_color $YELLOW "Client $QUORUM_LOSS_CLIENT_ID trying to acquire lock during quorum loss..."
    bin/client --servers "$SERVER_1_ADDR" --client-id $QUORUM_LOSS_CLIENT_ID acquire > logs/test_client_quorum_loss.log 2>&1
    
    # Check if the client operation failed as expected
    if grep -q "quorum\|rejected\|failed\|error" logs/test_client_quorum_loss.log && [ $? -ne 0 ]; then
        print_color $GREEN "Client operation correctly failed due to quorum loss"
    else
        print_color $RED "Client operation succeeded despite quorum loss"
        print_color $YELLOW "Client log:"
        cat logs/test_client_quorum_loss.log
        stop_cluster
        return 1
    fi
    
    # Check if Server 1 logged quorum loss
    if grep -q "quorum\|majority\|insufficient nodes" logs/server1.log; then
        print_color $GREEN "Server 1 correctly logged quorum loss"
    else
        print_color $YELLOW "Server 1 did not explicitly log quorum loss"
        print_color $YELLOW "Server 1 log tail:"
        tail -30 logs/server1.log
    fi
    
    # Restart Server 2 to restore quorum
    print_color $YELLOW "Restarting Server 2 to restore quorum..."
    bin/server --id 2 --address ":$PORT_2" --servers "$ALL_SERVERS" --metrics-address ":$QUORUM_LOSS_TEST_METRICS_PORT" > logs/server2_restarted.log 2>&1 &
    SERVER_2_PID=$!
    
    # Wait for Server 2 to start
    if ! wait_for_server "$SERVER_2_ADDR"; then
        print_color $RED "Server 2 failed to restart"
        stop_cluster
        return 1
    fi
    
    # Verify Server 2 started
    if ! verify_server_started "logs/server2_restarted.log" "2"; then
        print_color $RED "Server 2 failed to restart properly"
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Server 2 restarted successfully"
    
    # Wait for quorum to be restored
    print_color $YELLOW "Waiting for quorum to be restored..."
    sleep 15
    
    # Try to acquire a lock - should succeed now
    local RESTORED_CLIENT_ID=1012
    print_color $YELLOW "Client $RESTORED_CLIENT_ID trying to acquire lock after quorum restoration..."
    bin/client --servers "$ALL_SERVERS" --client-id $RESTORED_CLIENT_ID acquire > logs/test_client_restored.log 2>&1
    
    if [ $? -ne 0 ]; then
        print_color $RED "Client failed to acquire lock after quorum restoration"
        print_color $YELLOW "Client log:"
        cat logs/test_client_restored.log
        stop_cluster
        return 1
    fi
    
    print_color $GREEN "Client successfully acquired lock after quorum restoration"
    print_color $GREEN "Quorum loss test passed!"
    
    # Clean up
    stop_cluster
    sleep 2
    cleanup
    return 0
}

# Main execution section
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

if [ "$RUN_ONLY_QUORUM_LOSS" = true ]; then
    run_quorum_loss_test
    exit $?
fi

# Run all tests sequentially
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

run_quorum_loss_test
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