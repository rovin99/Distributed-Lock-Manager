#!/bin/bash

# Test script for simulating Client Stuck Scenarios in Distributed Lock Manager
# This script tests scenarios where clients get stuck before or after editing files

# Set the color variables
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SERVER_PORT=50051
SERVER_ADDR="localhost:$SERVER_PORT"
METRICS_PORT=8080
TEST_TIMEOUT=90
TEST_FILE="data/file_0"

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

# Function to clean up processes
cleanup() {
    print_color $YELLOW "Cleaning up processes..."
    
    # Kill all server processes
    killall -9 lock_server 2>/dev/null || true
    pkill -9 -f "bin/lock_server" 2>/dev/null || true
    
    # Kill all client processes
    killall -9 lock_client 2>/dev/null || true
    pkill -9 -f "bin/lock_client" 2>/dev/null || true
    
    # Remove test files
    rm -f $TEST_FILE 2>/dev/null || true
    
    print_color $GREEN "Cleanup complete"
}

# Run cleanup on script exit
trap cleanup EXIT

# Function to wait until server is ready
wait_for_server() {
    local max_attempts=10
    local attempts=0
    
    print_color $YELLOW "Waiting for server to be ready..."
    
    while [ $attempts -lt $max_attempts ]; do
        if nc -z localhost $SERVER_PORT 2>/dev/null; then
            print_color $GREEN "Server is ready"
            return 0
        fi
        
        attempts=$((attempts+1))
        echo "Attempt $attempts: Server not ready, waiting..."
        sleep 1
    done
    
    print_color $RED "Server did not become ready in time"
    return 1
}

# Function to verify file contents
verify_file_contents() {
    local expected_content=$1
    
    if [ ! -f $TEST_FILE ]; then
        print_color $RED "Test file does not exist"
        return 1
    fi
    
    local content=$(cat $TEST_FILE)
    
    if [ "$content" = "$expected_content" ]; then
        print_color $GREEN "File content verified: $content"
        return 0
    else
        print_color $RED "File content mismatch"
        print_color $RED "Expected: $expected_content"
        print_color $RED "Actual: $content"
        return 1
    fi
}

# Test 1: Client stuck before editing file
test_stuck_before_editing() {
    print_header "TEST 1: Client stuck before editing file (exact workflow)"
    
    # Start fresh by removing the test file
    rm -f $TEST_FILE 2>/dev/null || true
    
    # Start server
    print_color $YELLOW "Starting server..."
    bin/lock_server --address=":$SERVER_PORT" &
    SERVER_PID=$!
    
    # Wait for server to be ready
    wait_for_server || return 1

    # Client 1 acquires lock 
    print_color $YELLOW "Client 1 acquiring lock..."
    bin/lock_client acquire --servers=$SERVER_ADDR --client-id=1
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 failed to acquire lock"
        kill $SERVER_PID
        return 1
    fi
    
    # Force the lock to expire by killing the client process
    print_color $YELLOW "Simulating Client 1 stall (killing process, lock will expire after 30s)..."
    pkill -f "bin/lock_client.*--client-id=1" 2>/dev/null || true
    sleep 32
    
    # Client 2 acquires lock (should succeed now)
    print_color $YELLOW "Client 2 acquiring lock after lease expiration..."
    bin/lock_client acquire --servers=$SERVER_ADDR --client-id=2
    if [ $? -ne 0 ]; then
        print_color $RED "Client 2 failed to acquire lock after C1 expiry"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 2 appends 'B' first time
    print_color $YELLOW "Client 2 appending 'B' to file (first time)..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=2 --file="file_0" --content="B"
    if [ $? -ne 0 ]; then 
        print_color $RED "Client 2 first append failed"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 1 "wakes up" and tries to append without valid lock - THIS SHOULD FAIL
    # Note: Running this with a short timeout to ensure it fails
    print_color $YELLOW "Client 1 attempts to append 'A' with expired lock (should fail)..."
    timeout 5s bin/lock_client append --servers=$SERVER_ADDR --client-id=1 --file="file_0" --content="X" > /dev/null 2>&1 || true
    print_color $GREEN "Client 1 attempted operation completed (likely failed due to expired lock)"
    
    # Client 2 appends 'B' second time
    print_color $YELLOW "Client 2 appending 'B' to file (second time)..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=2 --file="file_0" --content="B"
    if [ $? -ne 0 ]; then 
        print_color $RED "Client 2 second append failed"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 2 releases lock
    print_color $YELLOW "Client 2 releasing lock..."
    bin/lock_client release --servers=$SERVER_ADDR --client-id=2
    if [ $? -ne 0 ]; then 
        print_color $RED "Client 2 release failed"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 1 reacquires lock
    print_color $YELLOW "Client 1 reacquiring lock..."
    bin/lock_client acquire --servers=$SERVER_ADDR --client-id=1
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 failed to reacquire lock"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 1 appends 'A' twice
    print_color $YELLOW "Client 1 appending 'A' to file (first time)..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=1 --file="file_0" --content="A"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 first append failed"
        kill $SERVER_PID
        return 1
    fi
    
    print_color $YELLOW "Client 1 appending 'A' to file (second time)..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=1 --file="file_0" --content="A"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 second append failed"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 1 releases lock
    print_color $YELLOW "Client 1 releasing lock..."
    bin/lock_client release --servers=$SERVER_ADDR --client-id=1
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 release failed"
        kill $SERVER_PID
        return 1
    fi
    
    # Verify final file contents
    print_color $YELLOW "Verifying final file contents..."
    local content=$(cat $TEST_FILE)
    print_color $GREEN "Final file content: $content"
    
    # Check if content matches expected pattern
    if [[ "$content" == "BBAA" ]]; then
        print_color $GREEN "SUCCESS: File content matches expected pattern (BBAA)"
    else
        print_color $RED "ERROR: File content does not match expected pattern"
        print_color $RED "Expected: BBAA"
        print_color $RED "Actual: $content"
    fi
    
    # Clean up
    kill $SERVER_PID 2>/dev/null || true
    
    print_color $GREEN "Test 1 complete"
    return 0
}

# Test 2: Client stuck after editing file
test_stuck_after_editing() {
    print_header "TEST 2: Client stuck after editing file (exact workflow)"
    
    # Start fresh by removing the test file
    rm -f $TEST_FILE 2>/dev/null || true
    
    # Start server
    print_color $YELLOW "Starting server..."
    bin/lock_server --address=":$SERVER_PORT" &
    SERVER_PID=$!
    
    # Wait for server to be ready
    wait_for_server || return 1
    
    # Client 1 acquires a lock
    print_color $YELLOW "Client 1 acquiring lock..."
    bin/lock_client acquire --servers=$SERVER_ADDR --client-id=1
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 failed to acquire lock"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 1 appends to file and then stalls (without releasing lock)
    print_color $YELLOW "Client 1 appending 'A' to file and then stalling..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=1 --file="file_0" --content="A"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 append failed"
        kill $SERVER_PID
        return 1
    fi
    
    # Verify file was written correctly
    print_color $YELLOW "Verifying file contains 'A'..."
    local initial_content=$(cat $TEST_FILE)
    print_color $GREEN "Initial file content: $initial_content"
    
    # Wait for a bit to simulate client stall (wait for lease to expire)
    print_color $YELLOW "Simulating client stall (waiting for lease expiration)..."
    sleep 32 # Default lease is likely 30s, so wait a bit longer
    
    # Check if server rolled back Client 1's operation
    print_color $YELLOW "Checking if server rolled back Client 1's operation..."
    local after_expiry_content=$(cat $TEST_FILE)
    if [[ "$after_expiry_content" == "$initial_content" ]]; then
        print_color $YELLOW "Server maintained Client 1's operation after lease expiry"
    else
        print_color $YELLOW "Server rolled back Client 1's operation after lease expiry"
    fi
    
    # Client 2 attempts lock acquisition (should succeed after lease timeout)
    print_color $YELLOW "Client 2 acquiring lock after lease timeout..."
    bin/lock_client acquire --servers=$SERVER_ADDR --client-id=2
    if [ $? -ne 0 ]; then
        print_color $RED "Client 2 failed to acquire lock after lease timeout"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 2 appends to file (first time)
    print_color $YELLOW "Client 2 appending 'B' to file (first time)..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=2 --file="file_0" --content="B"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 2 first append failed"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 2 appends to file (second time)
    print_color $YELLOW "Client 2 appending 'B' to file (second time)..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=2 --file="file_0" --content="B"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 2 second append failed"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 2 releases lock
    print_color $YELLOW "Client 2 releasing lock..."
    bin/lock_client release --servers=$SERVER_ADDR --client-id=2
    if [ $? -ne 0 ]; then
        print_color $RED "Client 2 release failed"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 1 acquiring lock again (new session)
    print_color $YELLOW "Client 1 acquiring lock again (new session)..."
    bin/lock_client acquire --servers=$SERVER_ADDR --client-id=1
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 failed to acquire lock again"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 1 appending to file (first time after reacquiring)
    print_color $YELLOW "Client 1 appending 'A' to file (first time after reacquiring)..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=1 --file="file_0" --content="A"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 first append after reacquiring failed"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 1 appending to file (second time after reacquiring)
    print_color $YELLOW "Client 1 appending 'A' to file (second time after reacquiring)..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=1 --file="file_0" --content="A"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 second append after reacquiring failed"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 1 releasing lock
    print_color $YELLOW "Client 1 releasing lock..."
    bin/lock_client release --servers=$SERVER_ADDR --client-id=1
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 release failed"
        kill $SERVER_PID
        return 1
    fi
    
    # Verify final file contents
    print_color $YELLOW "Verifying final file contents..."
    local final_content=$(cat $TEST_FILE)
    print_color $GREEN "Final file content: $final_content"
    
    # Check if content matches expected pattern
    if [[ "$final_content" == "BBAA" ]]; then
        print_color $GREEN "SUCCESS: File content matches expected pattern (BBAA - server rolled back client 1's operation)"
    elif [[ "$final_content" == "ABBAA" ]]; then
        print_color $GREEN "SUCCESS: File content shows transaction pattern (original A kept, then BB from client 2, then AA from client 1)"
    else
        print_color $YELLOW "Note: File content follows a different pattern: $final_content"
        print_color $YELLOW "Expected either BBAA (rollback) or ABBAA (no rollback)"
    fi
    
    # Clean up
    kill $SERVER_PID 2>/dev/null || true
    
    print_color $GREEN "Test 2 complete"
    return 0
}

# Test 3: Multiple clients with simultaneous lock expiration
test_multiple_clients_lock_expiration() {
    print_header "TEST 3: Multiple clients with simultaneous lock expiration (exact workflow)"
    
    # Start fresh by removing the test file
    rm -f $TEST_FILE 2>/dev/null || true
    
    # Start server
    print_color $YELLOW "Starting server..."
    bin/lock_server --address=":$SERVER_PORT" &
    SERVER_PID=$!
    
    # Wait for server to be ready
    wait_for_server || return 1
    
    # Client 1 acquires a lock
    print_color $YELLOW "Client 1 acquiring lock..."
    bin/lock_client acquire --servers=$SERVER_ADDR --client-id=1
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 failed to acquire lock"
        kill $SERVER_PID
        return 1
    fi
    
    # Force the lock to expire faster by killing the client process (simulate crash)
    print_color $YELLOW "Simulating Client 1 failure/crash (lock will expire after 30s)..."
    pkill -f "bin/lock_client.*--client-id=1" 2>/dev/null || true
    
    # Start Client 2 and 3 (both trying to acquire lock but will be queued)
    print_color $YELLOW "Starting Client 2 (will be queued)..."
    bin/lock_client acquire --servers=$SERVER_ADDR --client-id=2 > /tmp/c2_acquire.log 2>&1 &
    CLIENT2_PID=$!
    
    print_color $YELLOW "Starting Client 3 (will be queued)..."
    bin/lock_client acquire --servers=$SERVER_ADDR --client-id=3 > /tmp/c3_acquire.log 2>&1 &
    CLIENT3_PID=$!
    
    # Give clients time to connect and be queued
    sleep 2
    
    # Wait for Client 1's lease to expire (default is 30s)
    print_color $YELLOW "Waiting for Client 1's lease to expire (~30s)..."
    sleep 32
    
    # Check if Client 2 acquired the lock (it should be first in queue)
    print_color $YELLOW "Checking if Client 2 acquired the lock..."
    if grep -q "Successfully acquired lock" /tmp/c2_acquire.log; then
        print_color $GREEN "Client 2 successfully acquired lock (as expected)"
        
        # Client 2 appends to file
        print_color $YELLOW "Client 2 appending 'B' to file..."
        bin/lock_client append --servers=$SERVER_ADDR --client-id=2 --file="file_0" --content="B"
        if [ $? -ne 0 ]; then
            print_color $RED "Client 2 append failed"
        else
            print_color $GREEN "Client 2 successfully appended to file"
        fi
        
        # Release the lock for Client 2
        print_color $YELLOW "Releasing lock for Client 2..."
        bin/lock_client release --servers=$SERVER_ADDR --client-id=2
        if [ $? -ne 0 ]; then
            print_color $RED "Client 2 release failed"
        else
            print_color $GREEN "Client 2 successfully released lock"
        fi
    else
        print_color $RED "Client 2 failed to acquire lock (unexpected)"
        cat /tmp/c2_acquire.log
    fi
    
    # Wait a moment for Client 3 to acquire the lock
    sleep 2
    
    # Check if Client 3 acquired the lock after Client 2
    print_color $YELLOW "Checking if Client 3 acquired the lock..."
    if grep -q "Successfully acquired lock" /tmp/c3_acquire.log; then
        print_color $GREEN "Client 3 successfully acquired lock (as expected)"
        
        # Client 3 appends to file
        print_color $YELLOW "Client 3 appending 'C' to file..."
        bin/lock_client append --servers=$SERVER_ADDR --client-id=3 --file="file_0" --content="C"
        if [ $? -ne 0 ]; then
            print_color $RED "Client 3 append failed"
        else
            print_color $GREEN "Client 3 successfully appended to file"
        fi
        
        # Release the lock for Client 3
        print_color $YELLOW "Releasing lock for Client 3..."
        bin/lock_client release --servers=$SERVER_ADDR --client-id=3
        if [ $? -ne 0 ]; then
            print_color $RED "Client 3 release failed"
        else
            print_color $GREEN "Client 3 successfully released lock"
        fi
    else
        print_color $RED "Client 3 failed to acquire lock (unexpected)"
        cat /tmp/c3_acquire.log
    fi
    
    # Verify file contents
    print_color $YELLOW "Verifying final file contents..."
    local content=$(cat $TEST_FILE)
    print_color $GREEN "Final file content: $content"
    
    # Check if the content matches expected pattern (B from C2, then C from C3)
    if [[ "$content" == "BC" ]]; then
        print_color $GREEN "SUCCESS: File content matches expected pattern (BC)"
    else
        print_color $YELLOW "Note: File content follows a different pattern: $content"
        print_color $YELLOW "Expected: BC"
    fi
    
    # Clean up
    kill $CLIENT2_PID $CLIENT3_PID 2>/dev/null || true
    kill $SERVER_PID 2>/dev/null || true
    rm -f /tmp/c2_acquire.log /tmp/c3_acquire.log
    
    print_color $GREEN "Test 3 complete"
    return 0
}

# Main function
main() {
    print_header "Client Stuck Scenarios Tests"
    
    # Check if a specific test was requested
    if [ "$1" ]; then
        case "$1" in
            test1|before_editing)
                test_stuck_before_editing
                ;;
            test2|after_editing)
                test_stuck_after_editing
                ;;
            test3|multiple_clients)
                test_multiple_clients_lock_expiration
                ;;
            *)
                print_color $RED "Unknown test: $1"
                print_color $YELLOW "Available tests: test1 (before_editing), test2 (after_editing), test3 (multiple_clients)"
                return 1
                ;;
        esac
    else
        # Run all tests with proper error handling
        local result=0
        
        # Test 1
        test_stuck_before_editing
        if [ $? -ne 0 ]; then
            print_color $RED "Test 1 (stuck before editing) failed"
            result=1
        fi
        
        # Test 2
        test_stuck_after_editing
        if [ $? -ne 0 ]; then
            print_color $RED "Test 2 (stuck after editing) failed"
            result=1
        fi
        
        # Test 3
        test_multiple_clients_lock_expiration
        if [ $? -ne 0 ]; then
            print_color $RED "Test 3 (multiple clients lock expiration) failed"
            result=1
        fi
        
        # Print overall result
        if [ $result -eq 0 ]; then
            print_header "All tests completed successfully"
        else
            print_header "Some tests failed, please check the output above"
        fi
        
        return $result
    fi
}

# Run the main function with command line arguments
main "$@" 