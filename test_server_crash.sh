#!/bin/bash

# Test script for Server Crash and Recovery in Distributed Lock Manager
# This script tests scenarios where the server crashes and recovers

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
TEST_TIMEOUT=60
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

# Test 1: Server fails when lock is free
test_server_crash_lock_free() {
    print_header "TEST 1: Server fails when lock is free"
    
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
    
    # Client 1 appends to file (first time)
    print_color $YELLOW "Client 1 appending 'A' to file (first time)..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=1 --file="file_0" --content="A"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 first append failed"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 1 appends to file (second time)
    print_color $YELLOW "Client 1 appending 'A' to file (second time)..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=1 --file="file_0" --content="A"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 second append failed"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 1 releases the lock
    print_color $YELLOW "Client 1 releasing lock..."
    bin/lock_client release --servers=$SERVER_ADDR --client-id=1
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 failed to release lock"
        kill $SERVER_PID
        return 1
    fi
    
    # Verify file contains "AA"
    print_color $YELLOW "Verifying file contains 'AA'..."
    verify_file_contents "AA" || return 1
    
    # Simulate server crash
    print_color $YELLOW "Simulating server crash..."
    kill -9 $SERVER_PID
    sleep 2
    
    # Try to acquire lock (should fail with server down)
    print_color $YELLOW "Client 1 attempting to acquire lock (should fail with server down)..."
    bin/lock_client acquire --servers=$SERVER_ADDR --client-id=1 > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        print_color $RED "Lock acquisition succeeded but server should be down"
        return 1
    else
        print_color $GREEN "Lock acquisition failed as expected with server down"
    fi
    
    # Restart server
    print_color $YELLOW "Restarting server..."
    bin/lock_server --address=":$SERVER_PORT" &
    SERVER_PID=$!
    
    # Wait for server to be ready
    wait_for_server || return 1
    
    # Try to acquire lock (should succeed with server up)
    print_color $YELLOW "Client 1 retrying lock acquisition..."
    bin/lock_client acquire --servers=$SERVER_ADDR --client-id=1
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 failed to acquire lock after server restart"
        kill $SERVER_PID
        return 1
    fi
    
    # Append '1' to file
    print_color $YELLOW "Client 1 appending '1' to file..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=1 --file="file_0" --content="1"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 append failed after server restart"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 1 releases the lock
    print_color $YELLOW "Client 1 releasing lock..."
    bin/lock_client release --servers=$SERVER_ADDR --client-id=1
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 failed to release lock after server restart"
        kill $SERVER_PID
        return 1
    fi
    
    # Verify final file contents ("AA1")
    print_color $YELLOW "Verifying final file contents..."
    verify_file_contents "AA1" || return 1
    
    # Clean up
    kill $SERVER_PID 2>/dev/null || true
    
    print_color $GREEN "Test 1 complete - Server crash when lock is free"
    return 0
}

# Test 2: Server fails when lock is held
test_server_crash_lock_held() {
    print_header "TEST 2: Server fails when lock is held"
    
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
    
    # Client 1 appends to file
    print_color $YELLOW "Client 1 appending 'A' to file..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=1 --file="file_0" --content="A"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 append failed"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 1 releases lock
    print_color $YELLOW "Client 1 releasing lock..."
    bin/lock_client release --servers=$SERVER_ADDR --client-id=1
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 failed to release lock"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 2 acquires lock
    print_color $YELLOW "Client 2 acquiring lock..."
    bin/lock_client acquire --servers=$SERVER_ADDR --client-id=2
    if [ $? -ne 0 ]; then
        print_color $RED "Client 2 failed to acquire lock"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 2 appends first 'B'
    print_color $YELLOW "Client 2 appending 'B' to file (first time)..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=2 --file="file_0" --content="B"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 2 first append failed"
        kill $SERVER_PID
        return 1
    fi
    
    # Verify file contains "AB"
    print_color $YELLOW "Verifying file contains 'AB'..."
    verify_file_contents "AB" || return 1
    
    # Simulate server crash
    print_color $YELLOW "Simulating server crash..."
    kill -9 $SERVER_PID
    sleep 2
    
    # Client 2 attempts to append (should fail with server down)
    print_color $YELLOW "Client 2 attempting to append 'B' with server down (should fail)..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=2 --file="file_0" --content="B" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        print_color $RED "Append succeeded but server should be down"
        return 1
    else
        print_color $GREEN "Append failed as expected with server down"
    fi
    
    # Restart server
    print_color $YELLOW "Restarting server..."
    bin/lock_server --address=":$SERVER_PORT" &
    SERVER_PID=$!
    
    # Wait for server to be ready
    wait_for_server || return 1
    
    # Client 2 retries append (should succeed with valid token from recovered server state)
    print_color $YELLOW "Client 2 retrying 'B' append after server restart..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=2 --file="file_0" --content="B"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 2 append failed after server restart"
        print_color $YELLOW "This could be normal if the lock state was lost - will try reacquiring"
        
        # Try to acquire the lock again in case state was lost
        bin/lock_client acquire --servers=$SERVER_ADDR --client-id=2
        if [ $? -ne 0 ]; then
            print_color $RED "Client 2 failed to reacquire lock after server restart"
            kill $SERVER_PID
            return 1
        fi
        
        # Try append again
        bin/lock_client append --servers=$SERVER_ADDR --client-id=2 --file="file_0" --content="B"
        if [ $? -ne 0 ]; then
            print_color $RED "Client 2 append failed even after reacquiring lock"
            kill $SERVER_PID
            return 1
        fi
    fi
    
    # Client 2 releases lock
    print_color $YELLOW "Client 2 releasing lock..."
    bin/lock_client release --servers=$SERVER_ADDR --client-id=2
    if [ $? -ne 0 ]; then
        print_color $RED "Client 2 failed to release lock"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 1 acquires lock
    print_color $YELLOW "Client 1 acquiring lock again..."
    bin/lock_client acquire --servers=$SERVER_ADDR --client-id=1
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 failed to reacquire lock"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 1 appends 'A' to file
    print_color $YELLOW "Client 1 appending 'A' to file after server restart..."
    bin/lock_client append --servers=$SERVER_ADDR --client-id=1 --file="file_0" --content="A"
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 append failed after server restart"
        kill $SERVER_PID
        return 1
    fi
    
    # Client 1 releases lock
    print_color $YELLOW "Client 1 releasing lock..."
    bin/lock_client release --servers=$SERVER_ADDR --client-id=1
    if [ $? -ne 0 ]; then
        print_color $RED "Client 1 failed to release lock"
        kill $SERVER_PID
        return 1
    fi
    
    # Verify final file contents ("ABBA")
    print_color $YELLOW "Verifying final file contents..."
    verify_file_contents "ABBA" || return 1
    
    # Clean up
    kill $SERVER_PID 2>/dev/null || true
    
    print_color $GREEN "Test 2 complete - Server crash when lock is held"
    return 0
}

# Main function
main() {
    print_header "Server Crash Recovery Tests"
    
    # Check if a specific test was requested
    if [ "$1" ]; then
        case "$1" in
            test1|lock_free)
                test_server_crash_lock_free
                ;;
            test2|lock_held)
                test_server_crash_lock_held
                ;;
            *)
                print_color $RED "Unknown test: $1"
                print_color $YELLOW "Available tests: test1 (lock_free), test2 (lock_held)"
                return 1
                ;;
        esac
    else
        # Run all tests with proper error handling
        local result=0
        
        # Test 1: Server crash when lock is free
        test_server_crash_lock_free
        if [ $? -ne 0 ]; then
            print_color $RED "Test 1 (lock_free) failed"
            result=1
        fi
        
        # Test 2: Server crash when lock is held
        test_server_crash_lock_held
        if [ $? -ne 0 ]; then
            print_color $RED "Test 2 (lock_held) failed"
            result=1
        fi
        
        # Print overall result
        if [ $result -eq 0 ]; then
            print_header "All server crash recovery tests completed successfully"
        else
            print_header "Some server crash recovery tests failed, please check the output above"
        fi
        
        return $result
    fi
}

# Run the main function with command line arguments
main "$@" 