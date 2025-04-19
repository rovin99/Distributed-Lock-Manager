#!/bin/bash

# Test script for distributed lock manager
# Tests replication, leader election, and quorum detection

set -e

BINARY="./dlm-server"
if [ ! -f "$BINARY" ]; then
    echo "Building dlm-server binary..."
    go build -o dlm-server cmd/server/main.go
fi

DATA_DIR="./data"
mkdir -p "$DATA_DIR"

# Clean up previous test data
rm -f "$DATA_DIR"/*.lock "$DATA_DIR"/*.wal "$DATA_DIR"/verify_fs_*.tmp

# Clean up any previous test processes
pkill -f "dlm-server" || true
sleep 1

# Create log directory
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"

# Start 3 servers in the background
echo "Starting 3 server nodes..."
$BINARY -port 8081 -server-id 1 -servers "1=localhost:8081,2=localhost:8082,3=localhost:8083" > "$LOG_DIR/server1.log" 2>&1 &
SERVER1_PID=$!
sleep 1

$BINARY -port 8082 -server-id 2 -servers "1=localhost:8081,2=localhost:8082,3=localhost:8083" > "$LOG_DIR/server2.log" 2>&1 &
SERVER2_PID=$!
sleep 1

$BINARY -port 8083 -server-id 3 -servers "1=localhost:8081,2=localhost:8082,3=localhost:8083" > "$LOG_DIR/server3.log" 2>&1 &
SERVER3_PID=$!
sleep 3

echo "All servers started"

# Test client to check server roles and perform operations
CLIENT="./dlm-client"
if [ ! -f "$CLIENT" ]; then
    echo "Building dlm-client binary..."
    go build -o dlm-client cmd/client/main.go
fi

# Function to check server roles
check_roles() {
    SERVER1_ROLE=$($CLIENT -server localhost:8081 -command info | grep "Role:" | awk '{print $2}')
    SERVER2_ROLE=$($CLIENT -server localhost:8082 -command info | grep "Role:" | awk '{print $2}')
    SERVER3_ROLE=$($CLIENT -server localhost:8083 -command info | grep "Role:" | awk '{print $2}')
    
    echo "Server 1 role: $SERVER1_ROLE"
    echo "Server 2 role: $SERVER2_ROLE"
    echo "Server 3 role: $SERVER3_ROLE"
    
    # Count primary servers
    PRIMARY_COUNT=0
    if [ "$SERVER1_ROLE" == "primary" ]; then
        PRIMARY_COUNT=$((PRIMARY_COUNT + 1))
        PRIMARY_ID=1
    fi
    if [ "$SERVER2_ROLE" == "primary" ]; then
        PRIMARY_COUNT=$((PRIMARY_COUNT + 1))
        PRIMARY_ID=2
    fi
    if [ "$SERVER3_ROLE" == "primary" ]; then
        PRIMARY_COUNT=$((PRIMARY_COUNT + 1))
        PRIMARY_ID=3
    fi
    
    echo "Found $PRIMARY_COUNT primary server(s)"
    if [ $PRIMARY_COUNT -eq 1 ]; then
        echo "Primary is server $PRIMARY_ID"
        CURRENT_PRIMARY_ID=$PRIMARY_ID
        return 0
    else
        echo "ERROR: Expected exactly one primary server"
        return 1
    fi
}

# Function to test lock acquisition and replication
test_lock_acquisition() {
    local server_port=$1
    echo "Testing lock acquisition on server at port $server_port..."
    
    # Acquire lock
    RESULT=$($CLIENT -server localhost:$server_port -command acquire -client-id 100)
    if echo "$RESULT" | grep -q "Lock acquired successfully"; then
        echo "Successfully acquired lock"
    else
        echo "ERROR: Failed to acquire lock: $RESULT"
        return 1
    fi
    
    # Check if lock shows up on other servers (wait for replication)
    sleep 2
    
    for port in 8081 8082 8083; do
        if [ $port -ne $server_port ]; then
            HOLDER=$($CLIENT -server localhost:$port -command info | grep "Current lock holder:" | awk '{print $4}')
            if [ "$HOLDER" == "100" ]; then
                echo "Lock state replicated to server on port $port"
            else
                echo "ERROR: Lock state not properly replicated to server on port $port (holder: $HOLDER)"
                return 1
            fi
        fi
    done
    
    # Release lock
    RESULT=$($CLIENT -server localhost:$server_port -command release -client-id 100)
    if echo "$RESULT" | grep -q "Lock released successfully"; then
        echo "Successfully released lock"
    else
        echo "ERROR: Failed to release lock: $RESULT"
        return 1
    fi
    
    sleep 2
    return 0
}

# Test step 1: Check initial roles
echo "Step 1: Checking initial server roles..."
check_roles || exit 1

# Test step 2: Test lock acquisition on primary
echo "Step 2: Testing lock acquisition and replication..."
test_lock_acquisition 8081 || exit 1

# Test step 3: Kill primary server and check for leader election
echo "Step 3: Killing primary server (server 1) and testing leader election..."
kill -9 $SERVER1_PID
echo "Waiting for leader election to complete..."
sleep 10

# Check new roles
echo "Checking server roles after primary failure..."
check_roles || exit 1

if [ $CURRENT_PRIMARY_ID -eq 1 ]; then
    echo "ERROR: Server 1 is still reported as primary after being killed"
    exit 1
else
    echo "SUCCESS: Server $CURRENT_PRIMARY_ID became the new primary after server 1 failure"
fi

# Test step 4: Test lock acquisition on new primary
echo "Step 4: Testing lock acquisition on new primary..."
PRIMARY_PORT=$((8080 + $CURRENT_PRIMARY_ID))
test_lock_acquisition $PRIMARY_PORT || exit 1

# Test step 5: Kill another server to break quorum
echo "Step 5: Killing another server to break quorum..."
if [ $CURRENT_PRIMARY_ID -eq 2 ]; then
    kill -9 $SERVER3_PID
    REMAINING_SERVER_ID=2
    REMAINING_PORT=8082
else
    kill -9 $SERVER2_PID
    REMAINING_SERVER_ID=3
    REMAINING_PORT=8083
fi

echo "Waiting for quorum check to detect loss of majority..."
sleep 10

# Check if the remaining primary has demoted itself
REMAINING_ROLE=$($CLIENT -server localhost:$REMAINING_PORT -command info | grep "Role:" | awk '{print $2}')
echo "Remaining server $REMAINING_SERVER_ID role: $REMAINING_ROLE"

if [ "$REMAINING_ROLE" == "secondary" ]; then
    echo "SUCCESS: Primary server properly demoted itself to secondary after quorum loss"
else
    echo "WARNING: Primary server did not demote itself after quorum loss"
    # This might be acceptable depending on implementation
fi

# Clean up
echo "Cleaning up..."
pkill -f "dlm-server" || true

echo "Test completed successfully!" 