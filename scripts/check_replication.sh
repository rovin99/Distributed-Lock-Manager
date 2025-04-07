#!/bin/bash

# Function to check server status
check_server() {
    local server_id=$1
    local server_addr=$2
    
    echo "Checking server $server_id at $server_addr..."
    
    # Create a temporary client to check server status
    go run cmd/test_client/main.go -addr $server_addr > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo "✅ Server $server_id is running and responding to requests"
    else
        echo "❌ Server $server_id is not responding to requests"
    fi
}

# Check each server
check_server "server1" "localhost:50051"
check_server "server2" "localhost:50052"
check_server "server3" "localhost:50053"

echo "Replication status check completed." 