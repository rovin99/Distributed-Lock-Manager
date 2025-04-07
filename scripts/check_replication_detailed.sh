#!/bin/bash

# Function to check server status with detailed information
check_server_detailed() {
    local server_id=$1
    local server_addr=$2
    
    echo "====================================================="
    echo "Checking server $server_id at $server_addr..."
    echo "====================================================="
    
    # Create a temporary client to check server status
    go run cmd/test_client/main.go -addr $server_addr > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo "✅ Server $server_id is running and responding to requests"
        
        # Check if the server has a replication manager
        if [ -f "logs/server_${server_id}.log" ]; then
            echo "Replication manager logs for $server_id:"
            echo "-----------------------------------------------------"
            grep -i "replication" "logs/server_${server_id}.log" | tail -n 10
            echo "-----------------------------------------------------"
        else
            echo "❌ No log file found for server $server_id"
        fi
    else
        echo "❌ Server $server_id is not responding to requests"
    fi
    echo ""
}

# Check each server
check_server_detailed "server1" "localhost:50051"
check_server_detailed "server2" "localhost:50052"
check_server_detailed "server3" "localhost:50053"

echo "Detailed replication status check completed." 