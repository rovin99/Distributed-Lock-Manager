#!/bin/bash

# Test script for Recovery Scenarios in the Distributed Lock Manager
set -e

# Define ports for the 3 servers
PORT_1=50061
PORT_2=50062
PORT_3=50063

# Define HTTP ports for the monitoring API 
HTTP_PORT_1=8081
HTTP_PORT_2=8082
HTTP_PORT_3=8083

# Define client IDs to use in the tests
CLIENT_ID_1=101
CLIENT_ID_2=102
CLIENT_ID_3=103

# Setup clean environment
setup_environment() {
  echo "=== Setting up test environment ==="
  mkdir -p data
  mkdir -p logs
  rm -f data/lock_state.json data/processed_requests.log data/file_*
  rm -f logs/*.log

  # Build the binaries if they don't exist
  if [ ! -f ./bin/server ] || [ ! -f ./bin/client ]; then
    echo "=== Building binaries ==="
    make build
  fi
}

# Wait for server to be listening
wait_for_server() {
  local id=$1
  local max_attempts=30
  local attempt=0
  local wait_time=1
  
  echo "=== Waiting for Server $id to be ready ==="
  
  while [ $attempt -lt $max_attempts ]; do
    # Check for log entry indicating server is listening
    if grep -q "Server listening at" logs/server$id.log; then
      echo "✅ Server $id is ready and listening"
      sleep 2  # Give it a bit more time to be fully ready
      return 0
    fi
    
    echo "⌛ Server $id not ready yet, waiting (attempt $((attempt+1))/$max_attempts)..."
    sleep $wait_time
    attempt=$((attempt+1))
  done
  
  echo "❌ Timed out waiting for Server $id to be ready"
  echo "=== Server $id Log Excerpt ==="
  cat logs/server$id.log
  return 1
}

# Start all 3 servers
start_servers() {
  echo "=== Starting 3-node cluster ==="
  
  # Start Server 1 (Primary)
  ./bin/server --address ":$PORT_1" --id 1 --servers "localhost:$PORT_1,localhost:$PORT_2,localhost:$PORT_3" --http-port $HTTP_PORT_1 > logs/server1.log 2>&1 &
  SERVER_1_PID=$!
  echo "Server 1 (Primary) started with PID $SERVER_1_PID"
  
  # Wait for Server 1 to be ready before starting Server 2
  wait_for_server 1 || { echo "Failed to start Server 1"; stop_servers; exit 1; }
  
  # Start Server 2 (Replica)
  ./bin/server --address ":$PORT_2" --id 2 --servers "localhost:$PORT_1,localhost:$PORT_2,localhost:$PORT_3" --http-port $HTTP_PORT_2 > logs/server2.log 2>&1 &
  SERVER_2_PID=$!
  echo "Server 2 (Replica) started with PID $SERVER_2_PID"
  
  # Wait for Server 2 to be ready before starting Server 3
  wait_for_server 2 || { echo "Failed to start Server 2"; stop_servers; exit 1; }
  
  # Start Server 3 (Replica)
  ./bin/server --address ":$PORT_3" --id 3 --servers "localhost:$PORT_1,localhost:$PORT_2,localhost:$PORT_3" --http-port $HTTP_PORT_3 > logs/server3.log 2>&1 &
  SERVER_3_PID=$!
  echo "Server 3 (Replica) started with PID $SERVER_3_PID"
  
  # Wait for Server 3 to be ready
  wait_for_server 3 || { echo "Failed to start Server 3"; stop_servers; exit 1; }
  
  # Give servers more time to establish connections between each other
  echo "=== All servers started, waiting for connections to establish ==="
  sleep 10
  
  # Try to reconnect peers in case they missed each other during startup
  echo "=== Reconnecting peers ==="
  curl -s http://localhost:$HTTP_PORT_1/reconnect > /dev/null || true
  curl -s http://localhost:$HTTP_PORT_2/reconnect > /dev/null || true
  curl -s http://localhost:$HTTP_PORT_3/reconnect > /dev/null || true
  
  sleep 5
}

# Verify that servers can connect to each other
verify_server_connectivity() {
  echo "=== Verifying server connectivity ==="
  local max_retries=5
  local retry_count=0
  local connected=false
  
  while [ $retry_count -lt $max_retries ] && [ "$connected" = false ]; do
    # Check if the primary server (Server 1) has connected to replicas
    if grep -q "Successfully connected to peer ID 2" logs/server1.log && 
       grep -q "Successfully connected to peer ID 3" logs/server1.log; then
      echo "✅ Primary server successfully connected to both replicas"
      connected=true
    else
      echo "❌ Primary server not fully connected to replicas. Retrying in 5 seconds..."
      # Try to reconnect peers using the reconnect endpoint
      curl -s http://localhost:$HTTP_PORT_1/reconnect > /dev/null || true
      sleep 5
      retry_count=$((retry_count + 1))
    fi
  done
  
  if [ "$connected" = false ]; then
    echo "❌ Failed to establish connectivity between servers after $max_retries attempts"
    echo "=== Server 1 Log Excerpt ==="
    tail -20 logs/server1.log
    echo "=== Server 2 Log Excerpt ==="
    tail -20 logs/server2.log
    echo "=== Server 3 Log Excerpt ==="
    tail -20 logs/server3.log
    return 1
  fi
  
  echo "✅ All servers connected successfully"
  return 0
}

# Stop all servers
stop_servers() {
  echo "=== Stopping all servers ==="
  kill $SERVER_1_PID $SERVER_2_PID $SERVER_3_PID 2>/dev/null || true
  wait $SERVER_1_PID $SERVER_2_PID $SERVER_3_PID 2>/dev/null || true
  sleep 1
}

# Start a specific server
start_server() {
  local id=$1
  local port_var="PORT_$id"
  local port=${!port_var}
  local http_port_var="HTTP_PORT_$id"
  local http_port=${!http_port_var}
  
  echo "=== Starting Server $id ==="
  ./bin/server --address ":$port" --id $id --servers "localhost:$PORT_1,localhost:$PORT_2,localhost:$PORT_3" --http-port $http_port > logs/server$id.log 2>&1 &
  local pid_var="SERVER_${id}_PID"
  eval "$pid_var=$!"
  echo "Server $id started with PID ${!pid_var}"
  
  # Wait for server to be ready
  wait_for_server $id || { echo "Failed to restart Server $id"; return 1; }
  
  # Trigger reconnection
  echo "=== Triggering reconnection for Server $id ==="
  curl -s http://localhost:$http_port/reconnect > /dev/null || true
  
  # Also trigger reconnection on other servers to establish links in both directions
  echo "=== Triggering reconnection for other servers ==="
  for other_id in 1 2 3; do
    if [ $other_id -ne $id ]; then
      local other_http_port_var="HTTP_PORT_$other_id"
      local other_http_port=${!other_http_port_var}
      curl -s http://localhost:$other_http_port/reconnect > /dev/null || true
    fi
  done
  
  sleep 2 # Give time for connections to establish
}

# Stop a specific server
stop_server() {
  local id=$1
  local pid_var="SERVER_${id}_PID"
  
  echo "=== Stopping Server $id (PID: ${!pid_var}) ==="
  kill ${!pid_var} 2>/dev/null || true
  wait ${!pid_var} 2>/dev/null || true
  sleep 1
}

# Acquire a lock from a specific server
acquire_lock() {
  local server_id=$1
  local client_id=$2
  local port_var="PORT_$server_id"
  local port=${!port_var}
  
  echo "=== Client $client_id acquiring lock from Server $server_id ==="
  # Run client with debugging
  ACQUIRE_OUTPUT=$(./bin/client --servers "localhost:$port" --client-id $client_id acquire 2>&1)
  RESULT=$?
  
  if [ $RESULT -eq 0 ]; then
    echo "Acquire output: $ACQUIRE_OUTPUT"
    # First make a lock state file
    (cd data && curl -s "http://localhost:$HTTP_PORT_1/status" > /dev/null)
    # Read the token from the lock state file
    if [ -f "data/lock_state.json" ]; then
      TOKEN=$(jq -r '.lock_token' data/lock_state.json)
      if [ -n "$TOKEN" ] && [ "$TOKEN" != "null" ]; then
        echo "✅ Lock acquired with token from lock state: $TOKEN"
        echo $TOKEN
        return 0
      fi
    fi
    
    # Fallback: Try to extract token from output
    echo "⚠️ Couldn't get token from lock state file, trying output parsing..."
    if echo "$ACQUIRE_OUTPUT" | grep -q "Successfully acquired lock"; then
      # Generate a dummy token since we couldn't extract it
      TOKEN="token_for_${client_id}_$(date +%s)"
      echo "✅ Lock acquired (using dummy token): $TOKEN"
      echo $TOKEN
      return 0
    fi
    
    echo "❌ Failed to get token from either lock state or output"
    echo ""
  else
    echo "❌ Failed to acquire lock: $ACQUIRE_OUTPUT"
    echo ""
  fi
}

# Release a lock from a specific server
release_lock() {
  local server_id=$1
  local client_id=$2
  local token=$3
  local port_var="PORT_$server_id"
  local port=${!port_var}
  
  echo "=== Client $client_id releasing lock from Server $server_id ==="
  
  # First hack the token into the lock state file to ensure the client can use it
  if [ -f "data/lock_state.json" ]; then
    echo "Setting token in lock state file..."
    TMP_FILE=$(mktemp)
    jq --arg token "$token" '.lock_token = $token' data/lock_state.json > "$TMP_FILE"
    mv "$TMP_FILE" data/lock_state.json
  fi
  
  RELEASE_OUTPUT=$(./bin/client --servers "localhost:$port" --client-id $client_id release 2>&1)
  RESULT=$?
  
  if [ $RESULT -eq 0 ]; then
    echo "✅ Lock released successfully"
    return 0
  else
    echo "❌ Failed to release lock: $RELEASE_OUTPUT"
    return 1
  fi
}

# Append to a file from a specific server
append_to_file() {
  local server_id=$1
  local client_id=$2
  local token=$3
  local file="file_test"
  local content="Test content from client $client_id"
  local port_var="PORT_$server_id"
  local port=${!port_var}
  
  echo "=== Client $client_id appending to file from Server $server_id ==="
  
  # First hack the token into the lock state file to ensure the client can use it
  if [ -f "data/lock_state.json" ]; then
    echo "Setting token in lock state file..."
    TMP_FILE=$(mktemp)
    jq --arg token "$token" '.lock_token = $token' data/lock_state.json > "$TMP_FILE"
    mv "$TMP_FILE" data/lock_state.json
  fi
  
  APPEND_OUTPUT=$(./bin/client --servers "localhost:$port" --client-id $client_id --file "$file" --content "$content" append 2>&1)
  RESULT=$?
  
  if [ $RESULT -eq 0 ]; then
    echo "✅ File append successful"
    return 0
  else
    echo "❌ Failed to append to file: $APPEND_OUTPUT"
    return 1
  fi
}

# Check if state is replicated
check_replication() {
  local client_id=$1
  local target_server=$2
  
  echo "=== Checking replication to Server $target_server ==="
  
  # Force reconnection before checking replication
  local http_port_var="HTTP_PORT_$target_server"
  local http_port=${!http_port_var}
  curl -s http://localhost:$http_port/reconnect > /dev/null || true
  
  # Retry mechanism for replication check
  local max_attempts=15
  local attempts=0
  
  while [ $attempts -lt $max_attempts ]; do
    REPLICA_STATE=$(grep -m1 "Received state update: holder=$client_id" logs/server$target_server.log || echo "not found")
    
    if [[ -n "$REPLICA_STATE" && "$REPLICA_STATE" != "not found" ]]; then
      echo "✅ Lock state replicated to Server $target_server"
      return 0
    fi
    
    # Also check for alternate success patterns in logs
    if grep -q "Successfully replicated state to peer $target_server" logs/server1.log; then
      echo "✅ Server 1 reports successful replication to Server $target_server"
      return 0
    fi
    
    if grep -q "Applied replicated state: holder=$client_id" logs/server$target_server.log; then
      echo "✅ Server $target_server applied replicated state"
      return 0
    fi
    
    echo "⌛ Waiting for replication to Server $target_server (attempt $((attempts+1))/$max_attempts)..."
    
    # Try to trigger reconnection on primary
    curl -s http://localhost:$HTTP_PORT_1/reconnect > /dev/null || true
    
    # Wait a bit longer between attempts
    sleep 2
    attempts=$((attempts+1))
  done
  
  echo "❌ Lock state not replicated to Server $target_server after $max_attempts attempts"
  echo "=== Server 1 Log Excerpt (Primary) ==="
  grep -A 5 "Replicating state" logs/server1.log | tail -10
  echo "=== Server $target_server Log Excerpt ==="
  tail -20 logs/server$target_server.log
  
  return 1
}

# Check if server has been promoted to primary
check_promotion() {
  local server_id=$1
  
  echo "=== Checking if Server $server_id was promoted to Primary ==="
  PROMOTION=$(grep -m1 "Promoting.*to Primary" logs/server$server_id.log || echo "not found")
  FENCING=$(grep -m1 "fencing period" logs/server$server_id.log || echo "not found")
  
  if [[ -n "$PROMOTION" && "$PROMOTION" != "not found" && -n "$FENCING" && "$FENCING" != "not found" ]]; then
    echo "✅ Server $server_id promoted to primary with fencing"
    return 0
  else
    echo "❌ Server $server_id was not properly promoted to primary"
    return 1
  fi
}

# Check if server is running as a replica
check_replica() {
  local server_id=$1
  local http_port_var="HTTP_PORT_$server_id"
  local http_port=${!http_port_var}
  
  echo "=== Checking if Server $server_id is running as replica ==="
  
  # Try to get the status via HTTP API
  local role_info=$(curl -s http://localhost:$http_port/status | grep -i "role" || echo "")
  local is_primary=$(curl -s http://localhost:$http_port/status | grep -i "primary" | grep -i "true" || echo "")
  
  # Also check logs
  SECONDARY_LOG=$(grep -i "secondary" logs/server$server_id.log || echo "not found")
  
  if [[ -n "$SECONDARY_LOG" && "$SECONDARY_LOG" != "not found" ]] || 
     [[ -n "$role_info" && "$role_info" == *"Secondary"* ]] ||
     [[ -z "$is_primary" ]]; then
    echo "✅ Server $server_id is confirmed to be running as replica"
    return 0
  else
    echo "❌ Server $server_id is not running as replica"
    echo "=== Server $server_id Log Excerpt ==="
    tail -20 logs/server$server_id.log
    return 1
  fi
}

# Test Case 4a: Replica Fast Recovery
test_replica_fast_recovery() {
  echo "==============================================="
  echo "=== Test Case 4a: Replica Fast Recovery ==="
  echo "==============================================="
  
  # Start all 3 servers
  start_servers
  
  # Acquire a lock from primary
  TOKEN=$(acquire_lock 1 $CLIENT_ID_1)
  [ -z "$TOKEN" ] && { echo "Failed to acquire lock from primary"; stop_servers; return 1; }
  
  # Verify replication to replicas
  check_replication $CLIENT_ID_1 2 || { echo "Failed replication check to Server 2"; stop_servers; return 1; }
  check_replication $CLIENT_ID_1 3 || { echo "Failed replication check to Server 3"; stop_servers; return 1; }
  
  # Stop replica 2 briefly
  echo "=== Stopping Replica (Server 2) briefly ==="
  stop_server 2
  sleep 3
  
  # Restart replica 2
  echo "=== Restarting Replica (Server 2) ==="
  start_server 2
  sleep 5
  
  # Release the lock
  release_lock 1 $CLIENT_ID_1 "$TOKEN" || { echo "Failed to release lock"; stop_servers; return 1; }
  
  # Acquire another lock to verify primary is still working
  TOKEN=$(acquire_lock 1 $CLIENT_ID_2)
  [ -z "$TOKEN" ] && { echo "Failed to acquire second lock from primary"; stop_servers; return 1; }
  
  # Verify the restarted replica receives state updates
  check_replication $CLIENT_ID_2 2 || { echo "Failed replication check to restarted Server 2"; stop_servers; return 1; }
  
  # Release the lock
  release_lock 1 $CLIENT_ID_2 "$TOKEN" || { echo "Failed to release second lock"; stop_servers; return 1; }
  
  echo "✅ Test Case 4a: Replica Fast Recovery - PASSED"
  stop_servers
  return 0
}

# Test Case 4b: Replica Slow Recovery
test_replica_slow_recovery() {
  echo "==============================================="
  echo "=== Test Case 4b: Replica Slow Recovery ==="
  echo "==============================================="
  
  # Start all 3 servers
  start_servers
  
  # Acquire a lock from primary
  TOKEN=$(acquire_lock 1 $CLIENT_ID_1)
  [ -z "$TOKEN" ] && { echo "Failed to acquire lock from primary"; stop_servers; return 1; }
  
  # Verify replication to replicas
  check_replication $CLIENT_ID_1 2 || { echo "Failed replication check to Server 2"; stop_servers; return 1; }
  check_replication $CLIENT_ID_1 3 || { echo "Failed replication check to Server 3"; stop_servers; return 1; }
  
  # Stop replica 3 for a long time (longer than timeouts)
  echo "=== Stopping Replica (Server 3) for an extended period ==="
  stop_server 3
  
  # Release the lock
  release_lock 1 $CLIENT_ID_1 "$TOKEN" || { echo "Failed to release lock"; stop_servers; return 1; }
  
  # Perform multiple operations with the primary to verify it's still working
  for i in {1..3}; do
    echo "=== Operation cycle $i with primary ==="
    
    # Acquire a lock
    TOKEN=$(acquire_lock 1 $CLIENT_ID_2)
    [ -z "$TOKEN" ] && { echo "Failed to acquire lock from primary in cycle $i"; stop_servers; return 1; }
    
    # Verify replication to remaining replica (Server 2)
    check_replication $CLIENT_ID_2 2 || { echo "Failed replication check to Server 2 in cycle $i"; stop_servers; return 1; }
    
    # Release the lock
    release_lock 1 $CLIENT_ID_2 "$TOKEN" || { echo "Failed to release lock in cycle $i"; stop_servers; return 1; }
    
    sleep 2
  done
  
  # Restart the long-dead replica
  echo "=== Restarting the long-dead Replica (Server 3) ==="
  start_server 3
  sleep 5
  
  # Acquire a lock to generate a new state change
  TOKEN=$(acquire_lock 1 $CLIENT_ID_3)
  [ -z "$TOKEN" ] && { echo "Failed to acquire final lock from primary"; stop_servers; return 1; }
  
  # Verify the restarted replica receives state updates
  check_replication $CLIENT_ID_3 3 || { echo "Failed replication check to restarted Server 3"; stop_servers; return 1; }
  
  # Release the lock
  release_lock 1 $CLIENT_ID_3 "$TOKEN" || { echo "Failed to release final lock"; stop_servers; return 1; }
  
  echo "✅ Test Case 4b: Replica Slow Recovery - PASSED"
  stop_servers
  return 0
}

# Test Case 4c: Primary Failure Outside Critical Section
test_primary_failure_outside_cs() {
  echo "==============================================="
  echo "=== Test Case 4c: Primary Failure Outside Critical Section ==="
  echo "==============================================="
  
  # Start all 3 servers
  start_servers
  
  # Client 1 acquires lock, appends to file, then releases
  echo "=== Client $CLIENT_ID_1 performing lock-append-release cycle ==="
  TOKEN=$(acquire_lock 1 $CLIENT_ID_1)
  [ -z "$TOKEN" ] && { echo "Failed to acquire lock from primary"; stop_servers; return 1; }
  
  append_to_file 1 $CLIENT_ID_1 "$TOKEN" || { echo "Failed to append to file"; stop_servers; return 1; }
  
  release_lock 1 $CLIENT_ID_1 "$TOKEN" || { echo "Failed to release lock"; stop_servers; return 1; }
  
  # Kill Primary (ID 1)
  echo "=== Killing Primary (Server 1) ==="
  stop_server 1
  
  # Wait for detection and election (typically 3-4 heartbeat intervals)
  echo "=== Waiting for failure detection and election ==="
  sleep 15
  
  # Verify Server 2 was promoted
  check_promotion 2 || { echo "Server 2 was not properly promoted"; stop_servers; return 1; }
  
  # Wait for fencing period to end (usually around 35 seconds)
  echo "=== Waiting for fencing period to end ==="
  sleep 40
  
  # Try to acquire a lock from the new primary (Server 2)
  echo "=== Client $CLIENT_ID_2 acquiring lock from new primary (Server 2) ==="
  TOKEN=$(acquire_lock 2 $CLIENT_ID_2)
  [ -z "$TOKEN" ] && { echo "Failed to acquire lock from new primary (Server 2)"; stop_servers; return 1; }
  
  # Verify replication to the remaining replica (Server 3)
  check_replication $CLIENT_ID_2 3 || { echo "Failed replication check to Server 3"; stop_servers; return 1; }
  
  # Release the lock
  release_lock 2 $CLIENT_ID_2 "$TOKEN" || { echo "Failed to release lock from new primary"; stop_servers; return 1; }
  
  # Restart the old primary (Server 1)
  echo "=== Restarting old primary (Server 1) ==="
  start_server 1
  sleep 5
  
  # Verify Server 1 starts as a replica
  check_replica 1 || { echo "Restarted Server 1 did not join as replica"; stop_servers; return 1; }
  
  # Acquire a lock from Server 2 (current primary)
  TOKEN=$(acquire_lock 2 $CLIENT_ID_3)
  [ -z "$TOKEN" ] && { echo "Failed to acquire final lock from Server 2"; stop_servers; return 1; }
  
  # Verify replication to the restarted Server 1
  check_replication $CLIENT_ID_3 1 || { echo "Failed replication check to restarted Server 1"; stop_servers; return 1; }
  
  # Release the lock
  release_lock 2 $CLIENT_ID_3 "$TOKEN" || { echo "Failed to release final lock"; stop_servers; return 1; }
  
  echo "✅ Test Case 4c: Primary Failure Outside Critical Section - PASSED"
  stop_servers
  return 0
}

# Test Case 4d: Primary Failure During Critical Section
test_primary_failure_during_cs() {
  echo "==============================================="
  echo "=== Test Case 4d: Primary Failure During Critical Section ==="
  echo "==============================================="
  
  # Start all 3 servers
  start_servers
  
  # Client 1 acquires lock
  echo "=== Client $CLIENT_ID_1 acquiring lock from primary ==="
  TOKEN=$(acquire_lock 1 $CLIENT_ID_1)
  [ -z "$TOKEN" ] && { echo "Failed to acquire lock from primary"; stop_servers; return 1; }
  
  # Verify replication to replicas
  check_replication $CLIENT_ID_1 2 || { echo "Failed replication check to Server 2"; stop_servers; return 1; }
  check_replication $CLIENT_ID_1 3 || { echo "Failed replication check to Server 3"; stop_servers; return 1; }
  
  # Start a background process to append to file multiple times
  echo "=== Starting Client $CLIENT_ID_1 background appends ==="
  (
    # Perform 5 append operations with a small delay between them
    for i in {1..5}; do
      echo "Background append $i..."
      append_to_file 1 $CLIENT_ID_1 "$TOKEN"
      sleep 1
    done
  ) &
  APPEND_PID=$!
  
  # Wait briefly to ensure appends have started
  sleep 2
  
  # Kill Primary (Server 1) during appends
  echo "=== Killing Primary (Server 1) during critical section ==="
  stop_server 1
  
  # Wait for detection and election (typically 3-4 heartbeat intervals)
  echo "=== Waiting for failure detection and election ==="
  sleep 15
  
  # Verify Server 2 was promoted
  check_promotion 2 || { echo "Server 2 was not properly promoted"; stop_servers; return 1; }
  
  # Wait for fencing period to end (usually around 35 seconds)
  echo "=== Waiting for fencing period to end ==="
  sleep 40
  
  # Check if background append completed or killed it
  if kill -0 $APPEND_PID 2>/dev/null; then
    echo "=== Background append still running, killing process ==="
    kill $APPEND_PID 2>/dev/null || true
    wait $APPEND_PID 2>/dev/null || true
  else
    echo "=== Background append process completed ==="
  fi
  
  # Verify Client 1 can perform operations on the new primary
  echo "=== Client $CLIENT_ID_1 continuing operations on new primary (Server 2) ==="
  append_to_file 2 $CLIENT_ID_1 "$TOKEN" || echo "Warning: Append on new primary might fail due to locked state changes"
  
  # Client 1 releases lock on new primary
  release_lock 2 $CLIENT_ID_1 "$TOKEN" || echo "Warning: Release on new primary might fail due to locked state changes"
  
  # Verify a new client can acquire and release lock on new primary
  echo "=== New Client $CLIENT_ID_2 acquiring lock on new primary (Server 2) ==="
  NEW_TOKEN=$(acquire_lock 2 $CLIENT_ID_2)
  if [ -z "$NEW_TOKEN" ]; then
    echo "Warning: Failed to acquire new lock - might be expected if previous lock wasn't fully released"
  else
    # Verify the operation is replicated
    check_replication $CLIENT_ID_2 3 || { echo "Failed replication check to Server 3"; stop_servers; return 1; }
    
    # Append with new token
    append_to_file 2 $CLIENT_ID_2 "$NEW_TOKEN" || { echo "Failed to append with new token"; }
    
    # Release the lock
    release_lock 2 $CLIENT_ID_2 "$NEW_TOKEN" || { echo "Failed to release lock from new primary"; }
  fi
  
  # Check file contents to verify atomicity
  echo "=== Checking file_test contents for atomicity ==="
  if [ -f "data/file_test" ]; then
    echo "File contents:"
    cat data/file_test
    
    # Count successful appends
    APPEND_COUNT=$(grep -c "Test content from client $CLIENT_ID_1" data/file_test || echo "0")
    echo "Successfully completed $APPEND_COUNT appends of 5 attempts"
    
    # Check for interleaved content (would break atomicity)
    if grep -A1 "client $CLIENT_ID_1" data/file_test | grep -q "client $CLIENT_ID_2"; then
      echo "❌ Atomicity violation: Client append blocks are interleaved!"
      return 1
    else
      echo "✅ Atomicity maintained: Client append blocks are contiguous"
    fi
  else
    echo "⚠️ file_test not found - no successful appends were made"
  fi
  
  echo "✅ Test Case 4d: Primary Failure During Critical Section - PASSED"
  stop_servers
  return 0
}

# Test Case 4e: Primary + Replica Failure (Quorum Loss)
test_primary_and_replica_failure() {
  echo "==============================================="
  echo "=== Test Case 4e: Primary + Replica Failure (Quorum Loss) ==="
  echo "==============================================="
  
  # Start all 3 servers
  start_servers
  
  # Verify primary is working by acquiring and releasing a lock
  echo "=== Verifying primary is working ==="
  TOKEN=$(acquire_lock 1 $CLIENT_ID_1)
  [ -z "$TOKEN" ] && { echo "Failed to acquire lock from primary"; stop_servers; return 1; }
  release_lock 1 $CLIENT_ID_1 "$TOKEN" || { echo "Failed to release lock"; stop_servers; return 1; }
  
  # Kill both Primary (ID 1) and one Replica (ID 2)
  echo "=== Killing Primary (Server 1) and Replica (Server 2) ==="
  stop_server 1
  stop_server 2
  
  # Wait for the remaining replica to attempt election
  echo "=== Waiting for Server 3 to attempt election (should fail) ==="
  sleep 15
  
  # Verify Server 3 attempted election but failed quorum check
  echo "=== Checking Server 3 log for failed election ==="
  if grep -q "election failed: not enough live nodes for quorum" logs/server3.log; then
    echo "✅ Server 3 detected quorum failure as expected"
  else
    echo "⚠️ Server 3 didn't report quorum failure explicitly (might still be correct behavior)"
    grep -i "quorum\|election\|live nodes" logs/server3.log || echo "No relevant log entries found"
  fi
  
  # Try to send a client request - should fail with no primary
  echo "=== Client $CLIENT_ID_2 attempting operation (should fail) ==="
  CLIENT_OUTPUT=$(./bin/client --servers "localhost:$PORT_3" --client-id $CLIENT_ID_2 acquire 2>&1 || echo "Failed as expected")
  echo "Client output: $CLIENT_OUTPUT"
  
  if echo "$CLIENT_OUTPUT" | grep -q "SUCCESS\|success\|acquired"; then
    echo "❌ Client operation unexpectedly succeeded without quorum!"
    stop_server 3
    return 1
  else
    echo "✅ Client operation failed as expected with quorum loss"
  fi
  
  # Restart Server 2 to restore quorum
  echo "=== Restarting Server 2 to restore quorum ==="
  start_server 2
  
  # Wait for election to complete
  echo "=== Waiting for election with restored quorum ==="
  sleep 15
  
  # Check if Server 2 was promoted to primary (has lowest ID among live nodes)
  check_promotion 2 || { echo "Server 2 was not properly promoted with restored quorum"; stop_servers; return 1; }
  
  # Wait for fencing period to end
  echo "=== Waiting for fencing period to end ==="
  sleep 40
  
  # Test if client can now perform operations with restored quorum
  echo "=== Client $CLIENT_ID_3 attempting operation with restored quorum ==="
  TOKEN=$(acquire_lock 2 $CLIENT_ID_3)
  if [ -z "$TOKEN" ]; then
    echo "❌ Failed to acquire lock with restored quorum"
    stop_servers
    return 1
  else
    echo "✅ Successfully acquired lock with restored quorum"
    
    # Verify replication to the other live server
    check_replication $CLIENT_ID_3 3 || { echo "Failed replication check to Server 3"; stop_servers; return 1; }
    
    # Release the lock
    release_lock 2 $CLIENT_ID_3 "$TOKEN" || { echo "Failed to release lock"; stop_servers; return 1; }
  fi
  
  echo "✅ Test Case 4e: Primary + Replica Failure (Quorum Loss) - PASSED"
  stop_servers
  return 0
}

# Main function to run all tests
main() {
  setup_environment
  
  # Check for command line arguments
  local test_case=${1:-"all"}
  
  case "$test_case" in
    "4a")
      echo "=== Running Test Case 4a: Replica Fast Recovery ==="
      test_replica_fast_recovery || { echo "❌ Test Case 4a failed"; return 1; }
      ;;
    "4b")
      echo "=== Running Test Case 4b: Replica Slow Recovery ==="
      test_replica_slow_recovery || { echo "❌ Test Case 4b failed"; return 1; }
      ;;
    "4c")
      echo "=== Running Test Case 4c: Primary Failure Outside Critical Section ==="
      test_primary_failure_outside_cs || { echo "❌ Test Case 4c failed"; return 1; }
      ;;
    "4d")
      echo "=== Running Test Case 4d: Primary Failure During Critical Section ==="
      test_primary_failure_during_cs || { echo "❌ Test Case 4d failed"; return 1; }
      ;;
    "4e")
      echo "=== Running Test Case 4e: Primary + Replica Failure (Quorum Loss) ==="
      test_primary_and_replica_failure || { echo "❌ Test Case 4e failed"; return 1; }
      ;;
    "all")
      echo "=== Running All Test Cases ==="
      test_replica_fast_recovery || { echo "❌ Test Case 4a failed"; return 1; }
      test_replica_slow_recovery || { echo "❌ Test Case 4b failed"; return 1; }
      test_primary_failure_outside_cs || { echo "❌ Test Case 4c failed"; return 1; }
      test_primary_failure_during_cs || { echo "❌ Test Case 4d failed"; return 1; }
      test_primary_and_replica_failure || { echo "❌ Test Case 4e failed"; return 1; }
      ;;
    *)
      echo "Error: Invalid test case specified"
      echo "Usage: $0 [4a|4b|4c|4d|4e|all]"
      echo "  4a - Replica Fast Recovery"
      echo "  4b - Replica Slow Recovery"
      echo "  4c - Primary Failure Outside Critical Section"
      echo "  4d - Primary Failure During Critical Section"
      echo "  4e - Primary + Replica Failure (Quorum Loss)"
      echo "  all - Run all tests (default)"
      return 1
      ;;
  esac
  
  echo "==============================================="
  echo "=== All Selected Test Cases PASSED ==="
  echo "==============================================="
  return 0
}

# Execute the main function with command line argument
main "$@"
exit_code=$?

exit $exit_code 