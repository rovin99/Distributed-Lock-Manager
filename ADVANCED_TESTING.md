# Advanced Testing for Distributed Lock Manager

This document explains the advanced test cases implemented in `test_advanced_replication.sh` that thoroughly test the hybrid approach to replication in the Distributed Lock Manager.

## Overview

The advanced tests address gaps in the original test coverage to ensure a more comprehensive validation of the DLM's replication, failover, fencing, and state management mechanisms.

## Running the Tests

To run all the advanced tests:

```bash
./test_advanced_replication.sh
```

## Test Cases

### 1. Enhanced Replication Test

**Purpose**: Verifies that state changes are properly replicated from primary to secondary.

**Improvements over original test**:
- Directly checks the secondary's lock state after operations
- Verifies both lock acquisition and release are replicated
- Uses JSON parsing to validate the exact state values

### 2. Fencing Behavior Test

**Purpose**: Validates the correct behavior of operations during the fencing period after failover.

**Improvements over original test**:
- Tests multiple operations against the fencing rules:
  - Lock acquisition (should be rejected)
  - Lock release (should be allowed)
  - File append (should be rejected)
- Checks that operations work correctly after fencing period ends
- Monitors metrics data to verify fencing activation

**Note on Connection Issues**: The test detects if clients are unable to connect to the server during the fencing period. This is a common implementation approach where the server may reject new connections entirely during fencing rather than accepting them and then rejecting specific operations. If connection failures are detected, the test will verify fencing based on server logs instead of client operation results.

### 3. Expanded Failover Test

**Purpose**: Ensures clients can continue operating after failover, not just survive.

**Improvements over original test**:
- Verifies the client remains functional after failover
- Tests actual operations (file append) after primary failure
- Validates client can acquire new locks from the promoted secondary

### 4. Improved Split-Brain Test

**Purpose**: Better simulates network partitions to test split-brain prevention.

**Improvements over original test**:
- Uses iptables for real network partitioning (when available)
- Tests client operations against both servers during the partition
- Verifies only one server accepts lock operations
- Checks for inconsistency in lock grants during split-brain scenarios

### 5. Lease Expiry Test

**Purpose**: Verifies that lock leases expire correctly after failover.

**New test (not in original)**:
- Acquires a lock with a short lease
- Kills the primary to trigger failover
- Waits for lease expiry during/after the fencing period
- Verifies a new client can acquire the lock after expiry

## Metrics Integration

The tests now integrate with the server's metrics system, which provides:
- Visibility into fencing activations
- Lock state monitoring
- Heartbeat and replication statistics

Metrics are exposed via HTTP and can be viewed at:
```
http://localhost:8080/metrics
```

## Requirements

- Go 1.15 or higher
- jq (for JSON parsing)
- nc (for port checking)
- curl (for metrics retrieval)
- iptables (optional, for improved split-brain testing)

## Interpreting Results

Each test will output detailed progress information and clear pass/fail messages. The final test summary will show how many tests passed or failed.

Detailed logs for each server and client can be found in the `logs/` directory after running the tests.

## Troubleshooting

If you encounter port conflicts when running tests (e.g., "address already in use"), the cleanup function should automatically handle this, but you can also manually kill processes:

```bash
# Kill processes using specific ports
lsof -ti :50051,:50052 | xargs kill -9

# Kill all lock_server and lock_client processes
pkill -9 -f "lock_server"
pkill -9 -f "lock_client"
``` 