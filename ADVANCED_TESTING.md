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

## File Operation Rollback on Lease Expiry

The DLM now includes a feature to handle partial appends when a client's lease expires. This addresses a potential data consistency issue:

**Problem Scenario:**
1. Client C1 acquires a lock with token T1
2. C1 appends 'A' to a file, but then pauses (e.g., during garbage collection)
3. The lease for the lock expires
4. Without a rollback mechanism, the file would contain the partial 'A' operation
5. Server grants the lock to Client C2
6. C2 appends 'B' twice (resulting in file content "ABB")
7. C1 resumes, its append operation fails the token check
8. C1 re-acquires the lock with a new token T3 and appends 'A' twice
9. The file ends up containing "ABBAA" (with unintended 'A' at the beginning)

**Implemented Solution:**
1. The FileManager now tracks all file operations performed with specific lock tokens
2. When a lock lease expires, the LockManager notifies the FileManager through a callback
3. The FileManager rolls back any operations performed with the expired token
4. In the scenario above:
   - C1's initial 'A' is rolled back when T1 expires
   - If C2's lease also expires naturally, its 'BB' operations would be rolled back too
   - If C2 explicitly releases the lock, its operations remain
   - C1's new operations with token T3 remain as long as the lock is held

This implementation ensures that when a client loses its lock due to lease expiry, any file operations it performed with that lock token are rolled back, maintaining data consistency. This prevents "orphaned" operations from clients that lost their locks during execution.

Note that this is a different approach than traditional database transactions - rather than using a two-phase commit protocol, this solution uses token-based operation tracking and rollback. This approach is simpler to implement and provides adequate safety for the distributed lock manager use case.

## Troubleshooting

If you encounter port conflicts when running tests (e.g., "address already in use"), the cleanup function should automatically handle this, but you can also manually kill processes:

```bash
# Kill processes using specific ports
lsof -ti :50051,:50052 | xargs kill -9

# Kill all lock_server and lock_client processes
pkill -9 -f "lock_server"
pkill -9 -f "lock_client"
``` 