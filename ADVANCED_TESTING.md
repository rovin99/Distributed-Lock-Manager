# Advanced Testing for Distributed Lock Manager

This document explains the advanced testing infrastructure for the Distributed Lock Manager, including various test scripts and their functionality.

## Overview

The test suite includes multiple levels of testing:

1. **Unit Tests** - Testing individual components in isolation
2. **Integration Tests** - Testing interactions between components
3. **System Tests** - Testing the complete system in various scenarios
4. **Failure Mode Tests** - Testing system behavior under failure conditions
5. **Replication Tests** - Testing the replication and consensus mechanisms

## Test Scripts

### 1. Advanced Replication Tests (`test_advanced_replication.sh`)

This script thoroughly tests the hybrid approach to replication in the Distributed Lock Manager.

**To run:**
```bash
./test_advanced_replication.sh
```

**Test cases include:**
- **Enhanced Replication Test** - Verifies state changes are properly replicated from primary to secondary
- **Fencing Behavior Test** - Validates the correct behavior of operations during the fencing period after failover
- **Expanded Failover Test** - Ensures clients can continue operating after failover
- **Improved Split-Brain Test** - Better simulates network partitions to test split-brain prevention
- **Lease Expiry Test** - Verifies that lock leases expire correctly after failover
- **Quorum Loss Test** - Tests the system's behavior when a majority of nodes are unavailable

To run a specific test:
```bash
./test_advanced_replication.sh [replication|fencing|failover|split-brain|lease-expiry|quorum-loss]
```

### 2. Recovery Scenario Tests (`test_recovery_scenarios.sh`)

This script tests the system's ability to recover from various failure scenarios.

**To run:**
```bash
./test_recovery_scenarios.sh
```

**Key scenarios tested include:**
- Server crash and restart
- Primary node failure and secondary promotion
- WAL recovery after crash
- Partial operations during node failure
- Request idempotency during recovery
- Lock state recovery

### 3. Basic Replication Tests (`test_replication.sh`)

This script performs basic replication tests including leader election and quorum detection.

**To run:**
```bash
./test_replication.sh
```

**Test cases include:**
- Initial server role validation
- Lock acquisition and replication verification
- Primary server failure and leader election
- Lock acquisition on the new primary
- Quorum loss testing

## Unit and Integration Tests

The Go test files contain unit and integration tests that can be run with the standard Go testing framework.

**To run all Go tests:**
```bash
go test ./...
```

**To run a specific test:**
```bash
go test ./internal/server -run TestDuplicateReleaseFullSequence
```

**Key test suites include:**

### Failure Mode Tests (`internal/server/failure_test.go`)

These tests verify the system's handling of various failure scenarios:

- **TestDelayedAppend** - Tests append requests that arrive after lock lease expiration
- **TestDuplicateRelease** - Tests duplicate lock release requests
- **TestPacketLossScenario** - Tests retry mechanism for lost packets
- **TestDuplicatedReleasePackets** - Tests handling of duplicated release packets
- **TestDuplicateReleaseFullSequence** - Comprehensive test of the exact sequence for duplicated release packets (Spec Test Case 1c)
- **TestIntegratedAppend** - Tests packet loss during file appends
- **TestClientStuckBeforeEditing** - Tests behavior when a client gets stuck before editing
- **TestClientStuckAfterEditing** - Tests behavior when a client gets stuck after partial editing

### Lock Manager Tests (`internal/lock_manager/lockmanager_test.go`)

These tests focus on the core lock manager functionality:

- **TestLockManagerBasic** - Basic lock acquisition and release
- **TestLockManagerConcurrent** - Concurrent lock operations
- **TestLockContention** - Lock contention between clients
- **TestAcquireWithTimeout** - Lock acquisition with timeouts
- **TestFIFOFairness** - Testing the fairness of lock distribution
- **TestMixedOperation** - Testing mixed operations from multiple clients
- **TestLeaseExpiration** - Testing lease expiration behavior

## Metrics Integration

The tests integrate with the server's metrics system, providing visibility into:
- Fencing activations
- Lock state monitoring
- Heartbeat and replication statistics

Metrics are exposed via HTTP and can be viewed at:
```
http://localhost:8080/metrics
```

## Interpreting Results

Each test outputs detailed progress information and clear pass/fail messages. The final test summary shows how many tests passed or failed.

Detailed logs for each server and client can be found in the `logs/` directory after running the tests.

## Troubleshooting

If you encounter port conflicts when running tests (e.g., "address already in use"), try:

```bash
# Kill processes using specific ports
lsof -ti :50051,:50052 | xargs kill -9

# Kill all lock_server and lock_client processes
pkill -9 -f "lock_server"
pkill -9 -f "lock_client"
```

For test-specific issues, check the corresponding log files in the `logs/` directory. 