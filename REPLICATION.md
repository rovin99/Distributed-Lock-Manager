# Primary-Secondary Replication for Distributed Lock Manager

This document explains the primary-secondary replication implementation in the Distributed Lock Manager system, following the "Hybrid Approach" design.

## Overview

The system implements a simple primary-secondary replication with the following characteristics:

1. **Primary Server**: Handles all client requests and replicates lock state to the secondary
2. **Secondary Server**: Rejects client requests, receives state updates from primary, monitors primary health
3. **Heartbeat Mechanism**: Secondary periodically checks if primary is alive
4. **Failover Process**: Secondary promotes itself to primary if the original primary fails
5. **Fencing Period**: New primary enters a fencing period to prevent split-brain problems
6. **Client Failover**: Clients can handle server failures and reconnect to the new primary

## Running the System

Use the provided `run_replicated.sh` script to start both primary and secondary servers:

```bash
# Basic usage - starts primary on port 50051 and secondary on port 50052
./run_replicated.sh

# Clear data before starting servers
./run_replicated.sh --clear-data

# Run automated failover test
./run_replicated.sh --failover-test
```

## Testing Failover

You can test failover in two ways:

1. **Manual Testing**:
   
   Start the servers:
   ```bash
   ./run_replicated.sh
   ```
   
   In another terminal, run a client that acquires and holds a lock:
   ```bash
   bin/lock_client hold --servers=localhost:50051,localhost:50052 --client-id=1 --timeout=120s
   ```
   
   Kill the primary server (the script will show the PID):
   ```bash
   kill <PRIMARY_PID>
   ```
   
   Observe the secondary server promoting itself (in logs/secondary.log) and the client reconnecting.

2. **Automated Testing**:
   
   ```bash
   ./run_replicated.sh --failover-test
   ```
   
   This will automatically start servers, run a client, kill the primary, and verify the client survives.

## Client Usage

The client supports multiple server addresses for failover:

```bash
# Basic lock acquisition with failover support
bin/lock_client acquire --servers=localhost:50051,localhost:50052 --client-id=1

# Acquire a lock, hold it for 60 seconds
bin/lock_client hold --servers=localhost:50051,localhost:50052 --client-id=1 --timeout=60s

# Append to a file (with automatic lock acquire/release)
bin/lock_client append --servers=localhost:50051,localhost:50052 --client-id=1 --file=file_0 --content="test data"

# Release a lock
bin/lock_client release --servers=localhost:50051,localhost:50052 --client-id=1
```

## Implementation Details

### Heartbeat Mechanism

The secondary server sends heartbeats to the primary every 2 seconds. If 3 consecutive heartbeats fail, the secondary starts the failover process.

### Failover Process

When the primary is detected as unreachable, the secondary:

1. Changes its role to primary
2. Enters a fencing period (lease duration + 5 seconds)
3. Rejects lock acquisition requests during fencing
4. Clears any stale lock state after fencing ends
5. Becomes fully operational as the new primary

### Fencing

Fencing prevents split-brain scenarios by:

1. Rejecting lock acquisitions during the fencing period
2. Allowing existing lock holders to release their locks
3. Forcibly clearing lock state after the fencing period ends

### Client Failover

The client library automatically:

1. Detects server failures
2. Tries to connect to the next server in the configured server list
3. Invalidates local state on failover (hasLock, lockToken)
4. Retries the original operation with the new server

## Monitoring

You can monitor the servers using:

```bash
# Monitor primary server
tail -f logs/primary.log

# Monitor secondary server
tail -f logs/secondary.log
```

# Enhanced Multi-Node Replication

In addition to the basic primary-secondary model, the system now supports an enhanced multi-node replication mode with leader election and majority-based consensus.

## Overview

The enhanced replication system has the following characteristics:

1. **Multiple Servers**: Supports 3 or more servers (odd number recommended for majority votes)
2. **Server States**: Each server can be in one of three states:
   - **Leader**: Processes client requests and broadcasts state updates
   - **Follower**: Receives updates from the leader and monitors leader health
   - **Candidate**: Temporary state when attempting to become the leader
3. **Epoch-Based Leadership**: Uses monotonically increasing epoch numbers to track leadership terms
4. **Leader Election**: Uses a simplified election protocol based on majority acknowledgment
5. **Fencing Mechanism**: Preserved from the 2-node system to ensure safety during failover

## Running Multi-Node Setup

To run a 3-node cluster:

```bash
# Start Node 1 (initial leader)
bin/lock_server --role primary --id 1 --address ":50051" --peers "localhost:50052,localhost:50053"

# Start Node 2 (follower)
bin/lock_server --role secondary --id 2 --address ":50052" --peers "localhost:50051,localhost:50053"

# Start Node 3 (follower)
bin/lock_server --role secondary --id 3 --address ":50053" --peers "localhost:50051,localhost:50052"
```

## Leader Election

The leader election process works as follows:

1. **Election Trigger**: If a follower doesn't receive heartbeats from the leader, it starts an election
2. **Promotion Attempt**: 
   - Server transitions to Candidate state
   - Increments epoch and votes for itself
   - Broadcasts ProposePromotion RPCs to all peers
3. **Voting Rules**:
   - A server votes only once per epoch
   - Vote is granted if the candidate's epoch is >= the server's current epoch
4. **Leadership Decision**:
   - Candidate becomes Leader if it receives votes from a majority of servers
   - Otherwise, it times out and tries again or steps down if it sees a higher epoch

## Epoch-Based Safety

Epochs provide a logical clock to order events:

1. All messages include the epoch number
2. Servers reject messages with epochs lower than their own
3. If a server receives a message with a higher epoch, it updates its own epoch and becomes a Follower
4. This prevents split-brain scenarios and ensures everyone follows the latest legitimate leader

## Testing Multi-Node Replication

Use the test script to verify the multi-node setup:

```bash
./test_advanced_replication.sh multi-node
```

The test script will:
1. Start a 3-node cluster
2. Verify basic operations 
3. Test failover scenarios
4. Ensure split-brain prevention works correctly 