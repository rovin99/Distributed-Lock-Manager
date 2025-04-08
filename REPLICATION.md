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