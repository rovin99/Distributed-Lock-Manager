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

## Test Case 4a: Replica Fast Recovery

**Scenario:** A replica node crashes or becomes temporarily unavailable but recovers quickly (within typical network timeouts or brief partition durations).

**How the Design Handles It:**

- **Primary Operation:** The Primary server continues operating normally as long as it maintains quorum (itself + the other remaining replica).
- **Quorum Check:** The Primary's periodic quorum checks before processing client writes will succeed, confirming connectivity with the healthy replica.
- **Asynchronous Replication:** Replication attempts from the Primary to the failed replica will fail silently or time out, but do not block the Primary due to the asynchronous nature of `replicateStateToPeers`.
- **Replica Recovery:** When the failed replica restarts, it loads its last persisted state from `lock_state.json`.
- **Re-establishing Connection:** The recovered replica attempts to connect to peers (`connectToPeers`). Other nodes (especially the Primary) also attempt connections.
- **State Synchronization:** Once connected, the recovered replica starts receiving new asynchronous `UpdateSecondaryState` messages from the Primary, eventually catching up to the current lock state. If using the reliable replication worker, the worker on the primary might retry sending the latest state upon successful reconnection.
- **Client Impact:** Clients communicating with the Primary experience no disruption.

**Outcome:** The system remains available, client operations continue, and the recovered replica seamlessly rejoins the cluster and synchronizes its state.

## Test Case 4b: Replica Slow Recovery

**Scenario:** A replica node crashes or becomes partitioned for an extended period, exceeding typical timeouts, but eventually recovers.

**How the Design Handles It:**

- **Primary Operation & Quorum:** Same as 4a, the Primary continues operating normally as long as it maintains quorum with the single remaining healthy replica.
- **Failed Replication:** Replication attempts to the down replica consistently fail but do not impact the Primary's operation.
- **Client Impact:** Clients communicating with the Primary experience no disruption.
- **Replica Recovery (Late):** When the replica finally recovers:
  - It loads its (potentially very stale) persisted state.
  - It connects to peers (`connectToPeers`).
  - It starts receiving the current state via `UpdateSecondaryState` from the Primary, overwriting its stale state.
- **State Synchronization:** The recovered replica catches up to the current lock state via subsequent replication messages.

**Outcome:** The system remains available during the extended replica outage. The replica successfully rejoins and synchronizes upon recovery without manual intervention.

## Test Case 4c: Primary Failure Outside Critical Section

**Scenario:** The Primary node crashes when no client holds the lock, or after a client has successfully acquired, used, and released the lock.

**How the Design Handles It:**

- **Failure Detection:** Replicas (S2, S3) detect the Primary (S1) failure via repeated failed Ping heartbeats.
- **Election Initiation:** A replica (e.g., S2) reaches its heartbeat failure threshold and initiates an election (`tryStartElection`).
- **Election Quorum Check:** S2 pings the other replica (S3). Since S3 is alive, a quorum of 2 exists ({S2, S3}).
- **Winner Determination:** The replica with the lowest server ID among the live quorum (e.g., S2 if IDs are 1, 2, 3) is elected as the new Primary.
- **Promotion & Fencing:** The winner (S2) promotes itself (`promoteToPrimary`), updates its internal state (`isPrimary=true`, `knownPrimaryID=S2`), and immediately enters the Fencing period. The other replica (S3) updates its `knownPrimaryID` to S2 and starts heartbeating S2.
- **Client Failover:** Clients attempting to contact the crashed S1 will fail. Their `retryRPC` logic triggers `tryNextServer`, cycling through addresses until they reach the new Primary (S2).
- **Fencing Handling:** Initial client requests to the new Primary (S2) are rejected with `SERVER_FENCING`. Clients continue retrying/failing over.
- **Post-Fencing Operation:** S2 completes its fencing period, calls `lockManager.ForceClearLockState()` (resetting the persisted lock state to unlocked), and begins accepting client requests. Client retries to S2 now succeed.
- **Old Primary Recovery:** If the old Primary (S1) restarts, it loads its state, attempts to connect to peers, and likely starts as a Replica (as S2 is now Primary). If it briefly thinks it's Primary, quorum checks or heartbeats from S3 (or pings from S2) will force it to step down (`demoteToReplica`).

**Outcome:** The system detects primary failure, elects a new leader via quorum, ensures safety via fencing, handles client failover, and allows the old primary to rejoin correctly as a replica.

## Test Case 4d: Primary Failure During Critical Section

**Scenario:** The Primary node crashes while a client holds the lock and is potentially performing operations (like file appends).

**How the Design Handles It:**

- **Detection, Election, Fencing:** Same as 4c – failure is detected, a new Primary (e.g., S2) is elected by quorum, and S2 enters fencing.
- **Client Operation Interruption:** The client (C1) holding the lock will find its operations (e.g., `FileAppend`, `RenewLease`) start failing when targeting the crashed Primary (S1).
- **Client Failover:** C1's `retryRPC` logic triggers failover, eventually targeting the new Primary (S2).
- **Fencing Rejection:** C1's requests to S2 during fencing are rejected (`SERVER_FENCING`).
- **Post-Fencing:** S2 finishes fencing and clears the lock state (`ForceClearLockState`).
- **Client Recovery:**
  - C1's subsequent retries to S2 (e.g., for `FileAppend` or `RenewLease`) will now fail with `INVALID_TOKEN` because S2 cleared the state and C1's original token is no longer valid on S2.
  - The client library's `invalidateLock` logic clears C1's local token state.
  - The client application must handle the `INVALID_TOKEN` error by recognizing its critical section was interrupted and restarting the entire operation sequence (re-acquire lock from S2, re-do all necessary appends).
- **Atomicity:** Atomicity of the client's multi-step critical section is ensured by this client-side retry logic. Partial work done before the crash (and persisted via WAL by the old Primary) remains, but the client re-does the full sequence, ensuring the final state reflects a complete, atomic execution (though potentially after other clients have run).

**Outcome:** The system recovers via election and fencing. The client whose operation was interrupted detects the failure via token invalidation and must retry its entire transaction, preserving application-level atomicity.

## Test Case 4e: Primary + Replica Failure (Quorum Loss)

**Scenario:** The Primary (S1) and one Replica (S2) crash simultaneously or become unavailable.

**How the Design Handles It:**

- **Failure Detection:** The remaining Replica (S3) detects the Primary failure via heartbeats.
- **Election Attempt:** S3 initiates an election (`tryStartElection`).
- **Quorum Check Failure:** S3 attempts to ping S1 (fails) and S2 (fails). The only live node is itself (count = 1). This is less than the required quorum of 2.
- **Election Abort:** The election process aborts. S3 remains a Replica and logs the quorum failure.
- **System Unavailability:** Since no node can establish itself as Primary (due to lack of quorum), the DLM service becomes unavailable.
- **Client Impact:** Clients attempting operations will fail to connect to S1 and S2. When they failover to S3, S3 will reject write requests with `SECONDARY_MODE`. Clients eventually exhaust retries and report `ServerUnavailableError`.
- **Recovery:**
  - If either S1 or S2 recovers, the cluster now has 2 live nodes.
  - The node that was already live (S3) or the newly recovered node will eventually detect the change (via heartbeats or periodic checks).
  - An election is initiated by one of the live nodes (e.g., S3 detects S2 is back via pings).
  - A quorum of 2 now exists. The node with the lowest ID among the live nodes (e.g., S2 if S1 is still down) is elected Primary.
  - The new Primary fences, clears state, and service is restored.

**Outcome:** The system correctly detects the loss of majority and becomes unavailable, preventing split-brain. Service is restored automatically once a quorum is re-established and a new leader is elected.
