# Replication System Implementation

This document outlines the implementation stages of the replication system for the Distributed Lock Manager.

## Stage 1: Connection Management (Completed)

Stage 1 focused on establishing the foundation for the replication system by implementing robust connection management between servers.

### Components Implemented

#### 1. Peer Connection (`peer.go`)
- **Connection State Management**
  - Implemented `ConnectionState` enum (Disconnected, Connecting, Connected)
  - Added state transitions with proper synchronization
  - Implemented connection status tracking and reporting

- **Connection Establishment**
  - Implemented exponential backoff retry mechanism
  - Added connection timeout handling
  - Implemented proper gRPC connection establishment
  - Added detailed logging for connection attempts and failures

- **Error Handling**
  - Added comprehensive error tracking
  - Implemented connection failure recovery
  - Added detailed error reporting and logging

#### 2. Connection Pool (`connection.go`)
- **Pool Management**
  - Implemented thread-safe connection pool
  - Added connection reuse and cleanup
  - Implemented connection state verification
  - Added connection lifecycle management

- **Connection Verification**
  - Added connection state checking
  - Implemented connection readiness verification
  - Added connection health monitoring
  - Implemented connection cleanup for failed connections

#### 3. Replication Manager (`manager.go`)
- **Manager Initialization**
  - Implemented server configuration handling
  - Added peer discovery and initialization
  - Implemented manager startup and shutdown
  - Added proper resource cleanup

- **Peer Management**
  - Implemented peer tracking and status monitoring
  - Added peer connection management
  - Implemented peer status reporting
  - Added peer reconnection handling

#### 4. Configuration (`config.go`)
- **Configuration Structures**
  - Defined `ServerConfig` for server settings
  - Implemented `ConnectionConfig` for connection parameters
  - Added `PeerInfo` for peer information
  - Implemented configuration validation

- **Default Configuration**
  - Added sensible defaults for all parameters
  - Implemented configuration customization
  - Added configuration validation

#### 5. Testing
- **Comprehensive Test Suite**
  - Added unit tests for all components
  - Implemented integration tests for the replication system
  - Added test utilities and helpers
  - Implemented test cleanup and verification

- **Test Coverage**
  - Connection establishment and failure
  - Connection pool management
  - Replication manager functionality
  - Configuration handling

#### 6. Cluster Management
- **Scripts**
  - Implemented `run_cluster.sh` for starting the cluster
  - Added `check_replication_detailed.sh` for status checking
  - Implemented proper server startup sequencing
  - Added logging and monitoring

### Key Features
- Robust connection handling with retries and backoff
- Thread-safe connection management
- Comprehensive error handling and reporting
- Detailed logging and status monitoring
- Proper resource cleanup and management

## Stage 2: Replication Protocol (Completed)

Stage 2 focused on implementing the replication protocol for the distributed lock manager.

### Components Implemented

#### 1. Replication Protocol (`protocol.go`)
- **Protocol Definition**
  - Implemented `OperationType` enum (Prepare, Commit, Abort)
  - Added `ReplicationRequest` and `ReplicationResponse` structures
  - Implemented two-phase commit protocol
  - Added protocol versioning and compatibility checks

- **Two-Phase Commit**
  - Implemented prepare phase with timeout handling
  - Added commit phase with consistency checks
  - Implemented abort phase for rollback
  - Added operation state tracking

- **State Management**
  - Implemented operation state tracking
  - Added timeout handling for operations
  - Implemented cleanup for completed operations
  - Added state consistency verification

#### 2. Error Handling
- **Comprehensive Error Handling**
  - Added detailed error reporting
  - Implemented error recovery mechanisms
  - Added error logging and monitoring
  - Implemented error state tracking

- **Timeout Handling**
  - Implemented operation timeouts
  - Added timeout recovery mechanisms
  - Implemented timeout logging
  - Added timeout state tracking

#### 3. Testing
- **Protocol Testing**
  - Added unit tests for protocol operations
  - Implemented integration tests for the protocol
  - Added test utilities and helpers
  - Implemented test cleanup and verification

- **Test Coverage**
  - Prepare, commit, and abort operations
  - Timeout handling
  - Error recovery
  - State consistency

### Key Features
- Robust two-phase commit protocol
- Comprehensive error handling and recovery
- Detailed logging and monitoring
- Proper resource cleanup and management
- Extensive test coverage

## Stage 3: Lock Replication (Completed)

Stage 3 focuses on implementing lock replication for the distributed lock manager.

### Planned Components

#### 1. Lock Replication
- **Lock State Replication**
  - Implement lock state replication
  - Add lock state consistency checks
  - Implement lock state reconciliation
  - Add lock state versioning

- **Lock Operation Replication**
  - Implement lock acquisition replication
  - Add lock release replication
  - Implement lock renewal replication
  - Add lock operation ordering

#### 2. Conflict Resolution
- **Lock Conflicts**
  - Implement lock conflict detection
  - Add conflict resolution strategies
  - Implement conflict logging
  - Add conflict reporting

- **Resolution Strategies**
  - Implement first-come-first-served resolution
  - Add priority-based resolution
  - Implement custom resolution strategies
  - Add resolution strategy configuration

#### 3. Lock Recovery
- **Lock State Recovery**
  - Implement lock state recovery after failures
  - Add lock state verification
  - Implement lock state cleanup
  - Add lock state reporting

- **Recovery Mechanisms**
  - Implement automatic lock recovery
  - Add manual lock recovery
  - Implement lock recovery verification
  - Add lock recovery reporting

#### 4. Performance Optimization
- **Replication Optimization**
  - Implement batch replication
  - Add replication pipelining
  - Implement replication compression
  - Add replication prioritization

- **Resource Management**
  - Implement resource usage monitoring
  - Add resource usage optimization
  - Implement resource cleanup
  - Add resource reporting

### Key Features (Planned)
- Robust lock state replication
- Comprehensive conflict resolution
- Automatic lock recovery after failures
- Performance optimizations for replication
- Detailed logging and monitoring

## Implementation Timeline

1. **Stage 1: Connection Management** (Completed)
   - Peer connection implementation
   - Connection pool implementation
   - Replication manager implementation
   - Configuration implementation
   - Testing and verification

2. **Stage 2: Replication Protocol** (Completed)
   - Protocol definition and implementation
   - Two-phase commit implementation
   - Error handling and recovery
   - Testing and verification

3. **Stage 3: Lock Replication** (Completed)
   - Lock state replication
   - Conflict resolution
   - Lock recovery
   - Performance optimization

## Testing Strategy

### Unit Testing
- Individual component testing
- Interface compliance testing
- Error handling testing
- Configuration testing

### Integration Testing
- Component interaction testing
- End-to-end functionality testing
- Failure scenario testing
- Performance testing

### System Testing
- Full system deployment testing
- Load testing
- Stress testing
- Long-running stability testing

## Monitoring and Observability

### Logging
- Detailed connection logs
- Replication operation logs
- Error and warning logs
- Performance metrics logs

### Metrics
- Connection status metrics
- Replication performance metrics
- Error rate metrics
- Resource usage metrics

### Alerts
- Connection failure alerts
- Replication failure alerts
- Performance degradation alerts
- Resource exhaustion alerts

## Detailed Review and Suggestions for Improvement

### Stage 1: Basic Server Configuration and Peer Discovery

#### What's Working Well
- Clean `ServerConfig` structure to hold server and peer information
- Practical command-line parsing for peer addresses
- Solid foundation for communication with gRPC connections

#### Suggestions for Improvement

##### Retry Mechanism for Peer Connections
If a peer connection fails (e.g., due to a temporary network issue), the current code logs the error and moves on. Consider adding a retry mechanism to attempt reconnection a few times before giving up:

```go
func (s *LockServer) connectToPeers() {
    for _, peer := range s.config.Peers {
        var conn *grpc.ClientConn
        var err error
        for attempts := 0; attempts < 3; attempts++ {
            conn, err = grpc.Dial(peer.Address, grpc.WithInsecure())
            if err == nil {
                break
            }
            time.Sleep(time.Second * time.Duration(attempts+1)) // Exponential backoff
        }
        if err != nil {
            s.logger.Printf("Failed to connect to peer %s after retries: %v", peer.ID, err)
            continue
        }
        s.peers[peer.ID] = conn
    }
}
```

##### Periodic Reconnection
Add a background goroutine to periodically check and reconnect to failed peers, enhancing resilience.

##### Security
Currently using `grpc.WithInsecure()`, which is fine for development, but plan to switch to secure connections (e.g., TLS) in production.

##### Testing Enhancement
Add a test to simulate a peer being unavailable initially and then coming online, verifying that the server eventually connects.

### Stage 2: Basic State Replication

#### What's Working Well
- Straightforward `ReplicationService` in the proto file
- Good starting point with replicating state after local lock acquisition
- Helpful logging of replication failures for debugging

#### Suggestions for Improvement

##### Consistency Guarantee
Currently, the system acquires the lock locally and then replicates to peers, but if replication fails, the system could become inconsistent. To ensure consistency, consider a two-phase approach:

1. **Prepare Phase**: Send the replication request to all peers and wait for acknowledgment.
2. **Commit Phase**: If enough peers agree, commit the change locally and confirm to peers.

```go
func (s *LockServer) LockAcquire(ctx context.Context, args *pb.LockArgs) (*pb.LockResponse, error) {
    // Prepare phase
    prepareSuccess := true
    for peerID, conn := range s.peers {
        client := pb.NewReplicationServiceClient(conn)
        resp, err := client.ReplicateState(ctx, &pb.ReplicationRequest{
            OperationType: "prepare_acquire",
            ClientId:      args.ClientId,
            LockId:        args.LockId,
        })
        if err != nil || !resp.Success {
            s.logger.Printf("Prepare failed for peer %s: %v", peerID, err)
            prepareSuccess = false
        }
    }
    if !prepareSuccess {
        return &pb.LockResponse{Status: pb.Status_ERROR, ErrorMessage: "Prepare failed"}, nil
    }

    // Local acquire and commit
    success, token := s.lockManager.AcquireWithTimeout(args.ClientId, ctx)
    if !success {
        return &pb.LockResponse{Status: pb.Status_ERROR, ErrorMessage: "Failed to acquire lock"}, nil
    }
    for peerID, conn := range s.peers {
        client := pb.NewReplicationServiceClient(conn)
        client.ReplicateState(ctx, &pb.ReplicationRequest{
            OperationType: "commit_acquire",
            ClientId:      args.ClientId,
            Token:         token,
            LockId:        args.LockId,
        })
    }
    return &pb.LockResponse{Status: pb.Status_OK, Token: token}, nil
}
```

##### Error Handling
If replication to a peer fails, the system continues with others but doesn't rollback the local change. Consider tracking failures and deciding whether to proceed based on a minimum number of successful replications.

##### Testing Enhancement
Test a scenario where one peer fails to replicate, ensuring the system either rolls back or logs the inconsistency appropriately.

### Stage 3: Leader Election

#### What's Working Well
- Well-defined proto messages for voting and heartbeats
- Good starting point with basic election logic (self-vote plus peer votes)
- Correct design choice to restrict operations to the leader

#### Suggestions for Improvement

##### Robustness
The current leader election is simple but vulnerable to network partitions or split-brain scenarios. Consider adopting an established algorithm like Raft or Paxos. For a simpler alternative, enhance the timeout-based approach:

```go
func (s *LockServer) startElection() {
    s.election.term++
    s.election.votedFor = s.config.ServerID
    votes := 1

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    var mu sync.Mutex
    for peerID, conn := range s.peers {
        go func(id string, client pb.ReplicationServiceClient) {
            resp, err := client.RequestVote(ctx, &pb.VoteRequest{
                Term:        s.election.term,
                CandidateId: s.config.ServerID,
            })
            mu.Lock()
            if err == nil && resp.VoteGranted {
                votes++
            }
            mu.Unlock()
        }(peerID, pb.NewReplicationServiceClient(conn))
    }

    time.Sleep(1 * time.Second) // Wait for votes
    if votes > len(s.peers)/2 {
        s.election.isLeader = true
        s.startHeartbeat()
    }
}
```

##### Heartbeat Frequency
Ensure the `startHeartbeat` method sends heartbeats frequently enough to prevent unnecessary elections but not so often as to overload the network.

##### Testing Enhancement
Simulate a network partition by blocking communication between some servers and verify that only one leader emerges.

### Stage 4: Quorum-Based Operations

#### What's Working Well
- Solid `Quorum` struct and logic to wait for a majority of responses
- Good fail-safe to release the lock locally if quorum isn't reached

#### Suggestions for Improvement

##### Rollback Mechanism
If quorum isn't reached, the system releases the local lock, but peers that already replicated the state might retain it. Enhance the rollback to notify successful peers:

```go
func (s *LockServer) LockAcquire(ctx context.Context, args *pb.LockArgs) (*pb.LockResponse, error) {
    if !s.election.isLeader {
        return &pb.LockResponse{Status: pb.Status_ERROR, ErrorMessage: "Not the leader"}, nil
    }

    success, token := s.lockManager.AcquireWithTimeout(args.ClientId, ctx)
    if !success {
        return &pb.LockResponse{Status: pb.Status_ERROR, ErrorMessage: "Failed to acquire lock"}, nil
    }

    quorum := &Quorum{size: (len(s.peers)/2)+1, responses: make(map[string]bool)}
    successfulPeers := make(map[string]struct{})

    for peerID, conn := range s.peers {
        client := pb.NewReplicationServiceClient(conn)
        resp, err := client.ReplicateState(ctx, &pb.ReplicationRequest{
            OperationType: "acquire",
            ClientId:      args.ClientId,
            Token:         token,
            LockId:        args.LockId,
        })
        if err == nil && resp.Success {
            successfulPeers[peerID] = struct{}{}
            if quorum.AddResponse(peerID, true) {
                return &pb.LockResponse{Status: pb.Status_OK, Token: token}, nil
            }
        }
    }

    // Rollback
    s.lockManager.Release(args.ClientId, token)
    for peerID, conn := range s.peers {
        if _, ok := successfulPeers[peerID]; ok {
            client := pb.NewReplicationServiceClient(conn)
            client.ReplicateState(ctx, &pb.ReplicationRequest{
                OperationType: "release",
                ClientId:      args.ClientId,
                Token:         token,
                LockId:        args.LockId,
            })
        }
    }
    return &pb.LockResponse{Status: pb.Status_ERROR, ErrorMessage: "Failed to get quorum"}, nil
}
```

##### Timeout
Add a timeout to replication attempts to avoid hanging indefinitely if peers are unresponsive.

##### Testing Enhancement
Test with exactly half the peers failing and verify the rollback completes correctly.

### Stage 5: Client Failover

#### What's Working Well
- Simple and effective round-robin approach to trying servers
- Thread-safe server selection with mutex

#### Suggestions for Improvement

##### Leader Discovery
Instead of trying servers sequentially, have the client query servers to find the current leader:

```go
func (c *Client) AcquireLock(ctx context.Context, lockID string) (string, error) {
    c.mu.RLock()
    server := c.currentServer
    c.mu.RUnlock()

    for _, s := range c.servers {
        conn, err := grpc.Dial(s, grpc.WithInsecure())
        if err != nil {
            continue
        }
        client := pb.NewLockServiceClient(conn)
        resp, err := client.LockAcquire(ctx, &pb.LockArgs{ClientId: rand.Int63(), LockId: lockID})
        conn.Close()
        if err == nil {
            c.mu.Lock()
            c.currentServer = s
            c.mu.Unlock()
            return resp.Token, nil
        }
        if resp != nil && resp.ErrorMessage == "Not the leader" {
            // Could extend this to return leader info from servers
            continue
        }
    }
    return "", errors.New("no available servers")
}
```

##### Exponential Backoff
Add backoff between retries to avoid overwhelming the system during failures.

##### Testing Enhancement
Test with multiple clients simultaneously failing over to ensure no race conditions occur.

### General Recommendations

#### Error Handling
Across all stages, ensure errors are logged and handled gracefully. For example, in peer communication, distinguish between temporary and permanent failures.

#### Testing
The testing plans are good—expand them to include edge cases like network partitions, server crashes during replication, and concurrent client requests.

### Implementation Details

#### Lock State Management
- `LockState` struct tracks lock ownership and metadata
- Supports exclusive and shared locks
- Handles lock timeouts and cleanup

#### Lock Operations
- `AcquireLock`: Implements distributed lock acquisition with replication
- `ReleaseLock`: Handles lock release with replication
- `GetLockState`: Retrieves current lock state
- `HandleLockTimeout`: Manages lock expiration
- `ReplicateLockState`: Replicates lock state to peers

#### Testing
- Unit tests for all lock operations
- Integration tests for distributed scenarios
- Concurrent access testing
- Timeout and recovery testing

## Stage 4: Leader Election (In Progress)
- Implement leader election protocol
- Add failure detection
- Create leader state replication
- Add leader change handling
- Implement testing

## Stage 5: Client Failover (Planned)
- Implement client failover strategy
- Add automatic client redirection
- Create failover testing
- Add monitoring and metrics
- Implement documentation

## Implementation Timeline
1. Stage 1: Connection Management ✓
2. Stage 2: Replication Protocol ✓
3. Stage 3: Lock Replication ✓
4. Stage 4: Leader Election (In Progress)
5. Stage 5: Client Failover (Planned)

## Testing Strategy
- Unit tests for each component
- Integration tests for distributed scenarios
- Performance testing
- Failure scenario testing
- Monitoring and observability

## Monitoring and Observability

### Logging
- Detailed connection logs
- Replication operation logs
- Error and warning logs
- Performance metrics logs

### Metrics
- Connection status metrics
- Replication performance metrics
- Error rate metrics
- Resource usage metrics

### Alerts
- Connection failure alerts
- Replication failure alerts
- Performance degradation alerts
- Resource exhaustion alerts

## Detailed Review and Suggestions for Improvement

### Stage 1: Basic Server Configuration and Peer Discovery

#### What's Working Well
- Clean `ServerConfig` structure to hold server and peer information
- Practical command-line parsing for peer addresses
- Solid foundation for communication with gRPC connections

#### Suggestions for Improvement

##### Retry Mechanism for Peer Connections
If a peer connection fails (e.g., due to a temporary network issue), the current code logs the error and moves on. Consider adding a retry mechanism to attempt reconnection a few times before giving up:

```go
func (s *LockServer) connectToPeers() {
    for _, peer := range s.config.Peers {
        var conn *grpc.ClientConn
        var err error
        for attempts := 0; attempts < 3; attempts++ {
            conn, err = grpc.Dial(peer.Address, grpc.WithInsecure())
            if err == nil {
                break
            }
            time.Sleep(time.Second * time.Duration(attempts+1)) // Exponential backoff
        }
        if err != nil {
            s.logger.Printf("Failed to connect to peer %s after retries: %v", peer.ID, err)
            continue
        }
        s.peers[peer.ID] = conn
    }
}
```

##### Periodic Reconnection
Add a background goroutine to periodically check and reconnect to failed peers, enhancing resilience.

##### Security
Currently using `grpc.WithInsecure()`, which is fine for development, but plan to switch to secure connections (e.g., TLS) in production.

##### Testing Enhancement
Add a test to simulate a peer being unavailable initially and then coming online, verifying that the server eventually connects.

### Stage 2: Basic State Replication

#### What's Working Well
- Straightforward `ReplicationService` in the proto file
- Good starting point with replicating state after local lock acquisition
- Helpful logging of replication failures for debugging

#### Suggestions for Improvement

##### Consistency Guarantee
Currently, the system acquires the lock locally and then replicates to peers, but if replication fails, the system could become inconsistent. To ensure consistency, consider a two-phase approach:

1. **Prepare Phase**: Send the replication request to all peers and wait for acknowledgment.
2. **Commit Phase**: If enough peers agree, commit the change locally and confirm to peers.

```go
func (s *LockServer) LockAcquire(ctx context.Context, args *pb.LockArgs) (*pb.LockResponse, error) {
    // Prepare phase
    prepareSuccess := true
    for peerID, conn := range s.peers {
        client := pb.NewReplicationServiceClient(conn)
        resp, err := client.ReplicateState(ctx, &pb.ReplicationRequest{
            OperationType: "prepare_acquire",
            ClientId:      args.ClientId,
            LockId:        args.LockId,
        })
        if err != nil || !resp.Success {
            s.logger.Printf("Prepare failed for peer %s: %v", peerID, err)
            prepareSuccess = false
        }
    }
    if !prepareSuccess {
        return &pb.LockResponse{Status: pb.Status_ERROR, ErrorMessage: "Prepare failed"}, nil
    }

    // Local acquire and commit
    success, token := s.lockManager.AcquireWithTimeout(args.ClientId, ctx)
    if !success {
        return &pb.LockResponse{Status: pb.Status_ERROR, ErrorMessage: "Failed to acquire lock"}, nil
    }
    for peerID, conn := range s.peers {
        client := pb.NewReplicationServiceClient(conn)
        client.ReplicateState(ctx, &pb.ReplicationRequest{
            OperationType: "commit_acquire",
            ClientId:      args.ClientId,
            Token:         token,
            LockId:        args.LockId,
        })
    }
    return &pb.LockResponse{Status: pb.Status_OK, Token: token}, nil
}
```

##### Error Handling
If replication to a peer fails, the system continues with others but doesn't rollback the local change. Consider tracking failures and deciding whether to proceed based on a minimum number of successful replications.

##### Testing Enhancement
Test a scenario where one peer fails to replicate, ensuring the system either rolls back or logs the inconsistency appropriately.

### Stage 3: Leader Election

#### What's Working Well
- Well-defined proto messages for voting and heartbeats
- Good starting point with basic election logic (self-vote plus peer votes)
- Correct design choice to restrict operations to the leader

#### Suggestions for Improvement

##### Robustness
The current leader election is simple but vulnerable to network partitions or split-brain scenarios. Consider adopting an established algorithm like Raft or Paxos. For a simpler alternative, enhance the timeout-based approach:

```go
func (s *LockServer) startElection() {
    s.election.term++
    s.election.votedFor = s.config.ServerID
    votes := 1

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    var mu sync.Mutex
    for peerID, conn := range s.peers {
        go func(id string, client pb.ReplicationServiceClient) {
            resp, err := client.RequestVote(ctx, &pb.VoteRequest{
                Term:        s.election.term,
                CandidateId: s.config.ServerID,
            })
            mu.Lock()
            if err == nil && resp.VoteGranted {
                votes++
            }
            mu.Unlock()
        }(peerID, pb.NewReplicationServiceClient(conn))
    }

    time.Sleep(1 * time.Second) // Wait for votes
    if votes > len(s.peers)/2 {
        s.election.isLeader = true
        s.startHeartbeat()
    }
}
```

##### Heartbeat Frequency
Ensure the `startHeartbeat` method sends heartbeats frequently enough to prevent unnecessary elections but not so often as to overload the network.

##### Testing Enhancement
Simulate a network partition by blocking communication between some servers and verify that only one leader emerges.

### Stage 4: Quorum-Based Operations

#### What's Working Well
- Solid `Quorum` struct and logic to wait for a majority of responses
- Good fail-safe to release the lock locally if quorum isn't reached

#### Suggestions for Improvement

##### Rollback Mechanism
If quorum isn't reached, the system releases the local lock, but peers that already replicated the state might retain it. Enhance the rollback to notify successful peers:

```go
func (s *LockServer) LockAcquire(ctx context.Context, args *pb.LockArgs) (*pb.LockResponse, error) {
    if !s.election.isLeader {
        return &pb.LockResponse{Status: pb.Status_ERROR, ErrorMessage: "Not the leader"}, nil
    }

    success, token := s.lockManager.AcquireWithTimeout(args.ClientId, ctx)
    if !success {
        return &pb.LockResponse{Status: pb.Status_ERROR, ErrorMessage: "Failed to acquire lock"}, nil
    }

    quorum := &Quorum{size: (len(s.peers)/2)+1, responses: make(map[string]bool)}
    successfulPeers := make(map[string]struct{})

    for peerID, conn := range s.peers {
        client := pb.NewReplicationServiceClient(conn)
        resp, err := client.ReplicateState(ctx, &pb.ReplicationRequest{
            OperationType: "acquire",
            ClientId:      args.ClientId,
            Token:         token,
            LockId:        args.LockId,
        })
        if err == nil && resp.Success {
            successfulPeers[peerID] = struct{}{}
            if quorum.AddResponse(peerID, true) {
                return &pb.LockResponse{Status: pb.Status_OK, Token: token}, nil
            }
        }
    }

    // Rollback
    s.lockManager.Release(args.ClientId, token)
    for peerID, conn := range s.peers {
        if _, ok := successfulPeers[peerID]; ok {
            client := pb.NewReplicationServiceClient(conn)
            client.ReplicateState(ctx, &pb.ReplicationRequest{
                OperationType: "release",
                ClientId:      args.ClientId,
                Token:         token,
                LockId:        args.LockId,
            })
        }
    }
    return &pb.LockResponse{Status: pb.Status_ERROR, ErrorMessage: "Failed to get quorum"}, nil
}
```

##### Timeout
Add a timeout to replication attempts to avoid hanging indefinitely if peers are unresponsive.

##### Testing Enhancement
Test with exactly half the peers failing and verify the rollback completes correctly.

### Stage 5: Client Failover

#### What's Working Well
- Simple and effective round-robin approach to trying servers
- Thread-safe server selection with mutex

#### Suggestions for Improvement

##### Leader Discovery
Instead of trying servers sequentially, have the client query servers to find the current leader:

```go
func (c *Client) AcquireLock(ctx context.Context, lockID string) (string, error) {
    c.mu.RLock()
    server := c.currentServer
    c.mu.RUnlock()

    for _, s := range c.servers {
        conn, err := grpc.Dial(s, grpc.WithInsecure())
        if err != nil {
            continue
        }
        client := pb.NewLockServiceClient(conn)
        resp, err := client.LockAcquire(ctx, &pb.LockArgs{ClientId: rand.Int63(), LockId: lockID})
        conn.Close()
        if err == nil {
            c.mu.Lock()
            c.currentServer = s
            c.mu.Unlock()
            return resp.Token, nil
        }
        if resp != nil && resp.ErrorMessage == "Not the leader" {
            // Could extend this to return leader info from servers
            continue
        }
    }
    return "", errors.New("no available servers")
}
```

##### Exponential Backoff
Add backoff between retries to avoid overwhelming the system during failures.

##### Testing Enhancement
Test with multiple clients simultaneously failing over to ensure no race conditions occur.

### General Recommendations

#### Error Handling
Across all stages, ensure errors are logged and handled gracefully. For example, in peer communication, distinguish between temporary and permanent failures.

#### Testing
The testing plans are good—expand them to include edge cases like network partitions, server crashes during replication, and concurrent client requests.

 