package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"Distributed-Lock-Manager/internal/file_manager"
	"Distributed-Lock-Manager/internal/lock_manager"
	pb "Distributed-Lock-Manager/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ServerRole defines the role of the server
type ServerRole string

const (
	// PrimaryRole indicates a primary server that accepts client requests
	PrimaryRole ServerRole = "primary"
	// SecondaryRole indicates a secondary server that replicates from primary
	SecondaryRole ServerRole = "secondary"
)

// HeartbeatConfig stores configuration for heartbeat mechanism
type HeartbeatConfig struct {
	Interval        time.Duration // Interval between heartbeats
	Timeout         time.Duration // Timeout for each heartbeat
	MaxFailureCount int           // Number of consecutive failures before failover
}

// Default heartbeat configuration
var DefaultHeartbeatConfig = HeartbeatConfig{
	Interval:        2 * time.Second,
	Timeout:         5 * time.Second,
	MaxFailureCount: 3,
}

// LockServer implements the LockServiceServer interface
type LockServer struct {
	pb.UnimplementedLockServiceServer
	lockManager  *lock_manager.LockManager
	fileManager  *file_manager.FileManager
	requestCache *RequestCache
	logger       *log.Logger
	recoveryDone bool                // Indicates if WAL recovery is complete
	metrics      *PerformanceMetrics // Added metrics for monitoring

	// Replication-related fields
	role               ServerRole                     // Current role (primary or secondary)
	isPrimary          bool                           // Is this server the primary?
	serverID           int32                          // ID of this server
	allServerAddresses map[int32]string               // Map of server ID -> address
	peerClients        map[int32]pb.LockServiceClient // Map of server ID -> gRPC client
	peerConns          map[int32]*grpc.ClientConn     // Map of server ID -> gRPC connection
	clusterSize        int                            // Total number of servers (e.g., 3)
	knownPrimaryID     int32                          // Server ID this node believes is the current primary

	// Atomic flags for election and primary status
	primarySuspected   atomic.Bool // Flag if this node suspects the primary is down
	electionInProgress atomic.Bool // Prevent concurrent elections

	// Replication queue for reliable state updates
	replicationQueue      []*pb.ReplicatedState // Queue of updates to be sent to secondary
	replicationQueueMu    sync.Mutex            // Protects the replication queue
	replicationInProgress atomic.Bool           // Flag to prevent multiple concurrent replication workers

	// Heartbeat configuration
	heartbeatConfig HeartbeatConfig // Configuration for heartbeat mechanism

	// Fencing-related fields
	isFencing      atomic.Bool   // Is the server in fencing period?
	fencingEndTime time.Time     // When does the fencing period end?
	fencingBuffer  time.Duration // Additional buffer time after lease duration

	// Added for mutex protection in startHeartbeatSender
	mu sync.Mutex
}

// NewLockServer initializes a new lock server
func NewLockServer() *LockServer {
	logger := log.New(os.Stdout, "[LockServer] ", log.LstdFlags)

	// Initialize lock manager with lease duration
	lm := lock_manager.NewLockManagerWithLeaseDuration(logger, 30*time.Second)

	// Initialize file manager with lock manager for token validation
	fm := file_manager.NewFileManagerWithWAL(true, true, lm)

	s := &LockServer{
		lockManager:  lm,
		fileManager:  fm,
		requestCache: NewRequestCacheWithSize(10*time.Minute, 10000),
		logger:       logger,
		recoveryDone: fm.IsRecoveryComplete(),
		role:         PrimaryRole, // Default to primary role
		isPrimary:    true,        // Default to primary
		serverID:     1,           // Default ID
		metrics:      NewPerformanceMetrics(logger),
	}
	return s
}

// NewReplicatedLockServer initializes a lock server with replication configuration
func NewReplicatedLockServer(role ServerRole, serverID int32, allAddrs map[int32]string, clusterSize int, initialPrimaryID int32) *LockServer {
	return NewReplicatedLockServerWithConfig(role, serverID, allAddrs, clusterSize, initialPrimaryID, DefaultHeartbeatConfig)
}

// NewReplicatedLockServerWithConfig initializes a lock server with replication and custom heartbeat config
func NewReplicatedLockServerWithConfig(role ServerRole, serverID int32, allAddrs map[int32]string, clusterSize int, initialPrimaryID int32, hbConfig HeartbeatConfig) *LockServer {
	logger := log.New(os.Stdout, fmt.Sprintf("[LockServer-%d] ", serverID), log.LstdFlags)

	// Initialize lock manager with lease duration
	lm := lock_manager.NewLockManagerWithLeaseDuration(logger, 30*time.Second)

	// Initialize file manager with lock manager for token validation
	fm := file_manager.NewFileManagerWithWAL(true, true, lm)

	isPrimary := role == PrimaryRole

	s := &LockServer{
		lockManager:        lm,
		fileManager:        fm,
		requestCache:       NewRequestCacheWithSize(10*time.Minute, 10000),
		logger:             logger,
		recoveryDone:       fm.IsRecoveryComplete(),
		role:               role,
		isPrimary:          isPrimary,
		serverID:           serverID,
		allServerAddresses: allAddrs,
		clusterSize:        clusterSize,
		knownPrimaryID:     initialPrimaryID,
		peerClients:        make(map[int32]pb.LockServiceClient),
		peerConns:          make(map[int32]*grpc.ClientConn),
		heartbeatConfig:    hbConfig,
		fencingBuffer:      5 * time.Second, // Default 5s buffer for fencing
		replicationQueue:   make([]*pb.ReplicatedState, 0),
		metrics:            NewPerformanceMetrics(logger),
	}

	// Connect to all peers
	s.connectToPeers()

	// Start heartbeat sender if this is a secondary
	if !isPrimary {
		go s.startHeartbeatSender()
	}

	// Start background replication worker for reliable updates if primary
	if isPrimary {
		go s.startReplicationWorker()
	}

	return s
}

// connectToPeers establishes gRPC connections to all peer servers
func (s *LockServer) connectToPeers() {
	s.logger.Printf("Connecting to all peers...")
	for id, addr := range s.allServerAddresses {
		if id == s.serverID {
			continue // Skip self
		}

		// Close existing connection if any (e.g., on reconnect attempt)
		if conn, ok := s.peerConns[id]; ok && conn != nil {
			conn.Close()
		}

		// Implement a non-blocking connection to peers
		// We don't want to block server startup if peers aren't available yet
		go func(peerID int32, peerAddr string) {
			// Dial options - no blocking so it doesn't hang server startup
			opts := []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				// Don't use WithBlock() - we want non-blocking behavior
				grpc.WithTimeout(5 * time.Second),
			}

			s.logger.Printf("Attempting to connect to peer ID %d at %s", peerID, peerAddr)
			conn, err := grpc.Dial(peerAddr, opts...)

			if err != nil {
				s.logger.Printf("Warning: Failed to connect to peer ID %d at %s: %v",
					peerID, peerAddr, err)

				s.mu.Lock()
				s.peerConns[peerID] = nil
				s.peerClients[peerID] = nil
				s.mu.Unlock()

				return
			}

			// Connection successful
			client := pb.NewLockServiceClient(conn)

			s.mu.Lock()
			s.peerConns[peerID] = conn
			s.peerClients[peerID] = client
			s.mu.Unlock()

			s.logger.Printf("Successfully connected to peer ID %d at %s", peerID, peerAddr)
		}(id, addr)
	}
}

// startHeartbeatSender periodically sends heartbeats to the primary
func (s *LockServer) startHeartbeatSender() {
	ticker := time.NewTicker(s.heartbeatConfig.Interval)
	defer ticker.Stop()

	failureCount := 0
	maxFailures := s.heartbeatConfig.MaxFailureCount

	s.logger.Printf("Starting heartbeat sender with interval=%v, timeout=%v, max failures=%d",
		s.heartbeatConfig.Interval, s.heartbeatConfig.Timeout, maxFailures)

	for range ticker.C {
		// Stop sending heartbeats if we're now the primary
		if s.isPrimary {
			s.logger.Printf("This server is now primary, stopping heartbeat")
			return
		}

		// Skip if we don't know who the primary is
		if s.knownPrimaryID < 1 {
			s.logger.Printf("No known primary to send heartbeat to")
			// TODO: Implement primary discovery or election trigger
			continue
		}

		// Get the primary's client with mutex protection
		s.mu.Lock()
		primaryID := s.knownPrimaryID
		primaryClient := s.peerClients[primaryID]
		s.mu.Unlock()

		if primaryClient == nil {
			s.logger.Printf("Heartbeat: Primary ID %d not connected, trying to reconnect peers...", primaryID)
			s.connectToPeers() // Attempt to reconnect

			// Re-fetch client after attempting reconnect
			s.mu.Lock()
			primaryClient = s.peerClients[s.knownPrimaryID]
			s.mu.Unlock()

			if primaryClient == nil {
				s.logger.Printf("Heartbeat: Still cannot connect to primary ID %d, skipping heartbeat cycle.", s.knownPrimaryID)
				failureCount++ // Increment failure count even if connection failed

				// Check failure count logic here (same as before)
				if failureCount >= maxFailures {
					s.logger.Printf("Primary potentially down after %d attempts (connection/ping failures).", maxFailures)
					// Try to start election
					s.tryStartElection()
					return // Stop this heartbeat loop
				}
				continue // Skip to next tick
			}
		}

		// Send heartbeat
		ctx, cancel := context.WithTimeout(context.Background(), s.heartbeatConfig.Timeout)
		resp, err := primaryClient.Ping(ctx, &pb.HeartbeatRequest{
			ServerId: s.serverID,
		})
		cancel()

		if err != nil {
			failureCount++
			s.logger.Printf("Heartbeat to primary ID %d failed (%d/%d): %v",
				s.knownPrimaryID, failureCount, maxFailures, err)

			// Check if we've reached the failure threshold
			if failureCount >= maxFailures {
				s.logger.Printf("Primary ID %d is unreachable after %d attempts, triggering election",
					s.knownPrimaryID, maxFailures)
				// Set flag to indicate the primary is suspected down
				s.primarySuspected.Store(true)
				// Start election
				s.tryStartElection()
				return // Stop this heartbeat loop
			}
		} else {
			// Reset failure count on success
			if failureCount > 0 {
				s.logger.Printf("Heartbeat to primary ID %d succeeded after %d failures",
					s.knownPrimaryID, failureCount)
				failureCount = 0
			}

			if resp.Status != pb.Status_OK {
				s.logger.Printf("Primary returned non-OK status: %s", resp.ErrorMessage)

				failureCount++
				// Check if we've reached the failure threshold
				if failureCount >= maxFailures {
					s.logger.Printf("Primary ID %d returned errors for %d consecutive heartbeats",
						s.knownPrimaryID, maxFailures)
					s.tryStartElection()
					return // Stop this heartbeat loop
				}
			}
		}
	}
}

// replicateStateToPeers sends the current lock state to all secondary servers
func (s *LockServer) replicateStateToPeers() {
	// Skip if not primary
	if !s.isPrimary {
		return
	}

	// Get current lock state under lock manager's mutex
	s.lockManager.GetMutex().Lock()
	holder := s.lockManager.CurrentHolderNoLock()
	token := s.lockManager.GetCurrentTokenNoLock()
	var expiryTimestamp int64
	expiry := s.lockManager.GetTokenExpirationNoLock()
	if !expiry.IsZero() {
		expiryTimestamp = expiry.Unix()
	}
	s.lockManager.GetMutex().Unlock()

	// Create the state update to send
	stateUpdate := &pb.ReplicatedState{
		LockHolder:      holder,
		LockToken:       token,
		ExpiryTimestamp: expiryTimestamp,
	}

	// Add to queue (if using reliable worker, otherwise skip queue)
	s.enqueueReplication(stateUpdate) // Keep the queue for reliability

	s.logger.Printf("Replicating state (Holder: %d) to all peers", stateUpdate.LockHolder)

	// Track success/failure of replication attempts
	var successCount int32
	var wg sync.WaitGroup

	// Send update to all connected peers
	for peerID, peerClient := range s.peerClients {
		if peerClient == nil {
			s.logger.Printf("Skipping replication to peer %d: not connected", peerID)
			continue
		}

		wg.Add(1)
		// Launch a goroutine for each peer
		go func(targetID int32, client pb.LockServiceClient, update *pb.ReplicatedState) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // Increased timeout for replication
			defer cancel()

			s.logger.Printf("Sending state update to peer %d (LockHolder=%d)", targetID, update.LockHolder)
			resp, err := client.UpdateSecondaryState(ctx, update)

			if err != nil {
				s.logger.Printf("Warning: Failed to replicate state to peer %d: %v", targetID, err)
				// Try to reconnect for future updates
				go func() {
					s.connectToPeers()
				}()
			} else if resp.Status != pb.Status_OK {
				s.logger.Printf("Warning: Peer %d rejected state update: %s", targetID, resp.ErrorMessage)
			} else {
				s.logger.Printf("Successfully replicated state to peer %d", targetID)
				atomic.AddInt32(&successCount, 1)
			}
		}(peerID, peerClient, stateUpdate)
	}

	// Wait for all replication attempts to complete
	wg.Wait()

	if successCount > 0 {
		s.logger.Printf("Replication completed: Successfully replicated to %d peers", successCount)
	} else {
		s.logger.Printf("Warning: Failed to replicate to any peers")
	}
}

// enqueueReplication adds a state update to the replication queue
func (s *LockServer) enqueueReplication(state *pb.ReplicatedState) {
	s.replicationQueueMu.Lock()
	defer s.replicationQueueMu.Unlock()

	// If queue is getting too large, log a warning (possible connectivity issues)
	if len(s.replicationQueue) > 100 {
		s.logger.Printf("WARNING: Replication queue contains %d pending updates", len(s.replicationQueue))
	}

	// For lock state updates, we can optimize by only keeping the latest state
	// Since earlier updates are superseded by the latest one
	if len(s.replicationQueue) > 0 {
		// Replace the last item with the new state (most recent always wins)
		s.replicationQueue[len(s.replicationQueue)-1] = state
	} else {
		// Queue was empty, add the state
		s.replicationQueue = append(s.replicationQueue, state)
	}

	s.logger.Printf("Enqueued state update for replication (queue size: %d)", len(s.replicationQueue))
}

// dequeueProcessedUpdate removes a processed update from the queue
func (s *LockServer) dequeueProcessedUpdate(processed *pb.ReplicatedState) {
	s.replicationQueueMu.Lock()
	defer s.replicationQueueMu.Unlock()

	// For lock state, we can just empty the queue since the latest state was applied
	if len(s.replicationQueue) > 0 {
		// Clear the queue since we successfully applied the latest state
		s.replicationQueue = s.replicationQueue[:0]
		s.logger.Printf("Cleared replication queue after successful update")
	}
}

// getNextReplicationUpdate gets the next update to process
func (s *LockServer) getNextReplicationUpdate() *pb.ReplicatedState {
	s.replicationQueueMu.Lock()
	defer s.replicationQueueMu.Unlock()

	if len(s.replicationQueue) == 0 {
		return nil
	}

	// Return the first item without removing it
	// It will be removed after successful processing
	return s.replicationQueue[0]
}

// startReplicationWorker starts a background worker to process the replication queue
func (s *LockServer) startReplicationWorker() {
	s.logger.Printf("Starting background replication worker")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// Stop if no longer primary
		if !s.isPrimary {
			s.logger.Printf("No longer primary, stopping replication worker")
			return
		}

		// Skip if there's already a replication in progress
		if s.replicationInProgress.Load() {
			continue
		}

		// Set the flag to prevent concurrent replication
		s.replicationInProgress.Store(true)

		// Process the queue
		s.processReplicationQueue()

		// Reset the flag
		s.replicationInProgress.Store(false)
	}
}

// processReplicationQueue processes pending updates in the replication queue
func (s *LockServer) processReplicationQueue() {
	s.logger.Printf("Processing replication queue...")

	// Get the next update to process
	update := s.getNextReplicationUpdate()
	if update == nil {
		s.logger.Printf("No pending updates in queue")
		return
	}

	// Try to replicate to all secondaries
	s.replicateStateToPeers()

	// Mark the replication as complete to allow next worker to run
	s.replicationInProgress.Store(false)
}

// IsRecoveryComplete returns whether WAL recovery is complete
func (s *LockServer) IsRecoveryComplete() bool {
	return s.recoveryDone && s.fileManager.IsRecoveryComplete()
}

// GetRecoveryError returns any error that occurred during recovery
func (s *LockServer) GetRecoveryError() error {
	return s.fileManager.GetRecoveryError()
}

// WaitForRecovery blocks until WAL recovery is complete or timeout is reached
func (s *LockServer) WaitForRecovery(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		if s.IsRecoveryComplete() {
			if err := s.GetRecoveryError(); err != nil {
				return fmt.Errorf("recovery completed with error: %v", err)
			}
			return nil
		}
		<-ticker.C
	}

	return fmt.Errorf("timeout waiting for WAL recovery")
}

// ClientInit handles the client initialization RPC
func (s *LockServer) ClientInit(ctx context.Context, args *pb.ClientInitArgs) (*pb.ClientInitResponse, error) {
	// Allow client initialization even if not primary
	clientID := args.ClientId
	s.logger.Printf("Client %d initialized or reconnected", clientID)

	return &pb.ClientInitResponse{
		Status:       pb.Status_OK,
		ErrorMessage: "",
	}, nil
}

// checkPeerRole verifies if the peer server has been promoted
func (s *LockServer) checkPeerRole() bool {
	// Skip split-brain check if we're not primary
	if !s.isPrimary {
		return false
	}

	// Check if any peer reports itself as primary
	for id, client := range s.peerClients {
		if client == nil {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := client.ServerInfo(ctx, &pb.ServerInfoRequest{})
		cancel()

		if err != nil {
			s.logger.Printf("Unable to check role of peer ID %d: %v", id, err)
			continue
		}

		if resp.Role == "primary" {
			s.logger.Printf("SPLIT-BRAIN DETECTED: Both this server and peer ID %d claim to be primary!", id)
			return true
		}
	}

	// No split-brain detected
	return false
}

// LockAcquire handles lock acquisition requests
func (s *LockServer) LockAcquire(ctx context.Context, args *pb.LockArgs) (*pb.LockResponse, error) {
	// Check for split-brain condition if this server thinks it's primary
	if s.isPrimary && s.checkPeerRole() {
		// We've been demoted to secondary, reject the request
		return &pb.LockResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "This server was demoted to secondary to avoid split-brain",
		}, nil
	}

	// Check if this server is in secondary mode
	if !s.isPrimary && !s.isFencing.Load() {
		return &pb.LockResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "This server is in secondary mode and cannot grant locks",
		}, nil
	}

	// Add detailed logging for fencing check
	isFencingNow := s.isFencing.Load()
	nowTime := time.Now()
	fencingEnds := s.fencingEndTime
	isBeforeEnd := nowTime.Before(fencingEnds)

	s.logger.Printf("DEBUG FENCING CHECK: ClientID=%d RequestID=%s isFencing=%t, Now=%v, End=%v, IsBeforeEnd=%t",
		args.ClientId, args.RequestId, isFencingNow, nowTime, fencingEnds, isBeforeEnd)

	// Check if the server is in fencing period
	if isFencingNow {
		s.logger.Printf("FENCING: Rejecting lock acquisition from client %d during fencing period", args.ClientId)
		return &pb.LockResponse{
			Status:       pb.Status_SERVER_FENCING,
			ErrorMessage: "Server is in fencing period and cannot grant locks",
		}, nil
	}

	s.logger.Printf("DEBUG FENCING CHECK: Proceeding with acquire for client %d", args.ClientId)

	// Add quorum check for primary
	if s.isPrimary {
		liveCount, err := s.checkQuorum()
		if err != nil {
			s.logger.Printf("Quorum lost before processing %s for client %d. Stepping down.", "LockAcquire", args.ClientId)
			s.demoteToReplica()
			return &pb.LockResponse{
				Status:       pb.Status_SECONDARY_MODE,
				ErrorMessage: fmt.Sprintf("Primary lost quorum (%d/%d live nodes), stepping down", liveCount, s.clusterSize),
			}, nil
		}
		// Quorum exists, proceed...
	}

	// Check if request has been processed already
	if cachedResp, exists := s.requestCache.Get(args.RequestId); exists {
		s.logger.Printf("Detected repeated request %s from client %d", args.RequestId, args.ClientId)
		if cachedResp != nil {
			return cachedResp.(*pb.LockResponse), nil
		}
	}

	// Check request validity
	if args.ClientId <= 0 {
		errMsg := "Invalid client ID"
		s.logger.Printf("Lock acquisition failed: %s", errMsg)
		s.metrics.TrackOperationFailure(OpLockAcquire)
		return &pb.LockResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: errMsg,
		}, nil
	}

	// Before acquiring lock, track the queue length for monitoring
	queueLength := s.lockManager.GetQueueLength()
	s.metrics.RecordQueueLength(queueLength)

	// Log current state for debugging
	s.logCurrentLockState("Before lock acquire")

	// Attempt to acquire the lock
	waitStart := time.Now()
	success, token := s.lockManager.Acquire(args.ClientId)
	waitTime := time.Since(waitStart)
	if waitTime > 10*time.Millisecond {
		s.metrics.RecordQueueWaitTime(waitTime)
	}

	if success {
		// Record successful acquisition in metrics
		s.metrics.RecordLockAcquisition(args.ClientId)

		s.logger.Printf("Lock acquired by client %d with token %s", args.ClientId, token)
		// Create response
		resp := &pb.LockResponse{
			Status: pb.Status_OK,
			Token:  token,
		}

		// Cache the response for idempotence
		s.requestCache.Set(args.RequestId, resp)

		// Log peer connection status before replication
		s.logger.Printf("DEBUG: Peer connections before replication: Peer1=%v, Peer2=%v, Peer3=%v",
			s.peerClients[1] != nil,
			s.peerClients[2] != nil,
			s.peerClients[3] != nil)

		// Replicate the state to secondaries
		s.logger.Printf("DEBUG: Starting replication of lock state for client %d to peers", args.ClientId)
		s.replicateStateToPeers()
		s.logger.Printf("DEBUG: Completed replication request for client %d", args.ClientId)

		// Log current state for debugging
		s.logCurrentLockState("After lock acquire")

		return resp, nil
	}

	// Lock acquisition failed
	s.metrics.TrackOperationFailure(OpLockAcquire)
	s.metrics.RecordLockContention()
	s.logger.Printf("Failed to acquire lock for client %d", args.ClientId)

	return &pb.LockResponse{
		Status:       pb.Status_ERROR,
		ErrorMessage: "Lock is currently held by another client",
	}, nil
}

// LockRelease handles the lock release RPC
func (s *LockServer) LockRelease(ctx context.Context, args *pb.LockArgs) (*pb.LockResponse, error) {
	// Check for split-brain condition if this server thinks it's primary
	if s.isPrimary && s.checkPeerRole() {
		// We've been demoted to secondary, reject the request
		return &pb.LockResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "This server was demoted to secondary to avoid split-brain",
			Token:        "",
		}, nil
	}

	// Check if we're in secondary mode
	if !s.isPrimary {
		return &pb.LockResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "Server is in secondary mode and cannot process client requests",
			Token:        "",
		}, nil
	}

	// For LockRelease, we allow operations during fencing period if token is valid
	// This helps clients clean up gracefully

	// Add quorum check for primary
	if s.isPrimary {
		liveCount, err := s.checkQuorum()
		if err != nil {
			s.logger.Printf("Quorum lost before processing %s for client %d. Stepping down.", "LockRelease", args.ClientId)
			s.demoteToReplica()
			return &pb.LockResponse{
				Status:       pb.Status_SECONDARY_MODE,
				ErrorMessage: fmt.Sprintf("Primary lost quorum (%d/%d live nodes), stepping down", liveCount, s.clusterSize),
				Token:        "",
			}, nil
		}
		// Quorum exists, proceed...
	}

	clientID := args.ClientId
	token := args.Token
	requestID := args.RequestId

	s.logger.Printf("Client %d attempting to release lock with token %s (request: %s)", clientID, token, requestID)

	// Check cache for duplicate request
	if cachedResp, exists := s.requestCache.Get(requestID); exists {
		s.logger.Printf("Found cached response for request %s", requestID)
		return cachedResp.(*pb.LockResponse), nil
	}

	// Mark request as in progress
	if !s.requestCache.MarkInProgress(requestID) {
		s.logger.Printf("Request %s already in progress", requestID)
		return &pb.LockResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: "Request already in progress",
		}, nil
	}

	// Validate token and check lock ownership
	if !s.lockManager.HasLockWithToken(clientID, token) {
		s.logger.Printf("Lock release failed: client %d doesn't hold the lock with token %s", clientID, token)
		resp := &pb.LockResponse{
			Status:       pb.Status_INVALID_TOKEN,
			ErrorMessage: "Invalid token or lock not held",
			Token:        "",
		}
		s.requestCache.Set(requestID, resp)
		return resp, nil
	}

	success := s.lockManager.Release(clientID, token)

	var resp *pb.LockResponse
	if success {
		s.logger.Printf("Lock released successfully by client %d", clientID)
		resp = &pb.LockResponse{
			Status:       pb.Status_OK,
			ErrorMessage: "",
			Token:        "",
		}

		// Replicate state to secondary
		s.replicateStateToPeers()
	} else {
		s.logger.Printf("Lock release failed for client %d: permission denied", clientID)
		resp = &pb.LockResponse{
			Status:       pb.Status_PERMISSION_DENIED,
			ErrorMessage: "Permission denied",
			Token:        "",
		}
	}

	s.requestCache.Set(requestID, resp)
	return resp, nil
}

// FileAppend handles file append requests
func (s *LockServer) FileAppend(ctx context.Context, args *pb.FileArgs) (*pb.FileResponse, error) {
	// Check for split-brain condition if this server thinks it's primary
	if s.isPrimary && s.checkPeerRole() {
		// We've been demoted to secondary, reject the request
		return &pb.FileResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "This server was demoted to secondary to avoid split-brain",
		}, nil
	}

	// Check if we're in secondary mode
	if !s.isPrimary {
		return &pb.FileResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "Server is in secondary mode and cannot process client requests",
		}, nil
	}

	// Check if the server is in fencing period
	if s.isFencing.Load() {
		s.logger.Printf("FENCING: Rejecting file append from client %d during fencing period", args.ClientId)
		return &pb.FileResponse{
			Status:       pb.Status_SERVER_FENCING,
			ErrorMessage: "Server is in fencing period and cannot process file operations",
		}, nil
	}

	// Add quorum check for primary
	if s.isPrimary {
		liveCount, err := s.checkQuorum()
		if err != nil {
			s.logger.Printf("Quorum lost before processing %s for client %d. Stepping down.", "FileAppend", args.ClientId)
			s.demoteToReplica()
			return &pb.FileResponse{
				Status:       pb.Status_SECONDARY_MODE,
				ErrorMessage: fmt.Sprintf("Primary lost quorum (%d/%d live nodes), stepping down", liveCount, s.clusterSize),
			}, nil
		}
		// Quorum exists, proceed...
	}

	// Check if request has been processed already
	if cachedResp, exists := s.requestCache.Get(args.RequestId); exists {
		s.logger.Printf("Found cached response for request %s", args.RequestId)
		return cachedResp.(*pb.FileResponse), nil
	}

	// Mark request as in progress
	if !s.requestCache.MarkInProgress(args.RequestId) {
		s.logger.Printf("Request %s already in progress", args.RequestId)
		return &pb.FileResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: "Request already in progress",
		}, nil
	}

	// Validate token and check lock ownership
	if !s.lockManager.HasLockWithToken(args.ClientId, args.Token) {
		s.logger.Printf("FileAppend rejected: client %d doesn't hold lock with token %s", args.ClientId, args.Token)
		resp := &pb.FileResponse{
			Status:       pb.Status_INVALID_TOKEN,
			ErrorMessage: "Invalid token or lock not held",
		}
		s.requestCache.Set(args.RequestId, resp)
		return resp, nil
	}

	// Process the append operation
	err := s.fileManager.AppendToFile(args.Filename, args.Content, args.ClientId, args.Token)
	if err != nil {
		s.logger.Printf("FileAppend error: %v", err)
		resp := &pb.FileResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: err.Error(),
		}
		s.requestCache.Set(args.RequestId, resp)
		return resp, nil
	}

	s.logger.Printf("FileAppend successful for client %d", args.ClientId)
	resp := &pb.FileResponse{
		Status: pb.Status_OK,
	}
	s.requestCache.Set(args.RequestId, resp)
	return resp, nil
}

// RenewLease handles lease renewal RPC
func (s *LockServer) RenewLease(ctx context.Context, args *pb.LeaseArgs) (*pb.LeaseResponse, error) {
	// Check for split-brain condition if this server thinks it's primary
	if s.isPrimary && s.checkPeerRole() {
		// We've been demoted to secondary, reject the request
		return &pb.LeaseResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "This server was demoted to secondary to avoid split-brain",
		}, nil
	}

	// Check if we're in secondary mode
	if !s.isPrimary {
		return &pb.LeaseResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "Server is in secondary mode and cannot process client requests",
		}, nil
	}

	// For RenewLease, we allow operations during fencing period if token is valid
	// This helps clients with valid leases maintain their leases during failover

	// Add quorum check for primary
	if s.isPrimary {
		liveCount, err := s.checkQuorum()
		if err != nil {
			s.logger.Printf("Quorum lost before processing %s for client %d. Stepping down.", "RenewLease", args.ClientId)
			s.demoteToReplica()
			return &pb.LeaseResponse{
				Status:       pb.Status_SECONDARY_MODE,
				ErrorMessage: fmt.Sprintf("Primary lost quorum (%d/%d live nodes), stepping down", liveCount, s.clusterSize),
			}, nil
		}
		// Quorum exists, proceed...
	}

	clientID := args.ClientId
	token := args.Token
	requestID := args.RequestId

	s.logger.Printf("Client %d attempting to renew lease with token %s (request: %s)", clientID, token, requestID)

	// Check cache for duplicate request
	if cachedResp, exists := s.requestCache.Get(requestID); exists {
		s.logger.Printf("Found cached response for request %s", requestID)
		return cachedResp.(*pb.LeaseResponse), nil
	}

	// Mark request as in progress
	if !s.requestCache.MarkInProgress(requestID) {
		s.logger.Printf("Request %s already in progress", requestID)
		return &pb.LeaseResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: "Request already in progress",
		}, nil
	}

	// Validate token and renew lease
	success := s.lockManager.RenewLease(clientID, token)

	var resp *pb.LeaseResponse
	if success {
		s.logger.Printf("Lease renewed for client %d", clientID)
		resp = &pb.LeaseResponse{
			Status:       pb.Status_OK,
			ErrorMessage: "",
		}

		// Replicate state to secondary after lease renewal
		s.replicateStateToPeers()
	} else {
		s.logger.Printf("Lease renewal failed for client %d: invalid token or not lock holder", clientID)
		resp = &pb.LeaseResponse{
			Status:       pb.Status_INVALID_TOKEN,
			ErrorMessage: "Invalid token or not lock holder",
		}
	}

	s.requestCache.Set(requestID, resp)
	return resp, nil
}

// UpdateSecondaryState handles replication from primary to secondary
func (s *LockServer) UpdateSecondaryState(ctx context.Context, state *pb.ReplicatedState) (*pb.ReplicationResponse, error) {
	s.mu.Lock() // Lock needed
	isCurrentlyPrimary := s.isPrimary
	s.mu.Unlock()

	if isCurrentlyPrimary { // Check if this server is primary
		s.logger.Printf("Warning: Primary server ID %d received UpdateSecondaryState call - ignoring", s.serverID)
		return &pb.ReplicationResponse{Status: pb.Status_ERROR, ErrorMessage: "Server is primary"}, nil
	}

	s.logger.Printf("Received state update: holder=%d, expiry=%d",
		state.LockHolder, state.ExpiryTimestamp)

	// Apply the replicated state to our lock manager
	err := s.lockManager.ApplyReplicatedState(
		state.LockHolder,
		state.LockToken,
		state.ExpiryTimestamp,
	)

	if err != nil {
		s.logger.Printf("Error applying replicated state: %v", err)
		return &pb.ReplicationResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: err.Error(),
		}, nil
	}

	// More detailed logging of success
	s.logger.Printf("Successfully applied replicated state: holder=%d, token length=%d, expiry timestamp=%d",
		state.LockHolder, len(state.LockToken), state.ExpiryTimestamp)

	return &pb.ReplicationResponse{
		Status: pb.Status_OK,
	}, nil
}

// Ping handles heartbeat requests from secondary
func (s *LockServer) Ping(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// Update timestamp to indicate liveness
	s.logger.Printf("Received heartbeat from server ID %d", req.ServerId)

	// Always return OK to confirm liveness
	return &pb.HeartbeatResponse{
		Status:       pb.Status_OK,
		ErrorMessage: "",
	}, nil
}

// ServerInfo returns information about this server instance
func (s *LockServer) ServerInfo(ctx context.Context, req *pb.ServerInfoRequest) (*pb.ServerInfoResponse, error) {
	s.logger.Printf("Received ServerInfo request")

	// Return the accurate role - "primary" if this server is currently primary, otherwise "secondary"
	role := "secondary"
	if s.isPrimary {
		role = "primary"
	}

	return &pb.ServerInfoResponse{
		ServerId: s.serverID,
		Role:     role,
	}, nil
}

// ClientClose handles the client close RPC
func (s *LockServer) ClientClose(ctx context.Context, args *pb.ClientInitArgs) (*pb.ClientInitResponse, error) {
	clientID := args.ClientId
	s.logger.Printf("Client %d closing connection", clientID)

	// If this client holds the lock, release it
	s.lockManager.ReleaseLockIfHeld(clientID)

	// Return success response
	return &pb.ClientInitResponse{
		Status:       pb.Status_OK,
		ErrorMessage: "Disconnected!",
	}, nil
}

// CreateFiles ensures the 100 files exist - now delegates to file manager
func CreateFiles() {
	// Create a temporary lock manager for file creation
	lm := lock_manager.NewLockManagerWithLeaseDuration(log.New(os.Stdout, "[FileCreation] ", log.LstdFlags), 30*time.Second)
	fm := file_manager.NewFileManager(false, lm)
	fm.CreateFiles()
}

// Cleanup closes any open files and performs other cleanup tasks
func (s *LockServer) Cleanup() {
	s.fileManager.Cleanup()
	s.logger.Println("Server cleanup complete")
}

// VerifyUniqueServerID checks for duplicate server IDs with retry
func (s *LockServer) VerifyUniqueServerID() error {
	// Verify that our server ID is unique among all peers
	if s.serverID <= 0 {
		return fmt.Errorf("invalid server ID: %d", s.serverID)
	}

	// Check with all peers
	for id, client := range s.peerClients {
		if client == nil {
			s.logger.Printf("Warning: Cannot verify uniqueness with peer ID %d (not connected)", id)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		info, err := client.ServerInfo(ctx, &pb.ServerInfoRequest{})
		cancel()

		if err != nil {
			s.logger.Printf("Warning: Failed to get server info from peer ID %d: %v", id, err)
			continue
		}

		if info.ServerId == s.serverID {
			return fmt.Errorf("duplicate server ID %d detected (peer at address %s)",
				s.serverID, s.allServerAddresses[id])
		}
	}

	return nil
}

// VerifySharedFilesystem checks if the shared filesystem is properly accessible with retry
func (s *LockServer) VerifySharedFilesystem() error {
	// Create a test file with unique content
	testFile := fmt.Sprintf("./data/verify_fs_%d.tmp", s.serverID)
	content := fmt.Sprintf("Server ID %d filesystem verification at %s",
		s.serverID, time.Now().Format(time.RFC3339))

	// Write the test file
	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write test file: %v", err)
	}
	defer os.Remove(testFile) // Clean up after test

	// Have peers verify they can access and read the file
	for id, client := range s.peerClients {
		if client == nil {
			s.logger.Printf("Warning: Cannot verify shared filesystem with peer ID %d (not connected)", id)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.VerifyFileAccess(ctx, &pb.FileAccessRequest{
			FilePath: testFile,
		})
		cancel()

		if err != nil {
			return fmt.Errorf("failed to request file verification from peer ID %d: %v", id, err)
		}

		if resp.Status != pb.Status_OK {
			return fmt.Errorf("peer ID %d failed to access test file: %s", id, resp.ErrorMessage)
		}

		// Check content using the ActualContent field
		if resp.ActualContent != content {
			return fmt.Errorf("peer ID %d read incorrect content: expected '%s', got '%s'",
				id, content, resp.ActualContent)
		}

		s.logger.Printf("Peer ID %d successfully verified file access", id)
	}

	return nil
}

// VerifyFileAccess implements the RPC to check file access from a peer
func (s *LockServer) VerifyFileAccess(ctx context.Context, req *pb.FileAccessRequest) (*pb.FileAccessResponse, error) {
	// Check if the file exists and can be read
	content, err := os.ReadFile(req.FilePath)
	if err != nil {
		s.logger.Printf("File access verification failed: %v", err)
		return &pb.FileAccessResponse{
			Status:        pb.Status_FILESYSTEM_ERROR,
			ErrorMessage:  fmt.Sprintf("Failed to read file: %v", err),
			ActualContent: "",
		}, nil
	}

	s.logger.Printf("Successfully read file %s for verification", req.FilePath)

	return &pb.FileAccessResponse{
		Status:        pb.Status_OK,
		ActualContent: string(content),
	}, nil
}

// GetMetrics returns the server's performance metrics tracker
func (s *LockServer) GetMetrics() *PerformanceMetrics {
	return s.metrics
}

// startSplitBrainChecker periodically checks if the peer has been promoted
func (s *LockServer) startSplitBrainChecker() {
	// With multiple peers, split-brain detection happens in the Ping handler
	// by checking if another server claiming to be primary sends a heartbeat
	// We can also add periodic quorum checks here in the future
	s.logger.Printf("Split-brain monitoring initialized")
}

// logCurrentLockState logs the current lock state for debugging purposes
func (s *LockServer) logCurrentLockState(context string) {
	holder := s.lockManager.CurrentHolder()
	expiry := s.lockManager.GetTokenExpiration()

	var expiryStr string
	if expiry.IsZero() {
		expiryStr = "not set"
	} else {
		expiryStr = expiry.Format(time.RFC3339)
	}

	s.logger.Printf("Lock state (%s): holder=%d, expiry=%s",
		context, holder, expiryStr)
}

// SetFencingBuffer sets the buffer time to add to lease duration for fencing
func (s *LockServer) SetFencingBuffer(buffer time.Duration) {
	s.fencingBuffer = buffer
	s.logger.Printf("Fencing buffer set to %v", buffer)
}

// ReconnectPeers attempts to reconnect to any disconnected peers
func (s *LockServer) ReconnectPeers() {
	s.logger.Printf("Explicitly reconnecting to peers...")
	s.connectToPeers()
}

// StartHTTPMonitoring starts an HTTP server for monitoring and control
func (s *LockServer) StartHTTPMonitoring(port int) {
	addr := fmt.Sprintf(":%d", port)
	s.logger.Printf("Starting HTTP monitoring server on %s", addr)

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		// Return basic status information
		fmt.Fprintf(w, "Server ID: %d\nRole: %s\nPrimary: %t\n",
			s.serverID, s.role, s.isPrimary)
	})

	http.HandleFunc("/reconnect", func(w http.ResponseWriter, r *http.Request) {
		s.logger.Printf("Received request to reconnect to peers")
		s.ReconnectPeers()
		fmt.Fprintf(w, "Reconnection attempt initiated\n")
	})

	// Run HTTP server in a goroutine
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			s.logger.Printf("HTTP monitoring server error: %v", err)
		}
	}()
}
