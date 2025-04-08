package server

import (
	"context"
	"fmt"
	"log"
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
	role        ServerRole           // Current role (primary or secondary)
	isPrimary   bool                 // Is this server the primary?
	serverID    int32                // ID of this server
	peerAddress string               // Address of the peer server
	peerClient  pb.LockServiceClient // gRPC client for peer communication
	peerConn    *grpc.ClientConn     // gRPC connection to peer

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
func NewReplicatedLockServer(role ServerRole, serverID int32, peerAddress string) *LockServer {
	return NewReplicatedLockServerWithConfig(role, serverID, peerAddress, DefaultHeartbeatConfig)
}

// NewReplicatedLockServerWithConfig initializes a lock server with replication and custom heartbeat config
func NewReplicatedLockServerWithConfig(role ServerRole, serverID int32, peerAddress string, hbConfig HeartbeatConfig) *LockServer {
	logger := log.New(os.Stdout, fmt.Sprintf("[LockServer-%d] ", serverID), log.LstdFlags)

	// Initialize lock manager with lease duration
	lm := lock_manager.NewLockManagerWithLeaseDuration(logger, 30*time.Second)

	// Initialize file manager with lock manager for token validation
	fm := file_manager.NewFileManagerWithWAL(true, true, lm)

	isPrimary := role == PrimaryRole

	s := &LockServer{
		lockManager:      lm,
		fileManager:      fm,
		requestCache:     NewRequestCacheWithSize(10*time.Minute, 10000),
		logger:           logger,
		recoveryDone:     fm.IsRecoveryComplete(),
		role:             role,
		isPrimary:        isPrimary,
		serverID:         serverID,
		peerAddress:      peerAddress,
		heartbeatConfig:  hbConfig,
		fencingBuffer:    5 * time.Second, // Default 5s buffer for fencing
		replicationQueue: make([]*pb.ReplicatedState, 0),
		metrics:          NewPerformanceMetrics(logger),
	}

	// Connect to peer if address is provided
	if peerAddress != "" {
		if err := s.connectToPeer(); err != nil {
			s.logger.Printf("Warning: Failed to connect to peer at %s: %v", peerAddress, err)
		}
	}

	// Start heartbeat sender if this is a secondary
	if !isPrimary && peerAddress != "" {
		go s.startHeartbeatSender()
	}

	// Start background replication worker for reliable updates
	if isPrimary && peerAddress != "" {
		go s.startReplicationWorker()
	}

	return s
}

// connectToPeer establishes a gRPC connection to the peer server
func (s *LockServer) connectToPeer() error {
	if s.peerAddress == "" {
		return fmt.Errorf("no peer address configured")
	}

	// Create gRPC connection
	conn, err := grpc.Dial(s.peerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to peer: %w", err)
	}

	// Create client
	client := pb.NewLockServiceClient(conn)

	s.peerConn = conn
	s.peerClient = client
	s.logger.Printf("Connected to peer at %s", s.peerAddress)

	return nil
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

		// Send heartbeat
		ctx, cancel := context.WithTimeout(context.Background(), s.heartbeatConfig.Timeout)
		resp, err := s.peerClient.Ping(ctx, &pb.HeartbeatRequest{
			ServerId: s.serverID,
		})
		cancel()

		if err != nil {
			failureCount++
			s.logger.Printf("Heartbeat failed (%d/%d): %v",
				failureCount, maxFailures, err)

			// Check if we've reached the failure threshold
			if failureCount >= maxFailures {
				s.logger.Printf("Primary is unreachable after %d attempts, promoting to primary",
					maxFailures)
				s.promoteToPrimary()
				return
			}
		} else {
			// Reset failure count on success
			if failureCount > 0 {
				s.logger.Printf("Heartbeat succeeded after %d failures", failureCount)
				failureCount = 0
			}

			if resp.Status != pb.Status_OK {
				s.logger.Printf("Primary returned non-OK status: %s", resp.ErrorMessage)
			}
		}
	}
}

// promoteToPrimary promotes this server from secondary to primary
func (s *LockServer) promoteToPrimary() {
	s.logger.Printf("Promoting server %d from secondary to primary", s.serverID)

	s.isPrimary = true
	s.role = PrimaryRole

	// Start fencing period
	leaseDuration := s.lockManager.GetLeaseDuration()
	fencingDuration := leaseDuration + s.fencingBuffer
	s.fencingEndTime = time.Now().Add(fencingDuration)
	s.isFencing.Store(true)

	s.logger.Printf("Entering fencing period for %v (until %v)",
		fencingDuration, s.fencingEndTime)

	// Attempt to log the current lock state before fencing
	s.logCurrentLockState("Before fencing")

	// Start a goroutine to end the fencing period
	go s.waitForFencingEnd()
}

// waitForFencingEnd waits for the fencing period to end
func (s *LockServer) waitForFencingEnd() {
	timeToWait := time.Until(s.fencingEndTime)
	if timeToWait <= 0 {
		timeToWait = 1 * time.Millisecond // Minimum wait
	}

	s.logger.Printf("Scheduling fencing end in %v", timeToWait)
	time.Sleep(timeToWait)

	s.logger.Printf("Fencing period ended, clearing lock state")
	s.isFencing.Store(false)

	// Log the lock state before clearing
	s.logCurrentLockState("Before forced clear")

	// Clear lock state to ensure safe operation
	if err := s.lockManager.ForceClearLockState(); err != nil {
		s.logger.Printf("Warning: Failed to clear lock state after fencing: %v", err)
	}

	// Log the lock state after clearing
	s.logCurrentLockState("After forced clear")

	s.logger.Printf("Server is now fully operational as primary")
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

// replicateStateToSecondary sends the current lock state to the secondary server
func (s *LockServer) replicateStateToSecondary() {
	// Skip if not primary or no peer connection
	if !s.isPrimary || s.peerClient == nil {
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

	// Add the update to the replication queue first to ensure reliability
	s.enqueueReplication(stateUpdate)

	// Send update asynchronously to avoid blocking client operations
	go func() {
		// Configuration for retries
		maxRetries := 3
		initialBackoff := 500 * time.Millisecond
		backoff := initialBackoff

		// Attempt to replicate with retries
		for attempt := 0; attempt < maxRetries; attempt++ {
			// Create a new context for each attempt
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

			// Send the replication request
			_, err := s.peerClient.UpdateSecondaryState(ctx, stateUpdate)
			cancel() // Cancel the context immediately after the call

			// If successful, we're done
			if err == nil {
				if attempt > 0 {
					s.logger.Printf("Successfully replicated state to secondary after %d retries", attempt)
				}

				// On success, remove this update from the queue if it matches
				s.dequeueProcessedUpdate(stateUpdate)
				return
			}

			// Log the error and prepare for retry
			s.logger.Printf("Failed to replicate state to secondary (attempt %d/%d): %v",
				attempt+1, maxRetries, err)

			// If this was the last attempt, give up
			if attempt == maxRetries-1 {
				s.logger.Printf("Immediate replication failed after %d attempts - relying on background worker", maxRetries)
				break
			}

			// Wait before retrying with exponential backoff
			time.Sleep(backoff)
			backoff *= 2 // Exponential backoff
		}

		// At this point, all retries have failed
		// The update is already in the queue, so the background worker will try again later

		// Try to reconnect to the peer for future replication attempts
		if err := s.connectToPeer(); err != nil {
			s.logger.Printf("Failed to reconnect to peer after replication failures: %v", err)
		}
	}()
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
	// Set the flag to indicate that a worker is running
	if !s.replicationInProgress.CompareAndSwap(false, true) {
		s.logger.Printf("Replication worker already running, not starting another one")
		return
	}

	s.logger.Printf("Starting background replication worker")

	go func() {
		defer s.replicationInProgress.Store(false)

		// Process the queue periodically
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			// Stop if we're no longer primary
			if !s.isPrimary {
				s.logger.Printf("No longer primary, stopping replication worker")
				return
			}

			select {
			case <-ticker.C:
				s.processReplicationQueue()
			}
		}
	}()
}

// processReplicationQueue processes pending updates in the replication queue
func (s *LockServer) processReplicationQueue() {
	// Skip if not primary or no peer connection
	if !s.isPrimary || s.peerClient == nil {
		return
	}

	// Get the next update to process
	update := s.getNextReplicationUpdate()
	if update == nil {
		// Queue is empty, nothing to do
		return
	}

	s.logger.Printf("Processing pending replication update from queue")

	// Try to send the update
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := s.peerClient.UpdateSecondaryState(ctx, update)
	if err != nil {
		s.logger.Printf("Background replication failed: %v - will retry later", err)
		return
	}

	// Success, remove the update from the queue
	s.dequeueProcessedUpdate(update)
	s.logger.Printf("Successfully processed pending replication update")
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

// LockAcquire handles requests to acquire a lock
func (s *LockServer) LockAcquire(ctx context.Context, args *pb.LockArgs) (*pb.LockResponse, error) {
	startTime := time.Now()
	s.metrics.IncrementConcurrentRequests()
	defer s.metrics.DecrementConcurrentRequests()
	defer s.metrics.TrackOperationLatency(OpLockAcquire, time.Since(startTime))

	// Check if request has been processed already
	if cachedResp, exists := s.requestCache.Get(args.RequestId); exists {
		s.logger.Printf("Detected repeated request %s from client %d", args.RequestId, args.ClientId)
		if cachedResp != nil {
			return cachedResp.(*pb.LockResponse), nil
		}
	}

	// Check if the server is in secondary mode
	if !s.isPrimary {
		s.logger.Printf("Cannot acquire lock - server is in secondary mode")
		return &pb.LockResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "This server is in secondary mode, try the primary",
		}, nil
	}

	// Check if the server is in fencing period
	if s.isFencing.Load() && time.Now().Before(s.fencingEndTime) {
		s.logger.Printf("Cannot acquire lock - server is in fencing period")
		return &pb.LockResponse{
			Status:       pb.Status_SERVER_FENCING,
			ErrorMessage: "Server is in fencing period, try again later",
		}, nil
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

		// Replicate the state to the secondary
		if s.peerClient != nil {
			s.replicateStateToSecondary()
		}

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
		s.replicateStateToSecondary()
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

// FileAppend handles the file append RPC
func (s *LockServer) FileAppend(ctx context.Context, args *pb.FileArgs) (*pb.FileResponse, error) {
	// Check if we're in secondary mode
	if !s.isPrimary {
		return &pb.FileResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "Server is in secondary mode and cannot process client requests",
		}, nil
	}

	// Check if we're in fencing period - reject file operations during fencing
	if s.isFencing.Load() {
		return &pb.FileResponse{
			Status:       pb.Status_SERVER_FENCING,
			ErrorMessage: "Server is in fencing period after failover",
		}, nil
	}

	clientID := args.ClientId
	token := args.Token
	requestID := args.RequestId

	s.logger.Printf("Client %d attempting to append to file with token %s (request: %s)", clientID, token, requestID)

	// Check cache for duplicate request
	if cachedResp, exists := s.requestCache.Get(requestID); exists {
		s.logger.Printf("Found cached response for request %s", requestID)
		return cachedResp.(*pb.FileResponse), nil
	}

	// Mark request as in progress
	if !s.requestCache.MarkInProgress(requestID) {
		s.logger.Printf("Request %s already in progress", requestID)
		return &pb.FileResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: "Request already in progress",
		}, nil
	}

	// Validate token and check lock ownership
	if !s.lockManager.HasLockWithToken(clientID, token) {
		s.logger.Printf("File append failed: client %d doesn't hold the lock with token %s", clientID, token)
		resp := &pb.FileResponse{
			Status:       pb.Status_INVALID_TOKEN,
			ErrorMessage: "Invalid token or lock not held",
		}
		s.requestCache.Set(requestID, resp)
		return resp, nil
	}

	// Process the file append
	err := s.fileManager.AppendToFile(args.Filename, args.Content, clientID, token)

	var resp *pb.FileResponse
	if err != nil {
		s.logger.Printf("File append error: %v", err)
		resp = &pb.FileResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: err.Error(),
		}
	} else {
		s.logger.Printf("File append successful for client %d", clientID)
		resp = &pb.FileResponse{
			Status:       pb.Status_OK,
			ErrorMessage: "",
		}
	}

	s.requestCache.Set(requestID, resp)
	return resp, nil
}

// RenewLease handles lease renewal RPC
func (s *LockServer) RenewLease(ctx context.Context, args *pb.LeaseArgs) (*pb.LeaseResponse, error) {
	// Check if we're in secondary mode
	if !s.isPrimary {
		return &pb.LeaseResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "Server is in secondary mode and cannot process client requests",
		}, nil
	}

	// For RenewLease, we allow operations during fencing period if token is valid
	// This helps clients with valid leases maintain their leases during failover

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
		s.replicateStateToSecondary()
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
	// This should only be called on the secondary
	if s.isPrimary {
		s.logger.Printf("Warning: Primary received UpdateSecondaryState call")
		return &pb.ReplicationResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: "Cannot update state on primary server",
		}, nil
	}

	s.logger.Printf("Received state update from primary: holder=%d, expiry=%v",
		state.LockHolder, time.Unix(state.ExpiryTimestamp, 0))

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

	return &pb.ReplicationResponse{
		Status:       pb.Status_OK,
		ErrorMessage: "",
	}, nil
}

// Ping handles heartbeat requests from secondary
func (s *LockServer) Ping(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// This should only be called on the primary
	if !s.isPrimary {
		s.logger.Printf("Warning: Secondary received Ping call from server %d", req.ServerId)
		return &pb.HeartbeatResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: "This server is not primary",
		}, nil
	}

	// Log heartbeat with reduced frequency to avoid flooding logs
	if time.Now().Second()%10 == 0 { // Log only every ~10 seconds
		s.logger.Printf("Received heartbeat from secondary server %d", req.ServerId)
	}

	return &pb.HeartbeatResponse{
		Status:       pb.Status_OK,
		ErrorMessage: "",
	}, nil
}

// ServerInfo returns information about this server instance
func (s *LockServer) ServerInfo(ctx context.Context, req *pb.ServerInfoRequest) (*pb.ServerInfoResponse, error) {
	s.logger.Printf("Received ServerInfo request")

	role := "primary"
	if !s.isPrimary {
		role = "secondary"
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
	if s.peerAddress == "" {
		// No peer to check against
		return nil
	}

	// Retry a few times since the peer might still be starting up
	maxRetries := 5
	retryDelay := 1 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Ensure we have a connection to the peer
		if s.peerClient == nil {
			if err := s.connectToPeer(); err != nil {
				lastErr = fmt.Errorf("failed to connect to peer for ID verification: %w", err)
				s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
				time.Sleep(retryDelay)
				continue
			}
		}

		// Use ServerInfo RPC to check peer's ID
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := s.peerClient.ServerInfo(ctx, &pb.ServerInfoRequest{})
		cancel()

		if err != nil {
			lastErr = fmt.Errorf("unable to verify peer server ID: %w", err)
			s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
			time.Sleep(retryDelay)
			continue
		}

		// Successfully got peer info - check for duplicate ID
		if resp.ServerId == s.serverID {
			errMsg := fmt.Sprintf("Duplicate server ID detected! This server and peer both have ID %d", s.serverID)
			s.logger.Printf("ERROR: %s", errMsg)
			return fmt.Errorf(errMsg)
		}

		// Success!
		s.logger.Printf("Verified unique server ID: local=%d, peer=%d", s.serverID, resp.ServerId)
		return nil
	}

	// If we reach here, we failed after all retries
	s.logger.Printf("WARNING: Could not verify server ID uniqueness after %d attempts: %v", maxRetries, lastErr)
	s.logger.Printf("Continuing startup, but be aware that duplicate IDs may cause issues")
	return nil // Allow startup to continue despite verification failure
}

// VerifySharedFilesystem checks if the shared filesystem is properly accessible with retry
func (s *LockServer) VerifySharedFilesystem() error {
	if s.peerAddress == "" {
		// No peer to verify with
		return nil
	}

	// Retry a few times since the peer might still be starting up
	maxRetries := 5
	retryDelay := 1 * time.Second

	// Try to validate direct filesystem access first
	lockStatePath := "./data/lock_state.json"
	testContent := []byte(fmt.Sprintf("Filesystem test from server %d at %s\n",
		s.serverID, time.Now().Format(time.RFC3339)))

	// Test if we can write to the path
	if err := os.WriteFile(lockStatePath+".test", testContent, 0644); err != nil {
		return fmt.Errorf("cannot write to data directory: %w", err)
	}
	defer os.Remove(lockStatePath + ".test")

	// Verify through peer with retries
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Ensure we have a connection to the peer
		if s.peerClient == nil {
			if err := s.connectToPeer(); err != nil {
				lastErr = fmt.Errorf("failed to connect to peer for filesystem verification: %w", err)
				s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
				time.Sleep(retryDelay)
				continue
			}
		}

		// Create a test file with random content
		testFileName := fmt.Sprintf("./data/fs_verify_%d.tmp", time.Now().UnixNano())
		testContent := fmt.Sprintf("Filesystem verification from server %d at %s",
			s.serverID, time.Now().Format(time.RFC3339))

		// Write test content to file
		if err := os.WriteFile(testFileName, []byte(testContent), 0644); err != nil {
			return fmt.Errorf("failed to write test file for filesystem verification: %w", err)
		}
		defer os.Remove(testFileName) // Clean up the test file when done

		// Ask peer to read the file
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := s.peerClient.VerifyFileAccess(ctx, &pb.FileAccessRequest{
			FilePath:        testFileName,
			ExpectedContent: testContent,
		})
		cancel()

		if err != nil {
			lastErr = fmt.Errorf("filesystem verification failed - error calling peer: %w", err)
			s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
			time.Sleep(retryDelay)
			continue
		}

		if resp.Status != pb.Status_OK {
			lastErr = fmt.Errorf("filesystem verification failed: %s", resp.ErrorMessage)
			s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
			time.Sleep(retryDelay)
			continue
		}

		if resp.ActualContent != testContent {
			lastErr = fmt.Errorf("filesystem verification failed - content mismatch: expected '%s', got '%s'",
				testContent, resp.ActualContent)
			s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
			time.Sleep(retryDelay)
			continue
		}

		// Success!
		s.logger.Printf("Shared filesystem verification successful")
		return nil
	}

	// If we reach here, we failed after all retries
	s.logger.Printf("WARNING: Could not verify shared filesystem after %d attempts: %v", maxRetries, lastErr)
	s.logger.Printf("Continuing startup, but be aware that filesystem sharing may not be working correctly")
	return nil // Allow startup to continue despite verification failure
}

// VerifyFileAccess implements the RPC to check file access from a peer
func (s *LockServer) VerifyFileAccess(ctx context.Context, req *pb.FileAccessRequest) (*pb.FileAccessResponse, error) {
	s.logger.Printf("Received filesystem verification request for file: %s", req.FilePath)

	// Attempt to read the specified file
	content, err := os.ReadFile(req.FilePath)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to read file %s: %v", req.FilePath, err)
		s.logger.Printf("Filesystem verification failed: %s", errMsg)
		return &pb.FileAccessResponse{
			Status:       pb.Status_FILESYSTEM_ERROR,
			ErrorMessage: errMsg,
		}, nil
	}

	actualContent := string(content)

	// Check if content matches what's expected
	if actualContent != req.ExpectedContent {
		errMsg := fmt.Sprintf("Content mismatch in file %s", req.FilePath)
		s.logger.Printf("Filesystem verification failed: %s", errMsg)
		return &pb.FileAccessResponse{
			Status:        pb.Status_FILESYSTEM_ERROR,
			ErrorMessage:  errMsg,
			ActualContent: actualContent,
		}, nil
	}

	s.logger.Printf("Filesystem verification successful for file: %s", req.FilePath)
	return &pb.FileAccessResponse{
		Status:        pb.Status_OK,
		ActualContent: actualContent,
	}, nil
}
