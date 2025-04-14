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
)

// ServerRole defines the role of the server (legacy)
type ServerRole string

const (
	// PrimaryRole indicates a primary server that accepts client requests
	PrimaryRole ServerRole = "primary"
	// SecondaryRole indicates a secondary server that replicates from primary
	SecondaryRole ServerRole = "secondary"
)

// ServerState defines the state of the server in the cluster
type ServerState string

const (
	// LeaderState indicates a leader server that accepts client requests
	LeaderState ServerState = "leader"
	// FollowerState indicates a follower server that replicates from leader
	FollowerState ServerState = "follower"
	// CandidateState indicates a server trying to become the leader
	CandidateState ServerState = "candidate"
)

// HeartbeatConfig stores configuration for heartbeat mechanism
type HeartbeatConfig struct {
	Interval        time.Duration // Interval between heartbeats
	Timeout         time.Duration // Timeout for each heartbeat
	MaxFailureCount int           // Number of consecutive failures before failover
}

// Default heartbeat configuration
var DefaultHeartbeatConfig = HeartbeatConfig{
	Interval:        10 * time.Second,
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
	role        ServerRole           // Legacy: Current role (primary or secondary)
	isPrimary   bool                 // Legacy: Is this server the primary?
	serverID    int32                // ID of this server
	peerAddress string               // Legacy: Address of the peer server
	peerClient  pb.LockServiceClient // Legacy: gRPC client for peer communication
	peerConn    *grpc.ClientConn     // Legacy: gRPC connection to peer

	// Enhanced replication fields
	serverState   atomic.Value                    // Current state (Leader/Follower/Candidate)
	currentEpoch  atomic.Int64                    // Current epoch number
	votedInEpoch  atomic.Int64                    // Last epoch this server voted in
	leaderAddress string                          // Address of the current leader
	leaderMu      sync.RWMutex                    // Protects leaderAddress
	peerAddresses []string                        // Addresses of all peer servers
	peerClients   map[string]pb.LockServiceClient // gRPC clients for peer communication
	peerClientsMu sync.RWMutex                    // Protects peerClients map
	electionTimer *time.Timer                     // Timer for leader election

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

	// Register file manager for lease expiry callbacks
	fm.RegisterForLeaseExpiry()

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
		peerClients:  make(map[string]pb.LockServiceClient),
	}

	// Initialize atomic values
	s.serverState.Store(LeaderState)
	s.currentEpoch.Store(0)
	s.votedInEpoch.Store(-1)

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

	// Register file manager for lease expiry callbacks
	fm.RegisterForLeaseExpiry()

	isPrimary := role == PrimaryRole

	// Set initial server state based on role
	var initialState ServerState
	if isPrimary {
		initialState = LeaderState
	} else {
		initialState = FollowerState
	}

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
		peerClients:      make(map[string]pb.LockServiceClient),
	}

	// Initialize atomic values
	s.serverState.Store(initialState)
	s.currentEpoch.Store(0)
	s.votedInEpoch.Store(-1)

	// If peerAddress is provided, add it to peerAddresses
	if peerAddress != "" {
		s.peerAddresses = []string{peerAddress}

		// Legacy: Connect to peer if address is provided
		if err := s.connectToPeer(); err != nil {
			s.logger.Printf("Warning: Failed to connect to peer at %s: %v", peerAddress, err)
		}
	}

	// Start heartbeat sender if this is a secondary/follower
	if !isPrimary && peerAddress != "" {
		go s.startHeartbeatSender()
	}

	// Start background replication worker for reliable updates
	if isPrimary && peerAddress != "" {
		go s.startReplicationWorker()

		// Start periodic split-brain check for primaries
		go s.startSplitBrainChecker()
	}

	return s
}

// NewReplicatedLockServerWithMultiPeers initializes a lock server with multiple peer configuration
func NewReplicatedLockServerWithMultiPeers(role ServerRole, serverID int32, peerAddresses []string, hbConfig HeartbeatConfig) *LockServer {
	logger := log.New(os.Stdout, fmt.Sprintf("[LockServer-%d] ", serverID), log.LstdFlags)

	// Initialize lock manager with lease duration
	lm := lock_manager.NewLockManagerWithLeaseDuration(logger, 30*time.Second)

	// Initialize file manager with lock manager for token validation
	fm := file_manager.NewFileManagerWithWAL(true, true, lm)

	// Register file manager for lease expiry callbacks
	fm.RegisterForLeaseExpiry()

	isPrimary := role == PrimaryRole

	// Set initial server state based on role
	var initialState ServerState
	if isPrimary {
		initialState = LeaderState
	} else {
		initialState = FollowerState
	}

	s := &LockServer{
		lockManager:      lm,
		fileManager:      fm,
		requestCache:     NewRequestCacheWithSize(10*time.Minute, 10000),
		logger:           logger,
		recoveryDone:     fm.IsRecoveryComplete(),
		role:             role,
		isPrimary:        isPrimary,
		serverID:         serverID,
		peerAddresses:    peerAddresses,
		heartbeatConfig:  hbConfig,
		fencingBuffer:    5 * time.Second, // Default 5s buffer for fencing
		replicationQueue: make([]*pb.ReplicatedState, 0),
		metrics:          NewPerformanceMetrics(logger),
		peerClients:      make(map[string]pb.LockServiceClient),
	}

	// Initialize atomic values
	s.serverState.Store(initialState)
	s.currentEpoch.Store(0)
	s.votedInEpoch.Store(-1)

	// Note: We don't immediately connect to peers; we'll do so dynamically as needed via getOrConnectPeer
	s.logger.Printf("Initialized server with %d peer addresses", len(peerAddresses))

	// Start heartbeat sender if this is a follower
	if !isPrimary && len(peerAddresses) > 0 {
		go s.startHeartbeatSender()
	}

	// If this is a leader and we have peers, start the replication worker and split-brain checker
	if isPrimary && len(peerAddresses) > 0 {
		go s.startReplicationWorker()
		go s.startSplitBrainChecker()
	}

	return s
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

// LockAcquire handles lock acquisition requests
func (s *LockServer) LockAcquire(ctx context.Context, args *pb.LockArgs) (*pb.LockResponse, error) {
	// Check if this server is in follower/candidate mode and not fencing
	if !s.isLeader() && !s.isFencing.Load() {
		leaderAddr := s.getLeaderAddress()
		errMsg := "This server is not the leader and cannot grant locks"
		if leaderAddr != "" {
			errMsg += fmt.Sprintf(" (current leader: %s)", leaderAddr)
		}

		return &pb.LockResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: errMsg,
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
	if isFencingNow && isBeforeEnd {
		s.logger.Printf("FENCING: Rejecting lock acquisition from client %d during fencing period", args.ClientId)
		return &pb.LockResponse{
			Status:       pb.Status_SERVER_FENCING,
			ErrorMessage: "Server is in fencing period and cannot grant locks",
		}, nil
	}

	s.logger.Printf("DEBUG FENCING CHECK: Proceeding with acquire for client %d", args.ClientId)

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

		// Replicate the state to all followers
		s.replicateStateToFollowers()

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
	// Check if we're in follower mode
	if !s.isLeader() {
		leaderAddr := s.getLeaderAddress()
		errMsg := "Server is not the leader and cannot process client requests"
		if leaderAddr != "" {
			errMsg += fmt.Sprintf(" (current leader: %s)", leaderAddr)
		}

		return &pb.LockResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: errMsg,
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

	// Check if the client holds the lock with the given token
	if !s.lockManager.HasLockWithToken(clientID, token) {
		// This could be a duplicate from a previous successful release that was already processed
		// Treat this as an idempotent success since the goal (client doesn't have the lock) is achieved
		s.logger.Printf("Client %d trying to release lock with token %s they don't hold - treating as idempotent success", clientID, token)
		resp := &pb.LockResponse{
			Status: pb.Status_OK,
			Token:  "",
		}
		s.requestCache.Set(requestID, resp)
		return resp, nil
	}

	// Log state before release
	s.logCurrentLockState("Before lock release")

	// Attempt to release the lock
	success := s.lockManager.Release(clientID, token)
	if success {
		s.logger.Printf("Lock released by client %d", clientID)

		// Create response
		resp := &pb.LockResponse{
			Status: pb.Status_OK,
		}

		// Cache the response
		s.requestCache.Set(requestID, resp)

		// Replicate the state to all followers
		s.replicateStateToFollowers()

		// Log state after release
		s.logCurrentLockState("After lock release")

		return resp, nil
	}

	// Release failed
	s.metrics.TrackOperationFailure(OpLockRelease)

	s.logger.Printf("Failed to release lock for client %d (unexpected error)", clientID)

	return &pb.LockResponse{
		Status:       pb.Status_ERROR,
		ErrorMessage: "Failed to release lock (internal error)",
	}, nil
}

// FileAppend handles file append requests
func (s *LockServer) FileAppend(ctx context.Context, args *pb.FileArgs) (*pb.FileResponse, error) {
	// Check if this server is in follower/candidate mode
	if !s.isLeader() {
		leaderAddr := s.getLeaderAddress()
		errMsg := "Server is not the leader and cannot process client requests"
		if leaderAddr != "" {
			errMsg += fmt.Sprintf(" (current leader: %s)", leaderAddr)
		}

		return &pb.FileResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: errMsg,
		}, nil
	}

	// For file operations, we also reject during fencing to ensure consistency
	if s.isFencing.Load() {
		return &pb.FileResponse{
			Status:       pb.Status_SERVER_FENCING,
			ErrorMessage: "Server is in fencing period and cannot process file operations",
		}, nil
	}

	// Check if request has been processed already
	if cachedResp, exists := s.requestCache.Get(args.RequestId); exists {
		s.logger.Printf("Detected repeated file append request %s from client %d", args.RequestId, args.ClientId)
		if cachedResp != nil {
			return cachedResp.(*pb.FileResponse), nil
		}
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
	// Check if this server is in follower/candidate mode
	if !s.isLeader() {
		leaderAddr := s.getLeaderAddress()
		errMsg := "Server is not the leader and cannot process client requests"
		if leaderAddr != "" {
			errMsg += fmt.Sprintf(" (current leader: %s)", leaderAddr)
		}

		return &pb.LeaseResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: errMsg,
		}, nil
	}

	// For lease renewal, allow operations during fencing if token is valid
	// This helps clients maintain their locks during a leader transition

	clientID := args.ClientId
	token := args.Token
	requestID := args.RequestId

	s.logger.Printf("Client %d attempting to renew lease with token %s (request: %s)", clientID, token, requestID)

	// Check if request has been processed already
	if cachedResp, exists := s.requestCache.Get(requestID); exists {
		s.logger.Printf("Detected repeated lease renewal request %s from client %d", requestID, clientID)
		if cachedResp != nil {
			return cachedResp.(*pb.LeaseResponse), nil
		}
	}

	// Mark request as in progress
	if !s.requestCache.MarkInProgress(requestID) {
		s.logger.Printf("Request %s already in progress", requestID)
		return &pb.LeaseResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: "Request already in progress",
		}, nil
	}

	// Validate token and check lock ownership
	if !s.lockManager.HasLockWithToken(clientID, token) {
		s.logger.Printf("Lease renewal rejected: client %d doesn't hold lock with token %s", clientID, token)
		resp := &pb.LeaseResponse{
			Status:       pb.Status_INVALID_TOKEN,
			ErrorMessage: "Invalid token or lock not held",
		}
		s.requestCache.Set(requestID, resp)
		return resp, nil
	}

	// Attempt to renew the lease
	success := s.lockManager.RenewLease(clientID, token)
	if success {
		s.logger.Printf("Lease renewed for client %d", clientID)
		resp := &pb.LeaseResponse{
			Status: pb.Status_OK,
		}
		s.requestCache.Set(requestID, resp)

		// Replicate the state to all followers
		s.replicateStateToFollowers()

		return resp, nil
	}

	// Lease renewal failed
	s.metrics.TrackOperationFailure(OpRenewLease)
	s.logger.Printf("Failed to renew lease for client %d", clientID)

	resp := &pb.LeaseResponse{
		Status:       pb.Status_ERROR,
		ErrorMessage: "Failed to renew lease",
	}
	s.requestCache.Set(requestID, resp)
	return resp, nil
}

// ServerInfo provides information about this server
func (s *LockServer) ServerInfo(ctx context.Context, req *pb.ServerInfoRequest) (*pb.ServerInfoResponse, error) {
	// Include information about server state, role, and current epoch
	var role string
	if s.isLeader() {
		role = string(LeaderState) // Use the new state terms
	} else if s.isFollower() {
		role = string(FollowerState)
	} else if s.isCandidate() {
		role = string(CandidateState)
	} else {
		// Legacy role mapping
		if s.isPrimary {
			role = string(PrimaryRole)
		} else {
			role = string(SecondaryRole)
		}
	}

	// Get the leader address if this server is not the leader
	leaderAddr := ""
	if !s.isLeader() {
		leaderAddr = s.getLeaderAddress()
	}

	return &pb.ServerInfoResponse{
		ServerId:      s.serverID,
		Role:          role,
		CurrentEpoch:  s.currentEpoch.Load(),
		LeaderAddress: leaderAddr,
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

// GetMetrics returns the server's performance metrics tracker
func (s *LockServer) GetMetrics() *PerformanceMetrics {
	return s.metrics
}
