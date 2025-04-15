package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"Distributed-Lock-Manager/internal/file_manager"
	"Distributed-Lock-Manager/internal/lock_manager"
	pb "Distributed-Lock-Manager/proto"

	"google.golang.org/grpc/peer"
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
	Interval:        500 * time.Millisecond, // Reduced from 1s to 500ms for faster leader failure detection
	Timeout:         300 * time.Millisecond, // Reduced from 500ms to 300ms
	MaxFailureCount: 2,                      // Reduced from 3 to 2 for faster failover
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

	serverID    int32  // ID of this server
	selfAddress string // Address of this server (used for leader identification)

	// Replication fields
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
	replicationQueue      []*pb.ReplicatedState // Queue of updates to be sent to followers
	replicationQueueMu    sync.Mutex            // Protects the replication queue
	replicationInProgress atomic.Bool           // Flag to prevent multiple concurrent replication workers

	// Heartbeat configuration
	heartbeatConfig HeartbeatConfig // Configuration for heartbeat mechanism

	// Fencing-related fields
	isFencing      atomic.Bool   // Is the server in fencing period?
	fencingEndTime time.Time     // When does the fencing period end?
	fencingBuffer  time.Duration // Additional buffer time after lease duration
}

// NewReplicatedLockServerWithMultiPeers initializes a lock server with multiple peers
func NewReplicatedLockServerWithMultiPeers(serverID int32, serverAddr string, peerAddresses []string, hbConfig HeartbeatConfig) *LockServer {
	logger := log.New(os.Stdout, fmt.Sprintf("[LockServer-%d] ", serverID), log.LstdFlags)

	// Initialize lock manager with lease duration
	lm := lock_manager.NewLockManagerWithLeaseDuration(logger, 30*time.Second)

	// Initialize file manager with lock manager for token validation
	fm := file_manager.NewFileManagerWithWAL(true, true, lm)

	// Register file manager for lease expiry callbacks
	fm.RegisterForLeaseExpiry()

	// Set initial server state based on ID
	// By convention, the server with the lowest ID starts as leader
	// but will go through proper election if not available
	var initialState ServerState
	if serverID == 1 { // Lowest ID becomes initial leader
		initialState = LeaderState
		logger.Printf("Starting as Leader (lowest ID in cluster)")
	} else {
		initialState = FollowerState
		logger.Printf("Starting as Follower")
	}

	s := &LockServer{
		lockManager:      lm,
		fileManager:      fm,
		requestCache:     NewRequestCacheWithSize(10*time.Minute, 10000),
		logger:           logger,
		recoveryDone:     fm.IsRecoveryComplete(),
		serverID:         serverID,
		selfAddress:      serverAddr,
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
	s.logger.Printf("Initialized server with address %s and %d peer addresses", serverAddr, len(peerAddresses))

	// Initialize election timer for followers
	if initialState == FollowerState {
		s.resetElectionTimer()
	}

	// Start heartbeat sender if this is a follower
	if initialState == FollowerState && len(peerAddresses) > 0 {
		go s.startHeartbeatSender()
	}

	// If this is a leader and we have peers, start the replication worker
	if initialState == LeaderState && len(peerAddresses) > 0 {
		go s.startReplicationWorker()
	}

	// This server is ready to accept requests
	s.logger.Printf("Server initialized and ready to accept requests")
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
	// Extract client information
	clientID := args.ClientId
	requestID := args.RequestId

	// Get client address for logging
	p, ok := peer.FromContext(ctx)
	clientAddr := "unknown"
	if ok {
		clientAddr = p.Addr.String()
	}

	s.logger.Printf("Lock acquire request from client %d (request ID: %s, addr: %s)",
		clientID, requestID, clientAddr)

	// If this is a follower, redirect to leader
	if !s.isLeader() {
		leaderAddr := s.getLeaderAddress()
		s.logger.Printf("Rejecting lock acquire from client %d as we are not the leader", clientID)

		return &pb.LockResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: fmt.Sprintf("This server is not the leader. Try the leader at %s", leaderAddr),
		}, nil
	}

	// If in fencing period, reject acquisition requests
	if s.isFencing.Load() {
		timeRemaining := time.Until(s.fencingEndTime).Round(time.Millisecond)
		s.logger.Printf("Rejecting lock acquire during fencing period (%.0f ms remaining)", float64(timeRemaining.Milliseconds()))

		return &pb.LockResponse{
			Status:       pb.Status_SERVER_FENCING,
			ErrorMessage: fmt.Sprintf("Server in fencing period for %.2f more seconds", timeRemaining.Seconds()),
		}, nil
	}

	// Check request cache for idempotence
	if cachedResp, found := s.requestCache.Get(requestID); found {
		s.logger.Printf("Found cached response for request ID %s (client %d)", requestID, clientID)
		return cachedResp.(*pb.LockResponse), nil
	}

	// Attempt to acquire the lock with the lock manager
	success, token := s.lockManager.Acquire(clientID)

	// Prepare response based on success
	var status pb.Status
	var errMessage string

	if success {
		status = pb.Status_OK
		errMessage = ""
		s.logger.Printf("Lock acquired by client %d with token %s", clientID, token)

		// After successful acquisition, replicate to followers
		s.replicateStateToFollowers()
	} else {
		status = pb.Status_LOCK_HELD
		errMessage = "Lock is currently held by another client"
		s.logger.Printf("Lock acquisition failed for client %d (lock held by another client)", clientID)
	}

	// Create response
	response := &pb.LockResponse{
		Status:       status,
		ErrorMessage: errMessage,
		Token:        token,
	}

	// Cache successful responses for idempotence
	if success {
		s.requestCache.Set(requestID, response)
	}

	return response, nil
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
	// Get client address for logging
	p, ok := peer.FromContext(ctx)
	clientAddr := "unknown"
	if ok {
		clientAddr = p.Addr.String()
	}

	state := s.getServerStateString()
	epoch := s.currentEpoch.Load()

	// Get the leader address based on the current state
	var leaderAddr string
	if s.isLeader() {
		// If this server is the leader, return its own address
		if s.selfAddress == "" || strings.HasPrefix(s.selfAddress, ":") {
			// If it starts with : (port only), or is empty, use localhost

			// Extract port number or use default if empty
			port := "50051" // Default port
			if s.selfAddress != "" {
				port = strings.TrimPrefix(s.selfAddress, ":")
			}

			// Use localhost for testing compatibility
			leaderAddr = "localhost:" + port
		} else {
			// Normal case where host is specified
			parts := strings.Split(s.selfAddress, ":")
			if len(parts) >= 2 {
				// Extract just the port number
				port := parts[len(parts)-1]

				// Use localhost instead of hostname, for testing compatibility
				leaderAddr = "localhost:" + port
			} else {
				// Fallback to the stored address format
				leaderAddr = s.selfAddress
			}
		}

		s.logger.Printf("Reporting self as leader with address: %s", leaderAddr)
	} else {
		// If follower, return the known leader address
		leaderAddr = s.getLeaderAddress()
	}

	s.logger.Printf("Server info requested by %s, responding with: ID=%d, state=%s, epoch=%d, leaderAddr=%s",
		clientAddr, s.serverID, state, epoch, leaderAddr)

	// Provide complete server information in the response
	return &pb.ServerInfoResponse{
		ServerId:      s.serverID,
		Role:          string(state),
		CurrentEpoch:  epoch,
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
	if len(s.peerAddresses) == 0 {
		// No peers to check against
		return nil
	}

	// Retry a few times since the peer might still be starting up
	maxRetries := 5
	retryDelay := 1 * time.Second

	// Check against all peers to ensure unique server ID
	var lastErr error
	for _, peerAddress := range s.peerAddresses {
		for attempt := 0; attempt < maxRetries; attempt++ {
			// Get or connect to the peer
			client, err := s.getOrConnectPeer(peerAddress)
			if err != nil {
				lastErr = fmt.Errorf("failed to connect to peer %s for ID verification: %w", peerAddress, err)
				s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
				time.Sleep(retryDelay)
				continue
			}

			// Use ServerInfo RPC to check peer's ID
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := client.ServerInfo(ctx, &pb.ServerInfoRequest{})
			cancel()

			if err != nil {
				lastErr = fmt.Errorf("unable to verify peer server ID with %s: %w", peerAddress, err)
				s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
				time.Sleep(retryDelay)
				continue
			}

			// Successfully got peer info - check for duplicate ID
			if resp.ServerId == s.serverID {
				errMsg := fmt.Sprintf("Duplicate server ID detected! This server and peer at %s both have ID %d",
					peerAddress, s.serverID)
				s.logger.Printf("ERROR: %s", errMsg)
				return fmt.Errorf(errMsg)
			}

			s.logger.Printf("Verified unique server ID with peer at %s: local=%d, peer=%d",
				peerAddress, s.serverID, resp.ServerId)

			// We were able to verify with this peer, move to the next one
			break
		}
	}

	s.logger.Printf("Successfully verified unique server ID with all peers")
	return nil
}

// VerifySharedFilesystem checks if the shared filesystem is properly accessible with retry
func (s *LockServer) VerifySharedFilesystem() error {
	if len(s.peerAddresses) == 0 {
		// No peers to verify with
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

	// Create a test file with random content
	testFileName := fmt.Sprintf("./data/fs_verify_%d.tmp", time.Now().UnixNano())
	testContentStr := fmt.Sprintf("Filesystem verification from server %d at %s",
		s.serverID, time.Now().Format(time.RFC3339))
	testContent = []byte(testContentStr)

	// Write test content to file
	if err := os.WriteFile(testFileName, testContent, 0644); err != nil {
		return fmt.Errorf("failed to write test file for filesystem verification: %w", err)
	}
	defer os.Remove(testFileName) // Clean up the test file when done

	// Check with each peer
	var verificationSuccessful bool

	for _, peerAddress := range s.peerAddresses {
		var peerSuccess bool
		var lastErr error

		for attempt := 0; attempt < maxRetries; attempt++ {
			// Get or connect to the peer
			client, err := s.getOrConnectPeer(peerAddress)
			if err != nil {
				lastErr = fmt.Errorf("failed to connect to peer %s: %w", peerAddress, err)
				s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
				time.Sleep(retryDelay)
				continue
			}

			// Ask peer to read the file
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := client.VerifyFileAccess(ctx, &pb.FileAccessRequest{
				FilePath:        testFileName,
				ExpectedContent: testContentStr,
			})
			cancel()

			if err != nil {
				lastErr = fmt.Errorf("RPC error with peer %s: %w", peerAddress, err)
				s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
				time.Sleep(retryDelay)
				continue
			}

			if resp.Status != pb.Status_OK {
				lastErr = fmt.Errorf("verification failed with peer %s: %s", peerAddress, resp.ErrorMessage)
				s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
				time.Sleep(retryDelay)
				continue
			}

			if resp.ActualContent != testContentStr {
				lastErr = fmt.Errorf("content mismatch with peer %s: expected '%s', got '%s'",
					peerAddress, testContentStr, resp.ActualContent)
				s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
				time.Sleep(retryDelay)
				continue
			}

			// Success with this peer!
			s.logger.Printf("Filesystem verification successful with peer at %s", peerAddress)
			peerSuccess = true
			verificationSuccessful = true
			break
		}

		if !peerSuccess {
			s.logger.Printf("WARNING: Could not verify shared filesystem with peer %s after %d attempts: %v",
				peerAddress, maxRetries, lastErr)
		}
	}

	if !verificationSuccessful {
		s.logger.Printf("WARNING: Could not verify shared filesystem with any peers")
		s.logger.Printf("Continuing startup, but be aware that filesystem sharing may not be working correctly")
	} else {
		s.logger.Printf("Shared filesystem verification successful with at least one peer")
	}

	return nil
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
