package server

import (
	"context"
	"fmt"
	"log"
	"os"
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

// LockServer implements the LockServiceServer interface
type LockServer struct {
	pb.UnimplementedLockServiceServer
	lockManager  *lock_manager.LockManager
	fileManager  *file_manager.FileManager
	requestCache *RequestCache
	logger       *log.Logger
	recoveryDone bool // Indicates if WAL recovery is complete

	// Replication-related fields
	role        ServerRole           // Current role (primary or secondary)
	isPrimary   bool                 // Is this server the primary?
	serverID    int32                // ID of this server
	peerAddress string               // Address of the peer server
	peerClient  pb.LockServiceClient // gRPC client for peer communication
	peerConn    *grpc.ClientConn     // gRPC connection to peer

	// Fencing-related fields
	isFencing      atomic.Bool // Is the server in fencing period?
	fencingEndTime time.Time   // When does the fencing period end?
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
	}
	return s
}

// NewReplicatedLockServer initializes a lock server with replication configuration
func NewReplicatedLockServer(role ServerRole, serverID int32, peerAddress string) *LockServer {
	logger := log.New(os.Stdout, fmt.Sprintf("[LockServer-%d] ", serverID), log.LstdFlags)

	// Initialize lock manager with lease duration
	lm := lock_manager.NewLockManagerWithLeaseDuration(logger, 30*time.Second)

	// Initialize file manager with lock manager for token validation
	fm := file_manager.NewFileManagerWithWAL(true, true, lm)

	isPrimary := role == PrimaryRole

	s := &LockServer{
		lockManager:  lm,
		fileManager:  fm,
		requestCache: NewRequestCacheWithSize(10*time.Minute, 10000),
		logger:       logger,
		recoveryDone: fm.IsRecoveryComplete(),
		role:         role,
		isPrimary:    isPrimary,
		serverID:     serverID,
		peerAddress:  peerAddress,
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
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	const maxConsecutiveFailures = 3
	failureCount := 0

	for range ticker.C {
		// Stop sending heartbeats if we're now the primary
		if s.isPrimary {
			s.logger.Printf("This server is now primary, stopping heartbeat")
			return
		}

		// Send heartbeat
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := s.peerClient.Ping(ctx, &pb.HeartbeatRequest{
			ServerId: s.serverID,
		})
		cancel()

		if err != nil {
			failureCount++
			s.logger.Printf("Heartbeat failed (%d/%d): %v",
				failureCount, maxConsecutiveFailures, err)

			// Check if we've reached the failure threshold
			if failureCount >= maxConsecutiveFailures {
				s.logger.Printf("Primary is unreachable after %d attempts, promoting to primary",
					maxConsecutiveFailures)
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
	fencingDuration := leaseDuration + 5*time.Second // Add 5s buffer
	s.fencingEndTime = time.Now().Add(fencingDuration)
	s.isFencing.Store(true)

	s.logger.Printf("Entering fencing period for %v (until %v)",
		fencingDuration, s.fencingEndTime)

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

	// Clear lock state to ensure safe operation
	if err := s.lockManager.ForceClearLockState(); err != nil {
		s.logger.Printf("Warning: Failed to clear lock state after fencing: %v", err)
	}

	s.logger.Printf("Server is now fully operational as primary")
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

	// Send update asynchronously to avoid blocking client operations
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := s.peerClient.UpdateSecondaryState(ctx, &pb.ReplicatedState{
			LockHolder:      holder,
			LockToken:       token,
			ExpiryTimestamp: expiryTimestamp,
		})

		if err != nil {
			s.logger.Printf("Failed to replicate state to secondary: %v", err)
		}
	}()
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

// LockAcquire handles the lock acquisition RPC
func (s *LockServer) LockAcquire(ctx context.Context, args *pb.LockArgs) (*pb.LockResponse, error) {
	// Check if we're in secondary mode
	if !s.isPrimary {
		return &pb.LockResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "Server is in secondary mode and cannot process client requests",
			Token:        "",
		}, nil
	}

	// Check if we're in fencing period
	if s.isFencing.Load() {
		return &pb.LockResponse{
			Status:       pb.Status_SERVER_FENCING,
			ErrorMessage: "Server is in fencing period after failover",
			Token:        "",
		}, nil
	}

	clientID := args.ClientId
	token := args.Token
	requestID := args.RequestId

	s.logger.Printf("Client %d attempting to acquire lock with token %s (request: %s)", clientID, token, requestID)

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

	// Check if client already holds the lock (for handling retries)
	if s.lockManager.HasLockWithToken(clientID, token) {
		s.logger.Printf("Client %d already holds the lock with token %s", clientID, token)
		resp := &pb.LockResponse{
			Status:       pb.Status_OK,
			ErrorMessage: "",
			Token:        token,
		}
		s.requestCache.Set(requestID, resp)
		return resp, nil
	}

	// Use the context-aware acquire method with timeout
	success, newToken := s.lockManager.AcquireWithTimeout(clientID, ctx)

	var resp *pb.LockResponse
	if success {
		s.logger.Printf("Lock acquired by client %d with token %s", clientID, newToken)
		resp = &pb.LockResponse{
			Status:       pb.Status_OK,
			ErrorMessage: "",
			Token:        newToken,
		}

		// Replicate state to secondary
		s.replicateStateToSecondary()
	} else {
		s.logger.Printf("Client %d failed to acquire lock", clientID)
		resp = &pb.LockResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: "Failed to acquire lock",
			Token:        "",
		}
	}

	s.requestCache.Set(requestID, resp)
	return resp, nil
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

	// Just log and respond - this confirms the primary is alive
	s.logger.Printf("Received heartbeat from secondary server %d", req.ServerId)

	return &pb.HeartbeatResponse{
		Status:       pb.Status_OK,
		ErrorMessage: "",
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
