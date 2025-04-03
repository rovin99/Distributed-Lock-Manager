package server

import (
	"context"
	"log"
	"os"
	"time"

	"Distributed-Lock-Manager/internal/file_manager"
	"Distributed-Lock-Manager/internal/lock_manager"
	pb "Distributed-Lock-Manager/proto"
)

// LockServer implements the LockServiceServer interface
type LockServer struct {
	pb.UnimplementedLockServiceServer
	lockManager  *lock_manager.LockManager
	fileManager  *file_manager.FileManager
	requestCache *RequestCache
	logger       *log.Logger
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
	}
	return s
}

// ClientInit handles the client initialization RPC
func (s *LockServer) ClientInit(ctx context.Context, args *pb.ClientInitArgs) (*pb.ClientInitResponse, error) {
	clientID := args.ClientId
	s.logger.Printf("Client %d initialized or reconnected", clientID)

	return &pb.ClientInitResponse{
		Status:       pb.Status_OK,
		ErrorMessage: "",
	}, nil
}

// LockAcquire handles the lock acquisition RPC
func (s *LockServer) LockAcquire(ctx context.Context, args *pb.LockArgs) (*pb.LockResponse, error) {
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

// RenewLease handles the lease renewal RPC
func (s *LockServer) RenewLease(ctx context.Context, args *pb.LeaseArgs) (*pb.LeaseResponse, error) {
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

	// Validate token and check lock ownership
	if !s.lockManager.HasLockWithToken(clientID, token) {
		s.logger.Printf("Lease renewal failed: client %d doesn't hold the lock with token %s", clientID, token)
		resp := &pb.LeaseResponse{
			Status:       pb.Status_INVALID_TOKEN,
			ErrorMessage: "Invalid token or lock not held",
		}
		s.requestCache.Set(requestID, resp)
		return resp, nil
	}

	success := s.lockManager.RenewLease(clientID, token)

	var resp *pb.LeaseResponse
	if success {
		s.logger.Printf("Lease renewed successfully for client %d", clientID)
		resp = &pb.LeaseResponse{
			Status:       pb.Status_OK,
			ErrorMessage: "",
		}
	} else {
		s.logger.Printf("Lease renewal failed for client %d", clientID)
		resp = &pb.LeaseResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: "Failed to renew lease",
		}
	}

	s.requestCache.Set(requestID, resp)
	return resp, nil
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
