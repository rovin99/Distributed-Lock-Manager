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
	requestCache *RequestCache // Added for idempotency
	logger       *log.Logger
}

// NewLockServer initializes a new lock server
func NewLockServer() *LockServer {
	logger := log.New(os.Stdout, "[LockServer] ", log.LstdFlags)

	// Enable write-ahead logging for file operations to handle server crashes
	fileManager := file_manager.NewFileManagerWithWAL(true, true) // Enable sync and WAL

	s := &LockServer{
		lockManager:  lock_manager.NewLockManagerWithLeaseDuration(logger, 30*time.Second),
		fileManager:  fileManager,
		requestCache: NewRequestCacheWithSize(10*time.Minute, 10000), // Cache responses for 10 minutes, max 10K entries
		logger:       logger,
	}
	return s
}

// ClientInit handles the client initialization RPC
func (s *LockServer) ClientInit(ctx context.Context, args *pb.Int) (*pb.StatusMsg, error) {
	clientID := args.Rc
	s.logger.Printf("Client %d initialized or reconnected", clientID)

	// Return both code and message
	return &pb.StatusMsg{Rc: 0, Message: "Connected!"}, nil
}

// LockAcquire handles the lock acquisition RPC
func (s *LockServer) LockAcquire(ctx context.Context, args *pb.LockArgs) (*pb.LockResponse, error) {
	clientID := args.ClientId
	requestID := args.RequestId

	s.logger.Printf("Client %d attempting to acquire lock (request ID: %s)", clientID, requestID)

	// Check if this is a duplicate request
	if cachedData, exists := s.requestCache.Get(requestID); exists {
		s.logger.Printf("Duplicate request detected for client %d, request ID %s - returning cached response",
			clientID, requestID)
		if lockResp, ok := cachedData.(*pb.LockResponse); ok {
			return lockResp, nil
		}
	}

	// Mark this request as in progress
	if !s.requestCache.MarkInProgress(requestID) {
		// If already in progress by another thread, wait for it to complete
		// or return the cached response
		s.logger.Printf("Request %s is already being processed - waiting for result", requestID)
		if cachedData, exists := s.requestCache.Get(requestID); exists {
			if lockResp, ok := cachedData.(*pb.LockResponse); ok {
				return lockResp, nil
			}
		}
	}

	// Check if client already holds the lock (for handling retries)
	if s.lockManager.HasLock(clientID) {
		// Get the current token
		_, token := s.lockManager.Acquire(clientID)
		s.logger.Printf("Client %d already holds the lock - handling retry with token %s", clientID, token)
		resp := &pb.LockResponse{
			Response: &pb.Response{Status: pb.Status_SUCCESS},
			Token:    token,
		}
		// Cache the response for future duplicates
		s.requestCache.Set(requestID, resp)
		return resp, nil
	}

	// Use the context-aware acquire method with timeout
	success, token := s.lockManager.AcquireWithTimeout(clientID, ctx)

	var resp *pb.LockResponse
	if success {
		s.logger.Printf("Lock acquired by client %d with token %s", clientID, token)
		resp = &pb.LockResponse{
			Response: &pb.Response{Status: pb.Status_SUCCESS},
			Token:    token,
		}
	} else {
		s.logger.Printf("Client %d timed out waiting for lock", clientID)
		resp = &pb.LockResponse{
			Response: &pb.Response{Status: pb.Status_TIMEOUT},
			Token:    "",
		}
	}

	// Cache the response
	s.requestCache.Set(requestID, resp)
	return resp, nil
}

// LockRelease handles the lock release RPC
func (s *LockServer) LockRelease(ctx context.Context, args *pb.LockArgs) (*pb.Response, error) {
	clientID := args.ClientId
	requestID := args.RequestId
	token := args.Token

	s.logger.Printf("Client %d attempting to release lock with token %s (request ID: %s)", clientID, token, requestID)

	// Check if this is a duplicate request
	if cachedData, exists := s.requestCache.Get(requestID); exists {
		s.logger.Printf("Duplicate request detected for client %d, request ID %s - returning cached response",
			clientID, requestID)
		if resp, ok := cachedData.(*pb.Response); ok {
			return resp, nil
		}
	}

	// Mark this request as in progress
	if !s.requestCache.MarkInProgress(requestID) {
		// If already in progress by another thread, wait for it to complete
		// or return the cached response
		s.logger.Printf("Request %s is already being processed - waiting for result", requestID)
		if cachedData, exists := s.requestCache.Get(requestID); exists {
			if resp, ok := cachedData.(*pb.Response); ok {
				return resp, nil
			}
		}
	}

	// If client doesn't hold the lock, this might be a retry after a successful release
	// where the response was lost. In this case, just return success.
	if !s.lockManager.HasLockWithToken(clientID, token) {
		s.logger.Printf("Client %d doesn't hold the lock with token %s - might be a retry after successful release or token expired", clientID, token)
		resp := &pb.Response{Status: pb.Status_SUCCESS}
		// Cache the response for future duplicates
		s.requestCache.Set(requestID, resp)
		return resp, nil
	}

	success := s.lockManager.Release(clientID, token)

	var resp *pb.Response
	if success {
		resp = &pb.Response{Status: pb.Status_SUCCESS}
	} else {
		resp = &pb.Response{Status: pb.Status_PERMISSION_DENIED}
	}

	// Cache the response
	s.requestCache.Set(requestID, resp)
	return resp, nil
}

// FileAppend handles the file append RPC
func (s *LockServer) FileAppend(ctx context.Context, args *pb.FileArgs) (*pb.Response, error) {
	clientID := args.ClientId
	requestID := args.RequestId
	token := args.Token

	s.logger.Printf("Client %d attempting to append to file with token %s (request ID: %s)", clientID, token, requestID)

	// Check if this is a duplicate request
	if cachedData, exists := s.requestCache.Get(requestID); exists {
		s.logger.Printf("Duplicate request detected for client %d, request ID %s - returning cached response",
			clientID, requestID)
		if resp, ok := cachedData.(*pb.Response); ok {
			return resp, nil
		}
	}

	// Mark this request as in progress
	if !s.requestCache.MarkInProgress(requestID) {
		// If already in progress by another thread, wait for it to complete
		// or return the cached response
		s.logger.Printf("Request %s is already being processed - waiting for result", requestID)
		if cachedData, exists := s.requestCache.Get(requestID); exists {
			if resp, ok := cachedData.(*pb.Response); ok {
				return resp, nil
			}
		}
	}

	// Check if this client holds the lock with the valid token
	if !s.lockManager.HasLockWithToken(clientID, token) {
		s.logger.Printf("File append failed: client %d doesn't hold the lock with token %s", clientID, token)
		resp := &pb.Response{Status: pb.Status_PERMISSION_DENIED}
		// Cache the response
		s.requestCache.Set(requestID, resp)
		return resp, nil
	}

	// Process the file append with request ID for write-ahead logging
	err := s.fileManager.AppendToFileWithRequestID(args.Filename, args.Content, requestID)

	var resp *pb.Response
	if err != nil {
		s.logger.Printf("File append error: %v", err)
		resp = &pb.Response{Status: pb.Status_FILE_ERROR}
	} else {
		s.logger.Printf("File append successful for client %d", clientID)
		// Renew the lease after a successful operation
		s.lockManager.RenewLease(clientID, token)
		resp = &pb.Response{Status: pb.Status_SUCCESS}
	}

	// Cache the response
	s.requestCache.Set(requestID, resp)
	return resp, nil
}

// RenewLease handles the lease renewal RPC
func (s *LockServer) RenewLease(ctx context.Context, args *pb.LeaseArgs) (*pb.Response, error) {
	clientID := args.ClientId
	requestID := args.RequestId
	token := args.Token

	s.logger.Printf("Client %d attempting to renew lease with token %s (request ID: %s)", clientID, token, requestID)

	// Check if this is a duplicate request
	if cachedData, exists := s.requestCache.Get(requestID); exists {
		s.logger.Printf("Duplicate request detected for client %d, request ID %s - returning cached response",
			clientID, requestID)
		if resp, ok := cachedData.(*pb.Response); ok {
			return resp, nil
		}
	}

	// Mark this request as in progress
	if !s.requestCache.MarkInProgress(requestID) {
		// If already in progress by another thread, wait for it to complete
		// or return the cached response
		s.logger.Printf("Request %s is already being processed - waiting for result", requestID)
		if cachedData, exists := s.requestCache.Get(requestID); exists {
			if resp, ok := cachedData.(*pb.Response); ok {
				return resp, nil
			}
		}
	}

	success := s.lockManager.RenewLease(clientID, token)

	var resp *pb.Response
	if success {
		resp = &pb.Response{Status: pb.Status_SUCCESS}
	} else {
		resp = &pb.Response{Status: pb.Status_LEASE_EXPIRED}
	}

	// Cache the response
	s.requestCache.Set(requestID, resp)
	return resp, nil
}

// ClientClose handles the client close RPC
func (s *LockServer) ClientClose(ctx context.Context, args *pb.Int) (*pb.StatusMsg, error) {
	clientID := args.Rc
	s.logger.Printf("Client %d closing connection", clientID)

	// If this client holds the lock, release it
	s.lockManager.ReleaseLockIfHeld(clientID)

	// Return both code and message
	return &pb.StatusMsg{Rc: 0, Message: "Disconnected!"}, nil
}

// CreateFiles ensures the 100 files exist - now delegates to file manager
func CreateFiles() {
	fm := file_manager.NewFileManager(false)
	fm.CreateFiles()
}

// Cleanup closes any open files and performs other cleanup tasks
func (s *LockServer) Cleanup() {
	s.fileManager.Cleanup()
	s.logger.Println("Server cleanup complete")
}
