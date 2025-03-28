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
	s := &LockServer{
		lockManager:  lock_manager.NewLockManager(logger),
		fileManager:  file_manager.NewFileManager(false), // Disable sync for better performance
		requestCache: NewRequestCache(10 * time.Minute),  // Cache responses for 10 minutes
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
func (s *LockServer) LockAcquire(ctx context.Context, args *pb.LockArgs) (*pb.Response, error) {
	clientID := args.ClientId
	requestID := args.RequestId

	s.logger.Printf("Client %d attempting to acquire lock (request ID: %s)", clientID, requestID)

	// Check if this is a duplicate request
	if cachedResp, exists := s.requestCache.Get(requestID); exists {
		s.logger.Printf("Duplicate request detected for client %d, request ID %s - returning cached response",
			clientID, requestID)
		return cachedResp, nil
	}

	// Check if client already holds the lock (for handling retries)
	if s.lockManager.HasLock(clientID) {
		s.logger.Printf("Client %d already holds the lock - handling retry", clientID)
		resp := &pb.Response{Status: pb.Status_SUCCESS}
		// Cache the response for future duplicates
		s.requestCache.Set(requestID, resp)
		return resp, nil
	}

	// Use the context-aware acquire method with timeout
	success := s.lockManager.AcquireWithTimeout(clientID, ctx)

	var resp *pb.Response
	if success {
		s.logger.Printf("Lock acquired by client %d", clientID)
		resp = &pb.Response{Status: pb.Status_SUCCESS}
	} else {
		s.logger.Printf("Client %d timed out waiting for lock", clientID)
		resp = &pb.Response{Status: pb.Status_TIMEOUT}
	}

	// Cache the response
	s.requestCache.Set(requestID, resp)
	return resp, nil
}

// LockRelease handles the lock release RPC
func (s *LockServer) LockRelease(ctx context.Context, args *pb.LockArgs) (*pb.Response, error) {
	clientID := args.ClientId
	requestID := args.RequestId

	s.logger.Printf("Client %d attempting to release lock (request ID: %s)", clientID, requestID)

	// Check if this is a duplicate request
	if cachedResp, exists := s.requestCache.Get(requestID); exists {
		s.logger.Printf("Duplicate request detected for client %d, request ID %s - returning cached response",
			clientID, requestID)
		return cachedResp, nil
	}

	// If client doesn't hold the lock, this might be a retry after a successful release
	// where the response was lost. In this case, just return success.
	if !s.lockManager.HasLock(clientID) {
		s.logger.Printf("Client %d doesn't hold the lock - might be a retry after successful release", clientID)
		resp := &pb.Response{Status: pb.Status_SUCCESS}
		// Cache the response for future duplicates
		s.requestCache.Set(requestID, resp)
		return resp, nil
	}

	success := s.lockManager.Release(clientID)

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

	s.logger.Printf("Client %d attempting to append to file (request ID: %s)", clientID, requestID)

	// Check if this is a duplicate request
	if cachedResp, exists := s.requestCache.Get(requestID); exists {
		s.logger.Printf("Duplicate request detected for client %d, request ID %s - returning cached response",
			clientID, requestID)
		return cachedResp, nil
	}

	// Check if this client holds the lock
	if !s.lockManager.HasLock(clientID) {
		s.logger.Printf("File append failed: client %d doesn't hold the lock", clientID)
		resp := &pb.Response{Status: pb.Status_PERMISSION_DENIED}
		// Cache the response
		s.requestCache.Set(requestID, resp)
		return resp, nil
	}

	// Process the file append
	err := s.fileManager.AppendToFile(args.Filename, args.Content)

	var resp *pb.Response
	if err != nil {
		s.logger.Printf("File append error: %v", err)
		resp = &pb.Response{Status: pb.Status_FILE_ERROR}
	} else {
		s.logger.Printf("File append successful for client %d", clientID)
		resp = &pb.Response{Status: pb.Status_SUCCESS}
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
