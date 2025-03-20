package server

import (
	"context"
	"log"
	"os"

	"Distributed-Lock-Manager/internal/file_manager"
	"Distributed-Lock-Manager/internal/lock_manager"
	pb "Distributed-Lock-Manager/proto"
)

// LockServer implements the LockServiceServer interface
type LockServer struct {
	pb.UnimplementedLockServiceServer
	lockManager *lock_manager.LockManager
	fileManager *file_manager.FileManager
	logger      *log.Logger
}

// NewLockServer initializes a new lock server
func NewLockServer() *LockServer {
	s := &LockServer{
		lockManager: lock_manager.NewLockManager(),
		fileManager: file_manager.NewFileManager(),
		logger:      log.New(os.Stdout, "[LockServer] ", log.LstdFlags),
	}
	return s
}

// ClientInit handles the client initialization RPC
func (s *LockServer) ClientInit(ctx context.Context, args *pb.Int) (*pb.Int, error) {
	s.logger.Printf("Client %d initialized", args.Rc)
	// Simple handshake: return 0 to acknowledge
	return &pb.Int{Rc: 0}, nil
}

// LockAcquire handles the lock acquisition RPC
func (s *LockServer) LockAcquire(ctx context.Context, args *pb.LockArgs) (*pb.Response, error) {
	clientID := args.ClientId
	
	success := s.lockManager.Acquire(clientID)
	if success {
		return &pb.Response{Status: pb.Status_SUCCESS}, nil
	}
	
	return &pb.Response{Status: pb.Status_FILE_ERROR}, nil
}

// LockRelease handles the lock release RPC
func (s *LockServer) LockRelease(ctx context.Context, args *pb.LockArgs) (*pb.Response, error) {
	clientID := args.ClientId
	
	success := s.lockManager.Release(clientID)
	if success {
		return &pb.Response{Status: pb.Status_SUCCESS}, nil
	}
	
	return &pb.Response{Status: pb.Status_FILE_ERROR}, nil
}

// FileAppend handles the file append RPC
func (s *LockServer) FileAppend(ctx context.Context, args *pb.FileArgs) (*pb.Response, error) {
	clientID := args.ClientId
	
	// Check if this client holds the lock
	if !s.lockManager.HasLock(clientID) {
		s.logger.Printf("File append failed: client %d doesn't hold the lock", clientID)
		return &pb.Response{Status: pb.Status_FILE_ERROR}, nil
	}
	
	err := s.fileManager.AppendToFile(args.Filename, args.Content)
	if err != nil {
		return &pb.Response{Status: pb.Status_FILE_ERROR}, nil
	}
	
	return &pb.Response{Status: pb.Status_SUCCESS}, nil
}

// ClientClose handles the client close RPC
func (s *LockServer) ClientClose(ctx context.Context, args *pb.Int) (*pb.Int, error) {
	clientID := args.Rc
	s.logger.Printf("Client %d closing connection", clientID)
	
	// If this client holds the lock, release it
	s.lockManager.ReleaseLockIfHeld(clientID)
	
	// Simple acknowledgment: return 0
	return &pb.Int{Rc: 0}, nil
}

// CreateFiles ensures the 100 files exist - now delegates to file manager
func CreateFiles() {
	fm := file_manager.NewFileManager()
	fm.CreateFiles()
}

// Cleanup closes any open files and performs other cleanup tasks
func (s *LockServer) Cleanup() {
	s.fileManager.Cleanup()
	s.logger.Println("Server cleanup complete")
}