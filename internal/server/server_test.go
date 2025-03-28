package server

import (
	"context"
	"testing"
	"time"

	"Distributed-Lock-Manager/internal/file_manager"
	pb "Distributed-Lock-Manager/proto"

	"github.com/stretchr/testify/assert"
)

// setupTestServer initializes a test server with a mock file manager
func setupTestServer() *LockServer {
	// Create a server with a test file manager that doesn't actually write to disk
	s := NewLockServer()
	// Replace the file manager with a test version if needed
	// s.fileManager = file_manager.NewFileManager(false)
	return s
}

// TestDuplicateFileAppend verifies that duplicate file append requests are idempotent
func TestDuplicateFileAppend(t *testing.T) {
	// Setup
	s := setupTestServer()
	ctx := context.Background()

	// Create a test file
	fm := file_manager.NewFileManager(false)
	fm.CreateFiles()

	// First, acquire the lock
	clientID := int32(1)
	requestID1 := "1-1"
	lockArgs := &pb.LockArgs{
		ClientId:  clientID,
		RequestId: requestID1,
	}

	resp, err := s.LockAcquire(ctx, lockArgs)
	assert.NoError(t, err)
	assert.Equal(t, pb.Status_SUCCESS, resp.Status)

	// Now append to a file
	fileRequestID := "1-2"
	fileArgs := &pb.FileArgs{
		Filename:  "file_0",
		Content:   []byte("test data"),
		ClientId:  clientID,
		RequestId: fileRequestID,
	}

	// First request
	resp1, err := s.FileAppend(ctx, fileArgs)
	assert.NoError(t, err)
	assert.Equal(t, pb.Status_SUCCESS, resp1.Status)

	// Duplicate request with same request ID
	resp2, err := s.FileAppend(ctx, fileArgs)
	assert.NoError(t, err)
	assert.Equal(t, pb.Status_SUCCESS, resp2.Status)

	// The response should be cached, so both should be identical
	assert.Equal(t, resp1, resp2)
}

// TestDuplicateLockAcquire verifies that duplicate lock acquisition requests are idempotent
func TestDuplicateLockAcquire(t *testing.T) {
	// Setup
	s := setupTestServer()
	ctx := context.Background()

	clientID := int32(1)
	requestID := "1-1"
	lockArgs := &pb.LockArgs{
		ClientId:  clientID,
		RequestId: requestID,
	}

	// First request
	resp1, err := s.LockAcquire(ctx, lockArgs)
	assert.NoError(t, err)
	assert.Equal(t, pb.Status_SUCCESS, resp1.Status)

	// Duplicate request with same request ID
	resp2, err := s.LockAcquire(ctx, lockArgs)
	assert.NoError(t, err)
	assert.Equal(t, pb.Status_SUCCESS, resp2.Status)

	// The response should be cached, so both should be identical
	assert.Equal(t, resp1, resp2)

	// A new request with a different request ID should still succeed
	// because the client already holds the lock
	newRequestID := "1-2"
	lockArgs2 := &pb.LockArgs{
		ClientId:  clientID,
		RequestId: newRequestID,
	}

	resp3, err := s.LockAcquire(ctx, lockArgs2)
	assert.NoError(t, err)
	assert.Equal(t, pb.Status_SUCCESS, resp3.Status)
}

// TestDuplicateLockRelease verifies that duplicate lock release requests are idempotent
func TestDuplicateLockRelease(t *testing.T) {
	// Setup
	s := setupTestServer()
	ctx := context.Background()

	clientID := int32(1)

	// First acquire the lock
	acquireRequestID := "1-1"
	lockArgs := &pb.LockArgs{
		ClientId:  clientID,
		RequestId: acquireRequestID,
	}

	resp, err := s.LockAcquire(ctx, lockArgs)
	assert.NoError(t, err)
	assert.Equal(t, pb.Status_SUCCESS, resp.Status)

	// Now release the lock
	releaseRequestID := "1-2"
	releaseArgs := &pb.LockArgs{
		ClientId:  clientID,
		RequestId: releaseRequestID,
	}

	// First release request
	resp1, err := s.LockRelease(ctx, releaseArgs)
	assert.NoError(t, err)
	assert.Equal(t, pb.Status_SUCCESS, resp1.Status)

	// Duplicate release request with same request ID
	resp2, err := s.LockRelease(ctx, releaseArgs)
	assert.NoError(t, err)
	assert.Equal(t, pb.Status_SUCCESS, resp2.Status)

	// The response should be cached, so both should be identical
	assert.Equal(t, resp1, resp2)
}

// TestCacheExpiration verifies that expired cache entries are removed
func TestCacheExpiration(t *testing.T) {
	cache := NewRequestCache(100 * time.Millisecond) // Very short TTL for testing

	requestID := "test-1"
	resp := &pb.Response{Status: pb.Status_SUCCESS}

	// Add to cache
	cache.Set(requestID, resp)

	// Immediately retrievable
	cachedResp, exists := cache.Get(requestID)
	assert.True(t, exists)
	assert.Equal(t, resp, cachedResp)

	// Wait for expiration
	time.Sleep(200 * time.Millisecond)

	// Should be gone now
	_, exists = cache.Get(requestID)
	assert.False(t, exists)
}
