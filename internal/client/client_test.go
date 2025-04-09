package client

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	pb "Distributed-Lock-Manager/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

// MockLockServiceClient is a mock for the LockServiceClient interface
type MockLockServiceClient struct {
	mock.Mock
}

func (m *MockLockServiceClient) ClientInit(ctx context.Context, in *pb.ClientInitArgs, opts ...grpc.CallOption) (*pb.ClientInitResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*pb.ClientInitResponse), args.Error(1)
}

func (m *MockLockServiceClient) LockAcquire(ctx context.Context, in *pb.LockArgs, opts ...grpc.CallOption) (*pb.LockResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.LockResponse), args.Error(1)
}

func (m *MockLockServiceClient) LockRelease(ctx context.Context, in *pb.LockArgs, opts ...grpc.CallOption) (*pb.LockResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.LockResponse), args.Error(1)
}

func (m *MockLockServiceClient) FileAppend(ctx context.Context, in *pb.FileArgs, opts ...grpc.CallOption) (*pb.FileResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.FileResponse), args.Error(1)
}

func (m *MockLockServiceClient) RenewLease(ctx context.Context, in *pb.LeaseArgs, opts ...grpc.CallOption) (*pb.LeaseResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.LeaseResponse), args.Error(1)
}

func (m *MockLockServiceClient) UpdateSecondaryState(ctx context.Context, in *pb.ReplicatedState, opts ...grpc.CallOption) (*pb.ReplicationResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.ReplicationResponse), args.Error(1)
}

func (m *MockLockServiceClient) Ping(ctx context.Context, in *pb.HeartbeatRequest, opts ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.HeartbeatResponse), args.Error(1)
}

func (m *MockLockServiceClient) ServerInfo(ctx context.Context, in *pb.ServerInfoRequest, opts ...grpc.CallOption) (*pb.ServerInfoResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.ServerInfoResponse), args.Error(1)
}

func (m *MockLockServiceClient) VerifyFileAccess(ctx context.Context, in *pb.FileAccessRequest, opts ...grpc.CallOption) (*pb.FileAccessResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.FileAccessResponse), args.Error(1)
}

// testRetryRPC is a version of retryRPC with shorter timeouts for testing
func testRetryRPC(lc *LockClient, operation string, rpcFunc func(context.Context) (*pb.LockResponse, error)) (*pb.LockResponse, error) {
	var lastErr error
	backoff := 5 * time.Millisecond // Much shorter backoff for tests

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Create a context with short timeout for this attempt
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)

		// Execute the RPC
		resp, err := rpcFunc(ctx)

		// Check for success
		if err == nil && resp.Status == pb.Status_OK {
			cancel() // Remember to cancel the context on success
			return resp, nil
		}

		// Handle token validation errors
		if err == nil && (resp.Status == pb.Status_INVALID_TOKEN || resp.Status == pb.Status_PERMISSION_DENIED) {
			cancel()
			lc.invalidateLock() // Clear lock state
			return resp, &InvalidTokenError{Message: resp.ErrorMessage}
		}

		// For other errors, back off and retry
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("server returned status: %v, message: %s", resp.Status, resp.ErrorMessage)
		}

		fmt.Printf("Attempt %d failed: %v, retrying quickly...\n", attempt+1, lastErr)
		cancel()
		time.Sleep(backoff)
		backoff *= 2 // Exponential backoff
	}

	// All retries failed
	lc.invalidateLock() // Clear lock state on persistent failure
	return nil, &ServerUnavailableError{
		Operation: operation,
		Attempts:  maxRetries,
		LastError: lastErr,
	}
}

// skipTest is a helper function to conditionally skip long tests
func skipTest(t *testing.T, message string) {
	if testing.Short() {
		t.Skip(message)
	}
}

// TestClientRetryLogicShort tests retry logic with fewer iterations for faster tests
func TestClientRetryLogicShort(t *testing.T) {
	// Set up a mock client
	mockClient := new(MockLockServiceClient)
	lc := &LockClient{
		client:         mockClient,
		id:             1,
		sequenceNumber: 0,
		clientUUID:     "test-uuid",
		lockToken:      "",
		hasLock:        false,
		renewalActive:  false,
		cancelRenewal:  make(chan struct{}),
		// Add server addresses to avoid divide by zero
		serverAddrs:      []string{"server1:50051", "server2:50051", "server3:50051"},
		currentServerIdx: 0,
	}

	t.Run("Direct token error handling", func(t *testing.T) {
		// Reset client state
		lc.hasLock = true
		lc.lockToken = "invalid-token"

		// Set up the mock to return invalid token error without retry
		mockClient.On("FileAppend", mock.Anything, mock.Anything).
			Return(&pb.FileResponse{
				Status: pb.Status_INVALID_TOKEN,
			}, nil).Once()

		// Execute file append operation
		err := lc.FileAppend("test.txt", []byte("test data"))

		// Verify error is InvalidTokenError and client state is updated
		assert.Error(t, err)
		assert.True(t, IsInvalidToken(err))
		assert.False(t, lc.hasLock)
		assert.Equal(t, "", lc.lockToken)
		mockClient.AssertExpectations(t)
	})

	t.Run("Direct success response", func(t *testing.T) {
		// Reset client state
		lc.hasLock = false
		lc.lockToken = ""

		// Set up the mock to return success without retry
		mockClient.On("LockAcquire", mock.Anything, mock.Anything).
			Return(&pb.LockResponse{
				Status: pb.Status_OK,
				Token:  "test-token",
			}, nil).Once()

		// Execute the lock acquire operation
		err := lc.LockAcquire()

		// Verify expectations
		assert.NoError(t, err)
		assert.True(t, lc.hasLock)
		assert.Equal(t, "test-token", lc.lockToken)
		mockClient.AssertExpectations(t)
	})

	t.Run("Error response handling", func(t *testing.T) {
		// Reset client state
		lc.hasLock = false
		lc.lockToken = ""

		// Set up a new mock for multiple calls
		newMockClient := new(MockLockServiceClient)
		lc.client = newMockClient

		// Mock a non-retriable error response with multiple attempts
		// We need to set up enough mocks to cover all retry attempts
		for i := 0; i < maxRetries; i++ {
			newMockClient.On("LockAcquire", mock.Anything, mock.Anything).
				Return(&pb.LockResponse{
					Status:       pb.Status_ERROR,
					ErrorMessage: "Invalid request",
				}, nil).Once()
		}

		// Execute the lock acquire operation
		err := lc.LockAcquire()

		// Verify expectations - should get an error after max retries
		assert.Error(t, err)
		assert.True(t, IsServerUnavailable(err), "Should be a ServerUnavailableError after max retries")
		assert.Contains(t, err.Error(), "Invalid request")
		newMockClient.AssertExpectations(t)
	})
}

// TestClientRetryLogic contains the original tests that might take longer
func TestClientRetryLogic(t *testing.T) {
	skipTest(t, "Skipping long-running retry tests")

	// Set up a mock client
	mockClient := new(MockLockServiceClient)
	lc := &LockClient{
		client:         mockClient,
		id:             1,
		sequenceNumber: 0,
		clientUUID:     "test-uuid",
		lockToken:      "",
		hasLock:        false,
		renewalActive:  false,
		cancelRenewal:  make(chan struct{}),
	}

	t.Run("Retry on network failure", func(t *testing.T) {
		// Set up the mock to fail on first call and succeed on second
		mockClient.On("LockAcquire", mock.Anything, mock.Anything).
			Return(nil, context.DeadlineExceeded).Once()

		mockClient.On("LockAcquire", mock.Anything, mock.Anything).
			Return(&pb.LockResponse{
				Status: pb.Status_OK,
				Token:  "test-token",
			}, nil).Once()

		// Execute the lock acquire operation
		err := lc.LockAcquire()

		// Verify expectations
		assert.NoError(t, err)
		assert.True(t, lc.hasLock)
		assert.Equal(t, "test-token", lc.lockToken)
		mockClient.AssertExpectations(t)
	})

	t.Run("Max retries exceeded", func(t *testing.T) {
		// Reset client state
		lc.hasLock = false
		lc.lockToken = ""

		// Set up the mock to fail all attempts
		for i := 0; i < maxRetries; i++ {
			mockClient.On("LockAcquire", mock.Anything, mock.Anything).
				Return(nil, context.DeadlineExceeded).Once()
		}

		// Execute the lock acquire operation
		err := lc.LockAcquire()

		// Verify error is ServerUnavailableError
		assert.Error(t, err)
		assert.True(t, IsServerUnavailable(err))
		mockClient.AssertExpectations(t)
	})

	t.Run("Invalid token error", func(t *testing.T) {
		// Reset client state
		lc.hasLock = true
		lc.lockToken = "invalid-token"

		// Set up the mock to return invalid token error
		mockClient.On("FileAppend", mock.Anything, mock.Anything).
			Return(&pb.FileResponse{
				Status: pb.Status_INVALID_TOKEN,
			}, nil).Once()

		// Execute file append operation
		err := lc.FileAppend("test.txt", []byte("test data"))

		// Verify error is InvalidTokenError and client state is updated
		assert.Error(t, err)
		assert.True(t, IsInvalidToken(err))
		assert.False(t, lc.hasLock)
		assert.Equal(t, "", lc.lockToken)
		mockClient.AssertExpectations(t)
	})
}

// TestLeaseRenewal tests the client's lease renewal behavior
func TestLeaseRenewal(t *testing.T) {
	skipTest(t, "Skipping long-running lease renewal tests")

	// Set up a mock client
	mockClient := new(MockLockServiceClient)
	lc := &LockClient{
		client:         mockClient,
		id:             1,
		sequenceNumber: 0,
		clientUUID:     "test-uuid",
		lockToken:      "test-token",
		hasLock:        true,
		renewalActive:  false,
		cancelRenewal:  make(chan struct{}),
	}

	// Make lease renewal interval shorter for testing
	oldLeaseRenewalInt := leaseRenewalInt
	leaseRenewalInt = 10 * time.Millisecond
	defer func() { leaseRenewalInt = oldLeaseRenewalInt }()

	t.Run("Successful lease renewal", func(t *testing.T) {
		// Set up the mock to handle lease renewal
		mockClient.On("RenewLease", mock.Anything, mock.Anything).
			Return(&pb.LeaseResponse{
				Status: pb.Status_OK,
			}, nil).Times(2) // Expect at least 2 renewals

		// Start lease renewal
		lc.startLeaseRenewal()

		// Wait for renewals to occur
		time.Sleep(25 * time.Millisecond)

		// Stop renewal
		lc.stopLeaseRenewal()

		// Verify that client still has lock
		assert.True(t, lc.hasLock)
		assert.Equal(t, "test-token", lc.lockToken)
		mockClient.AssertExpectations(t)
	})

	t.Run("Failed lease renewal", func(t *testing.T) {
		// Reset client state
		lc.hasLock = true
		lc.lockToken = "test-token"
		lc.renewalActive = false

		// Set up the mock to return invalid token error on renewal
		mockClient.On("RenewLease", mock.Anything, mock.Anything).
			Return(&pb.LeaseResponse{
				Status: pb.Status_INVALID_TOKEN,
			}, nil).Once()

		// Start lease renewal
		lc.startLeaseRenewal()

		// Wait for renewal to occur and fail
		time.Sleep(25 * time.Millisecond)

		// Verify that client has released lock
		assert.False(t, lc.hasLock)
		assert.Equal(t, "", lc.lockToken)
		mockClient.AssertExpectations(t)
	})
}

// TestErrorHandling tests how client handles specific errors
func TestErrorHandling(t *testing.T) {
	skipTest(t, "Skipping long-running error handling tests")

	// Set up a mock client
	mockClient := new(MockLockServiceClient)
	lc := &LockClient{
		client:         mockClient,
		id:             1,
		sequenceNumber: 0,
		clientUUID:     "test-uuid",
		lockToken:      "test-token",
		hasLock:        true,
		renewalActive:  false,
		cancelRenewal:  make(chan struct{}),
		// Add server addresses to prevent divide by zero in tryNextServer
		serverAddrs:      []string{"server1:50051", "server2:50051"},
		currentServerIdx: 0,
	}

	t.Run("Server unavailable error handling", func(t *testing.T) {
		// Mock the gRPC call directly without causing tryNextServer to be called
		// This will avoid the divide by zero error
		mockClient.On("LockRelease", mock.Anything, mock.Anything).
			Return(nil, errors.New("connection refused")).Once()

		// Make the client think it already tried all servers
		lc.currentServerIdx = len(lc.serverAddrs) - 1

		// Try to release lock when server is unavailable
		err := lc.LockRelease()

		// Verify error and client state
		assert.Error(t, err)
		assert.False(t, lc.hasLock)
		assert.Equal(t, "", lc.lockToken)
		mockClient.AssertExpectations(t)
	})
}

// TestRequestDeduplication tests that clients generate unique request IDs
func TestRequestDeduplication(t *testing.T) {
	lc := &LockClient{
		id:             1,
		sequenceNumber: 0,
		clientUUID:     "test-uuid",
	}

	// Generate multiple request IDs
	id1 := lc.GenerateRequestID()
	id2 := lc.GenerateRequestID()
	id3 := lc.GenerateRequestID()

	// Verify they're all different
	assert.NotEqual(t, id1, id2)
	assert.NotEqual(t, id2, id3)
	assert.NotEqual(t, id1, id3)
}

// TestServerFailover tests the client's ability to switch between servers when one fails
func TestServerFailover(t *testing.T) {
	skipTest(t, "Skipping long-running server failover tests")

	// Set up a mock client
	mockClient := new(MockLockServiceClient)

	t.Run("Failover on server failure", func(t *testing.T) {
		// Create a client for this test
		testClient := &LockClient{
			client:           mockClient,
			id:               1,
			sequenceNumber:   0,
			clientUUID:       "test-uuid",
			lockToken:        "",
			hasLock:          false,
			renewalActive:    false,
			cancelRenewal:    make(chan struct{}),
			serverAddrs:      []string{"server1:50051", "server2:50051", "server3:50051"},
			currentServerIdx: 0,
		}

		// Mock the LockAcquire RPC to simulate server connection error
		mockClient.On("LockAcquire", mock.Anything, mock.Anything).
			Return(nil, errors.New("connection refused")).Once()

		// Mock the LockAcquire RPC for the successful retry after server switch
		mockClient.On("LockAcquire", mock.Anything, mock.Anything).
			Return(&pb.LockResponse{
				Status: pb.Status_OK,
				Token:  "test-token",
			}, nil).Once()

		// We can't directly test tryNextServer as it's not exportable,
		// but we can track the effect of server switching by:
		// 1. Making the first server connection fail
		// 2. Making the second succeed
		// 3. Verifying the client gets the lock despite the first failure

		// Execute lock acquire operation
		err := testClient.LockAcquire()

		// Verify expectations
		assert.NoError(t, err)
		assert.True(t, testClient.hasLock)
		assert.Equal(t, "test-token", testClient.lockToken)
		mockClient.AssertExpectations(t)
	})
}

// TestClientInit tests the client initialization
func TestClientInit(t *testing.T) {
	// Set up a mock client
	mockClient := new(MockLockServiceClient)
	lc := &LockClient{
		client:         mockClient,
		id:             1,
		sequenceNumber: 0,
		clientUUID:     "test-uuid",
		cancelRenewal:  make(chan struct{}),
	}

	t.Run("Successful client initialization", func(t *testing.T) {
		// Set up the mock to return success for ClientInit
		mockClient.On("ClientInit", mock.Anything, mock.Anything).
			Return(&pb.ClientInitResponse{
				Status: pb.Status_OK,
			}, nil).Once()

		// Execute the client init operation
		err := lc.ClientInit()

		// Verify expectations
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("Client initialization failure", func(t *testing.T) {
		// Create a new mock for this test case
		newMockClient := new(MockLockServiceClient)
		lc.client = newMockClient

		// Set up the mock to handle multiple retries
		for i := 0; i < maxRetries; i++ {
			newMockClient.On("ClientInit", mock.Anything, mock.Anything).
				Return(&pb.ClientInitResponse{
					Status:       pb.Status_ERROR,
					ErrorMessage: "Initialization failed",
				}, nil).Once()
		}

		// Execute the client init operation
		err := lc.ClientInit()

		// Verify expectations
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "client initialization failed")
		newMockClient.AssertExpectations(t)
	})
}

// TestInvalidTokenHandling tests automatic lock invalidation logic across different operations
func TestInvalidTokenHandling(t *testing.T) {
	// Set up a mock client
	mockClient := new(MockLockServiceClient)

	t.Run("FileAppend with invalid token", func(t *testing.T) {
		lc := &LockClient{
			client:         mockClient,
			id:             1,
			sequenceNumber: 0,
			clientUUID:     "test-uuid",
			lockToken:      "expired-token",
			hasLock:        true,
			renewalActive:  false,
			cancelRenewal:  make(chan struct{}),
		}

		// Set up the mock to return invalid token error
		mockClient.On("FileAppend", mock.Anything, mock.Anything).
			Return(&pb.FileResponse{
				Status: pb.Status_INVALID_TOKEN,
			}, nil).Once()

		// Try to append to a file with the expired token
		err := lc.FileAppend("file.txt", []byte("test data"))

		// Verify expectations
		assert.Error(t, err)
		assert.True(t, IsInvalidToken(err), "Error should be InvalidTokenError")
		assert.False(t, lc.hasLock, "Client should invalidate its lock")
		assert.Equal(t, "", lc.lockToken, "Token should be cleared")
		mockClient.AssertExpectations(t)
	})

	t.Run("RenewLease with invalid token", func(t *testing.T) {
		lc := &LockClient{
			client:         mockClient,
			id:             1,
			sequenceNumber: 0,
			clientUUID:     "test-uuid",
			lockToken:      "expired-token",
			hasLock:        true,
			renewalActive:  false,
			cancelRenewal:  make(chan struct{}),
		}

		// Set up the mock to return invalid token error
		mockClient.On("RenewLease", mock.Anything, mock.Anything).
			Return(&pb.LeaseResponse{
				Status: pb.Status_INVALID_TOKEN,
			}, nil).Once()

		// Try to renew the expired token
		err := lc.renewLease("expired-token")

		// Verify expectations
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "lease renewal failed")
		assert.False(t, lc.hasLock, "Client should invalidate its lock")
		assert.Equal(t, "", lc.lockToken, "Token should be cleared")
		mockClient.AssertExpectations(t)
	})

	t.Run("LockRelease with invalid token", func(t *testing.T) {
		lc := &LockClient{
			client:         mockClient,
			id:             1,
			sequenceNumber: 0,
			clientUUID:     "test-uuid",
			lockToken:      "expired-token",
			hasLock:        true,
			renewalActive:  false,
			cancelRenewal:  make(chan struct{}),
		}

		// Set up the mock to return invalid token error
		mockClient.On("LockRelease", mock.Anything, mock.Anything).
			Return(&pb.LockResponse{
				Status: pb.Status_INVALID_TOKEN,
			}, nil).Once()

		// Try to release with the expired token
		err := lc.LockRelease()

		// Verify expectations
		assert.Error(t, err)
		assert.True(t, IsInvalidToken(err), "Error should be InvalidTokenError")
		assert.False(t, lc.hasLock, "Client should invalidate its lock")
		assert.Equal(t, "", lc.lockToken, "Token should be cleared")
		mockClient.AssertExpectations(t)
	})
}

// TestErrorTypes tests the custom error types and error checking helpers
func TestErrorTypes(t *testing.T) {
	t.Run("ServerUnavailableError", func(t *testing.T) {
		// Create a ServerUnavailableError
		originalErr := fmt.Errorf("connection refused")
		serverErr := &ServerUnavailableError{
			Operation: "LockAcquire",
			Attempts:  5,
			LastError: originalErr,
		}

		// Check the error message format
		expectedMsg := "server unavailable: LockAcquire operation failed after 5 attempts: connection refused"
		assert.Equal(t, expectedMsg, serverErr.Error())

		// Test the helper function
		assert.True(t, IsServerUnavailable(serverErr))
		assert.False(t, IsServerUnavailable(fmt.Errorf("some other error")))
		assert.False(t, IsServerUnavailable(nil))
	})

	t.Run("InvalidTokenError", func(t *testing.T) {
		// Create an InvalidTokenError
		tokenErr := &InvalidTokenError{
			Message: "token expired",
		}

		// Check the error message format
		expectedMsg := "invalid token: token expired"
		assert.Equal(t, expectedMsg, tokenErr.Error())

		// Test the helper function
		assert.True(t, IsInvalidToken(tokenErr))
		assert.False(t, IsInvalidToken(fmt.Errorf("some other error")))
		assert.False(t, IsInvalidToken(nil))
	})
}

// TestRetryRPCShort tests our faster testRetryRPC implementation
func TestRetryRPCShort(t *testing.T) {
	// Create a client
	mockClient := new(MockLockServiceClient)
	lc := &LockClient{
		client:         mockClient,
		id:             1,
		sequenceNumber: 0,
		clientUUID:     "test-uuid",
		lockToken:      "",
		hasLock:        false,
		renewalActive:  false,
		cancelRenewal:  make(chan struct{}),
	}

	t.Run("Retry succeeds after failure", func(t *testing.T) {
		// Test that we can get a success after a failure
		called := false
		rpcFunc := func(ctx context.Context) (*pb.LockResponse, error) {
			if !called {
				called = true
				return nil, errors.New("first attempt fails")
			}
			return &pb.LockResponse{
				Status: pb.Status_OK,
				Token:  "test-token",
			}, nil
		}

		start := time.Now()
		resp, err := testRetryRPC(lc, "TestOperation", rpcFunc)
		elapsed := time.Since(start)

		// Verify results
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, pb.Status_OK, resp.Status)
		assert.True(t, called, "Function should have been called at least once")

		// Verify we're using much shorter timeouts
		assert.Less(t, elapsed, 100*time.Millisecond, "Should complete quickly with short timeouts")
	})

	t.Run("Max retries results in error", func(t *testing.T) {
		// Test max retries with failure
		attempts := 0
		rpcFunc := func(ctx context.Context) (*pb.LockResponse, error) {
			attempts++
			return nil, errors.New("always fails")
		}

		start := time.Now()
		resp, err := testRetryRPC(lc, "TestOperation", rpcFunc)
		elapsed := time.Since(start)

		// Verify results
		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, maxRetries, attempts, "Should retry exactly maxRetries times")
		assert.True(t, IsServerUnavailable(err), "Should return ServerUnavailableError")

		// Verify we're using much shorter timeouts
		assert.Less(t, elapsed, 200*time.Millisecond, "Should complete quickly with short timeouts")
	})

	t.Run("Invalid token error handling", func(t *testing.T) {
		// Test token validation error
		rpcFunc := func(ctx context.Context) (*pb.LockResponse, error) {
			return &pb.LockResponse{
				Status:       pb.Status_INVALID_TOKEN,
				ErrorMessage: "token expired",
			}, nil
		}

		// Set client as having a lock
		lc.hasLock = true
		lc.lockToken = "expired-token"

		resp, err := testRetryRPC(lc, "TestOperation", rpcFunc)

		// Verify results
		assert.Error(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, pb.Status_INVALID_TOKEN, resp.Status)
		assert.True(t, IsInvalidToken(err), "Should return InvalidTokenError")
		assert.False(t, lc.hasLock, "Should invalidate lock")
		assert.Equal(t, "", lc.lockToken, "Should clear token")
	})
}
