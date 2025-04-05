package client

import (
	"context"
	"errors"
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

// TestClientRetryLogic tests the client's retry mechanism for handling network failures
func TestClientRetryLogic(t *testing.T) {
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

	t.Run("Server unavailable error handling", func(t *testing.T) {
		// Set up mock to simulate server unavailable
		mockClient.On("LockRelease", mock.Anything, mock.Anything).
			Return(nil, errors.New("connection refused")).Times(maxRetries)

		// Try to release lock when server is unavailable
		err := lc.LockRelease()

		// Verify error and client state
		assert.Error(t, err)
		assert.True(t, IsServerUnavailable(err))

		// Client should assume lock is released when server is unavailable
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
