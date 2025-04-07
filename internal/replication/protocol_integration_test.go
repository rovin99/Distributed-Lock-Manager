//go:build integration

package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicationProtocolIntegration(t *testing.T) {
	// Create a cluster of 3 servers
	servers := make([]*ReplicationProtocol, 3)
	for i := range servers {
		logger := &mockLogger{t: t}
		servers[i] = NewReplicationProtocol(logger)
	}

	// Connect servers to each other
	for i := range servers {
		for j := range servers {
			if i != j {
				peer := &Peer{
					ID:   fmt.Sprintf("server%d", j),
					Conn: nil, // In real implementation, this would be a gRPC connection
				}
				servers[i].AddPeer(peer)
			}
		}
	}

	// Test prepare phase
	ctx := context.Background()
	req := &ReplicationRequest{
		OperationType: OperationPrepare,
		ClientID:      "client1",
		LockID:        "lock1",
		Timestamp:     time.Now().UnixNano(),
	}

	// Prepare on all servers
	for i, server := range servers {
		resp, err := server.Prepare(ctx, req)
		require.NoError(t, err, "Server %d prepare failed", i)
		assert.True(t, resp.Success, "Server %d prepare response not successful", i)
	}

	// Test commit phase
	req.OperationType = OperationCommit
	for i, server := range servers {
		resp, err := server.Commit(ctx, req)
		require.NoError(t, err, "Server %d commit failed", i)
		assert.True(t, resp.Success, "Server %d commit response not successful", i)
	}

	// Verify state on all servers
	for i, server := range servers {
		opType, exists := server.GetOperationStatus(req.LockID)
		assert.True(t, exists, "Server %d operation not found", i)
		assert.Equal(t, OperationCommit, opType, "Server %d operation not committed", i)
	}
}

func TestReplicationProtocolFailureRecovery(t *testing.T) {
	// Create a cluster of 3 servers
	servers := make([]*ReplicationProtocol, 3)
	for i := range servers {
		logger := &mockLogger{t: t}
		servers[i] = NewReplicationProtocol(logger)
	}

	// Connect servers to each other
	for i := range servers {
		for j := range servers {
			if i != j {
				peer := &Peer{
					ID:   fmt.Sprintf("server%d", j),
					Conn: nil, // In real implementation, this would be a gRPC connection
				}
				servers[i].AddPeer(peer)
			}
		}
	}

	// Test prepare phase with one server failing
	ctx := context.Background()
	req := &ReplicationRequest{
		OperationType: OperationPrepare,
		ClientID:      "client1",
		LockID:        "lock1",
		Timestamp:     time.Now().UnixNano(),
	}

	// Prepare on all servers except one
	for i, server := range servers {
		if i != 1 { // Skip server 1
			resp, err := server.Prepare(ctx, req)
			require.NoError(t, err, "Server %d prepare failed", i)
			assert.True(t, resp.Success, "Server %d prepare response not successful", i)
		}
	}

	// Test commit phase
	req.OperationType = OperationCommit
	for i, server := range servers {
		if i != 1 { // Skip server 1
			resp, err := server.Commit(ctx, req)
			require.NoError(t, err, "Server %d commit failed", i)
			assert.True(t, resp.Success, "Server %d commit response not successful", i)
		}
	}

	// Verify state on all servers except the failed one
	for i, server := range servers {
		if i != 1 { // Skip server 1
			opType, exists := server.GetOperationStatus(req.LockID)
			assert.True(t, exists, "Server %d operation not found", i)
			assert.Equal(t, OperationCommit, opType, "Server %d operation not committed", i)
		}
	}
}

func TestReplicationProtocolTimeout(t *testing.T) {
	// Create a cluster of 3 servers
	servers := make([]*ReplicationProtocol, 3)
	for i := range servers {
		logger := &mockLogger{t: t}
		servers[i] = NewReplicationProtocol(logger)
	}

	// Connect servers to each other
	for i := range servers {
		for j := range servers {
			if i != j {
				peer := &Peer{
					ID:   fmt.Sprintf("server%d", j),
					Conn: nil, // In real implementation, this would be a gRPC connection
				}
				servers[i].AddPeer(peer)
			}
		}
	}

	// Test prepare phase
	ctx := context.Background()
	req := &ReplicationRequest{
		OperationType: OperationPrepare,
		ClientID:      "client1",
		LockID:        "lock1",
		Timestamp:     time.Now().UnixNano(),
	}

	// Prepare on all servers
	for i, server := range servers {
		resp, err := server.Prepare(ctx, req)
		require.NoError(t, err, "Server %d prepare failed", i)
		assert.True(t, resp.Success, "Server %d prepare response not successful", i)
	}

	// Wait for timeout
	time.Sleep(31 * time.Second)

	// Verify operations are aborted on all servers
	for i, server := range servers {
		opType, exists := server.GetOperationStatus(req.LockID)
		assert.False(t, exists, "Server %d operation still exists after timeout", i)
		assert.Empty(t, opType, "Server %d operation type not empty after timeout", i)
	}
}
