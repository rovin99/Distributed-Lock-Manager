package replication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockLogger implements the Logger interface for testing
type mockLogger struct {
	t *testing.T
}

func (m *mockLogger) Infof(format string, args ...interface{})  { m.t.Logf("INFO: "+format, args...) }
func (m *mockLogger) Warnf(format string, args ...interface{})  { m.t.Logf("WARN: "+format, args...) }
func (m *mockLogger) Errorf(format string, args ...interface{}) { m.t.Logf("ERROR: "+format, args...) }
func (m *mockLogger) Debugf(format string, args ...interface{}) { m.t.Logf("DEBUG: "+format, args...) }

func TestNewReplicationProtocol(t *testing.T) {
	logger := &mockLogger{t: t}
	protocol := NewReplicationProtocol(logger)
	require.NotNil(t, protocol)
	assert.NotNil(t, protocol.peers)
	assert.NotNil(t, protocol.preparedOperations)
	assert.NotNil(t, protocol.committedOperations)
	assert.NotNil(t, protocol.operationTimeouts)
}

func TestPrepareCommitAbort(t *testing.T) {
	logger := &mockLogger{t: t}
	protocol := NewReplicationProtocol(logger)
	ctx := context.Background()

	// Test prepare
	req := &ReplicationRequest{
		OperationType: OperationPrepare,
		ClientID:      "client1",
		LockID:        "lock1",
		Timestamp:     time.Now().UnixNano(),
	}

	resp, err := protocol.Prepare(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Test commit
	req.OperationType = OperationCommit
	resp, err = protocol.Commit(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Test abort on non-prepared operation
	req.OperationType = OperationAbort
	resp, err = protocol.Abort(ctx, req)
	require.Error(t, err)
	assert.False(t, resp.Success)
}

func TestDuplicatePrepare(t *testing.T) {
	logger := &mockLogger{t: t}
	protocol := NewReplicationProtocol(logger)
	ctx := context.Background()

	req := &ReplicationRequest{
		OperationType: OperationPrepare,
		ClientID:      "client1",
		LockID:        "lock1",
		Timestamp:     time.Now().UnixNano(),
	}

	// First prepare
	resp, err := protocol.Prepare(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Second prepare should succeed (idempotent)
	resp, err = protocol.Prepare(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestOperationTimeout(t *testing.T) {
	logger := &mockLogger{t: t}
	protocol := NewReplicationProtocol(logger)
	ctx := context.Background()

	req := &ReplicationRequest{
		OperationType: OperationPrepare,
		ClientID:      "client1",
		LockID:        "lock1",
		Timestamp:     time.Now().UnixNano(),
	}

	// Prepare operation
	resp, err := protocol.Prepare(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Wait for timeout
	time.Sleep(31 * time.Second)

	// Operation should be aborted
	opType, exists := protocol.GetOperationStatus(req.LockID)
	assert.False(t, exists)
	assert.Empty(t, opType)
}

func TestReplicateState(t *testing.T) {
	logger := &mockLogger{t: t}
	protocol := NewReplicationProtocol(logger)
	ctx := context.Background()

	req := &ReplicationRequest{
		OperationType: OperationPrepare,
		ClientID:      "client1",
		LockID:        "lock1",
		Timestamp:     time.Now().UnixNano(),
	}

	// Test successful replication
	resp, err := protocol.ReplicateState(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Verify operation is committed
	opType, exists := protocol.GetOperationStatus(req.LockID)
	assert.True(t, exists)
	assert.Equal(t, OperationCommit, opType)
}

func TestCleanupOperation(t *testing.T) {
	logger := &mockLogger{t: t}
	protocol := NewReplicationProtocol(logger)
	ctx := context.Background()

	req := &ReplicationRequest{
		OperationType: OperationPrepare,
		ClientID:      "client1",
		LockID:        "lock1",
		Timestamp:     time.Now().UnixNano(),
	}

	// Prepare operation
	resp, err := protocol.Prepare(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Cleanup operation
	protocol.CleanupOperation(req.LockID)

	// Operation should be removed
	opType, exists := protocol.GetOperationStatus(req.LockID)
	assert.False(t, exists)
	assert.Empty(t, opType)
}
