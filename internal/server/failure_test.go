package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "Distributed-Lock-Manager/proto"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// TestDelayedAppend tests the scenario where a file append request is delayed
// until after the lock lease has expired and another client has acquired the lock
func TestDelayedAppend(t *testing.T) {
	// Create a LockServer with a very short lease duration for testing
	server := NewLockServer()
	leaseDuration := 1 * time.Second
	server.lockManager.SetLeaseDuration(leaseDuration)

	// Get the original file content for later comparison
	filename := "file_0"
	CreateFiles() // Ensure test files exist
	originalContent, err := server.fileManager.ReadFile(filename)
	assert.NoError(t, err, "Failed to read original file content")

	// Step 1: Client 1 acquires lock
	client1ID := int32(1)
	reqID1 := uuid.New().String()
	lockResp, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client1ID,
		RequestId: reqID1,
	})
	assert.NoError(t, err, "LockAcquire failed")
	assert.Equal(t, pb.Status_OK, lockResp.Status, "Failed to acquire lock")
	token1 := lockResp.Token

	// Step 2: Create a delayed file append request from Client 1
	// We'll create the request but not process it yet
	fileArgs := &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("Delayed Append Content"),
		Token:     token1,
		RequestId: uuid.New().String(),
	}

	// Step 3: Wait for the lease to expire
	time.Sleep(leaseDuration + 500*time.Millisecond) // Add buffer to ensure expiry

	// Step 4: Client 2 acquires the now-free lock
	client2ID := int32(2)
	reqID2 := uuid.New().String()
	lockResp2, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client2ID,
		RequestId: reqID2,
	})
	assert.NoError(t, err, "LockAcquire for client 2 failed")
	assert.Equal(t, pb.Status_OK, lockResp2.Status, "Client 2 failed to acquire lock")

	// Step 5: Now process Client 1's delayed append request
	fileResp, err := server.FileAppend(context.Background(), fileArgs)
	assert.NoError(t, err, "FileAppend RPC failed")

	// Step 6: Assert the append request was rejected due to invalid token
	assert.Equal(t, pb.Status_INVALID_TOKEN, fileResp.Status,
		"FileAppend should have been rejected with INVALID_TOKEN")

	// Step 7: Verify the file content hasn't changed
	newContent, err := server.fileManager.ReadFile(filename)
	assert.NoError(t, err, "Failed to read new file content")
	assert.Equal(t, string(originalContent), string(newContent),
		"File content should not have changed")
}

// TestDuplicateRelease tests the scenario where a client sends duplicate
// lock release requests
func TestDuplicateRelease(t *testing.T) {
	// Create a LockServer
	server := NewLockServer()

	// Step 1: Client 1 acquires lock
	client1ID := int32(1)
	reqID1Acquire := uuid.New().String()
	lockResp, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client1ID,
		RequestId: reqID1Acquire,
	})
	assert.NoError(t, err, "LockAcquire failed")
	assert.Equal(t, pb.Status_OK, lockResp.Status, "Failed to acquire lock")
	token1 := lockResp.Token

	// Step 2: Generate RequestID for release
	reqID1Release := uuid.New().String()

	// Step 3: Create LockRelease request but don't process it yet (simulating delay)
	releaseArgs := &pb.LockArgs{
		ClientId:  client1ID,
		Token:     token1,
		RequestId: reqID1Release,
	}

	// Step 4: Simulate Client 1 retrying LockRelease with the same RequestID
	releaseResp, err := server.LockRelease(context.Background(), releaseArgs)
	assert.NoError(t, err, "LockRelease RPC failed")
	assert.Equal(t, pb.Status_OK, releaseResp.Status, "Initial lock release failed")

	// Step 5: Client 2 acquires the now-free lock
	client2ID := int32(2)
	reqID2Acquire := uuid.New().String()
	lockResp2, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client2ID,
		RequestId: reqID2Acquire,
	})
	assert.NoError(t, err, "LockAcquire for client 2 failed")
	assert.Equal(t, pb.Status_OK, lockResp2.Status, "Client 2 failed to acquire lock")
	token2 := lockResp2.Token

	// Step 6: Now process the "delayed" original LockRelease from Client 1
	releaseResp2, err := server.LockRelease(context.Background(), releaseArgs)
	assert.NoError(t, err, "Delayed LockRelease RPC failed")
	assert.Equal(t, pb.Status_OK, releaseResp2.Status,
		"Duplicate lock release should return the cached success response")

	// Step 7: Verify Client 2 still holds the lock
	hasLock := server.lockManager.HasLockWithToken(client2ID, token2)
	assert.True(t, hasLock, "Client 2 should still hold the lock after duplicate release attempt")

	// Step 8: Further verification - Client 2 should be able to append to file
	fileResp, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client2ID,
		Filename:  "file_0",
		Content:   []byte("Client 2 Content"),
		Token:     token2,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp.Status, "Client 2 should be able to append to file")
}

// TestPacketLossScenario tests the scenario where a client's lock acquire packet is lost
// and it needs to retry acquisition
func TestPacketLossScenario(t *testing.T) {
	// Create a LockServer
	server := NewLockServer()

	// Step 1: Client 1 sends LockAcquire (but we'll simulate it being lost by not processing it)
	client1ID := int32(1)
	reqID1Acquire := uuid.New().String()

	// Step 2: Client 2 acquires the lock
	client2ID := int32(2)
	reqID2Acquire := uuid.New().String()
	lockResp2, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client2ID,
		RequestId: reqID2Acquire,
	})
	assert.NoError(t, err, "LockAcquire for client 2 failed")
	assert.Equal(t, pb.Status_OK, lockResp2.Status, "Client 2 failed to acquire lock")
	token2 := lockResp2.Token

	// Step 3: Client 2 appends to file
	filename := "file_0"
	fileResp, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client2ID,
		Filename:  filename,
		Content:   []byte("B"),
		Token:     token2,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp.Status, "Client 2 should be able to append to file")

	// Step 4: Client 2 releases the lock
	reqID2Release := uuid.New().String()
	releaseResp, err := server.LockRelease(context.Background(), &pb.LockArgs{
		ClientId:  client2ID,
		Token:     token2,
		RequestId: reqID2Release,
	})
	assert.NoError(t, err, "LockRelease RPC failed")
	assert.Equal(t, pb.Status_OK, releaseResp.Status, "Lock release failed")

	// Step 5: Client 1 retries lock acquisition
	lockResp1, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client1ID,
		RequestId: reqID1Acquire,
	})
	assert.NoError(t, err, "LockAcquire retry failed")
	assert.Equal(t, pb.Status_OK, lockResp1.Status, "Client 1 failed to acquire lock on retry")
	token1 := lockResp1.Token

	// Step 6: Client 1 appends to file
	fileResp1, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("A"),
		Token:     token1,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp1.Status, "Client 1 should be able to append to file")

	// Step 7: Verify the file content contains "BA"
	content, err := server.fileManager.ReadFile(filename)
	assert.NoError(t, err, "Failed to read file content")
	assert.Contains(t, string(content), "BA", "File should contain 'BA'")
}

// TestDuplicatedReleasePackets tests the scenario where a client's release request
// is duplicated and the server receives it multiple times
func TestDuplicatedReleasePackets(t *testing.T) {
	// Create a LockServer
	server := NewLockServer()
	filename := "file_0"

	// Step 1: Client 1 acquires lock
	client1ID := int32(1)
	reqID1Acquire := uuid.New().String()
	lockResp1, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client1ID,
		RequestId: reqID1Acquire,
	})
	assert.NoError(t, err, "LockAcquire failed")
	assert.Equal(t, pb.Status_OK, lockResp1.Status, "Failed to acquire lock")
	token1 := lockResp1.Token

	// Step 2: Client 1 appends to file
	fileResp1, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("A"),
		Token:     token1,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp1.Status, "Client 1 should be able to append to file")

	// Step 3: Client 1 sends LockRelease (delayed)
	reqID1Release := uuid.New().String()
	releaseArgs := &pb.LockArgs{
		ClientId:  client1ID,
		Token:     token1,
		RequestId: reqID1Release,
	}

	// Step 4: Client 1 retries LockRelease (with same request ID)
	releaseResp, err := server.LockRelease(context.Background(), releaseArgs)
	assert.NoError(t, err, "LockRelease RPC failed")
	assert.Equal(t, pb.Status_OK, releaseResp.Status, "Lock release failed")

	// Step 5: Client 2 acquires the lock
	client2ID := int32(2)
	reqID2Acquire := uuid.New().String()
	lockResp2, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client2ID,
		RequestId: reqID2Acquire,
	})
	assert.NoError(t, err, "LockAcquire for client 2 failed")
	assert.Equal(t, pb.Status_OK, lockResp2.Status, "Client 2 failed to acquire lock")
	token2 := lockResp2.Token

	// Step 6: Now process the delayed/duplicated LockRelease from Client 1
	releaseResp2, err := server.LockRelease(context.Background(), releaseArgs)
	assert.NoError(t, err, "Duplicated LockRelease RPC failed")
	assert.Equal(t, pb.Status_OK, releaseResp2.Status, "Duplicated lock release should return success")

	// Step 7: Client 2 should still hold the lock - verify with append
	fileResp2, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client2ID,
		Filename:  filename,
		Content:   []byte("B"),
		Token:     token2,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp2.Status, "Client 2 should still be able to append to file")

	// Step 8: Client 2 releases the lock
	reqID2Release := uuid.New().String()
	releaseResp3, err := server.LockRelease(context.Background(), &pb.LockArgs{
		ClientId:  client2ID,
		Token:     token2,
		RequestId: reqID2Release,
	})
	assert.NoError(t, err, "LockRelease RPC failed")
	assert.Equal(t, pb.Status_OK, releaseResp3.Status, "Lock release failed")

	// Step 9: Client 1 acquires the lock again
	reqID1Acquire2 := uuid.New().String()
	lockResp3, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client1ID,
		RequestId: reqID1Acquire2,
	})
	assert.NoError(t, err, "Second LockAcquire for client 1 failed")
	assert.Equal(t, pb.Status_OK, lockResp3.Status, "Client 1 failed to reacquire lock")
	token1New := lockResp3.Token

	// Step 10: Client 1 appends to the file again
	fileResp3, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("A"),
		Token:     token1New,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "Second FileAppend for client 1 failed")
	assert.Equal(t, pb.Status_OK, fileResp3.Status, "Client 1 should be able to append to file again")

	// Step 11: Verify the file content contains "BAABA" pattern
	content, err := server.fileManager.ReadFile(filename)
	assert.NoError(t, err, "Failed to read file content")
	assert.Contains(t, string(content), "BAABA", "File should contain 'BAABA' pattern")
}

// TestIntegratedAppend tests the scenario with packet loss during file appends
// to verify idempotency of file operations
func TestIntegratedAppend(t *testing.T) {
	// Create a LockServer
	server := NewLockServer()
	filename := "file_0"
	CreateFiles() // Ensure test files exist

	// Step 1: Client 1 acquires lock
	client1ID := int32(1)
	reqID1Acquire := uuid.New().String()
	lockResp1, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client1ID,
		RequestId: reqID1Acquire,
	})
	assert.NoError(t, err, "LockAcquire failed")
	assert.Equal(t, pb.Status_OK, lockResp1.Status, "Failed to acquire lock")
	token1 := lockResp1.Token

	// Step 2: Client 1 successfully appends '1'
	reqID_Append1 := uuid.New().String()
	fileResp1, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("1"),
		Token:     token1,
		RequestId: reqID_Append1,
	})
	assert.NoError(t, err, "FileAppend '1' RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp1.Status, "Client 1 should be able to append '1'")

	// Step 3: Client 1 attempts to append 'A' but the request is lost (we're simulating this)
	// We don't actually send a request here - just creating the request ID for later use
	reqID_AppendA := uuid.New().String()

	// Step 4: Client 1 retries appending 'A' (first retry) - request reaches server, but response is lost
	fileRespA1, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("A"),
		Token:     token1,
		RequestId: reqID_AppendA, // Same request ID as the "lost" request
	})
	assert.NoError(t, err, "FileAppend 'A' retry 1 RPC failed")
	assert.Equal(t, pb.Status_OK, fileRespA1.Status, "First retry of append 'A' should succeed")

	// Step 5: Client 1 retries appending 'A' again (second retry) - idempotency should prevent double append
	fileRespA2, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("A"),
		Token:     token1,
		RequestId: reqID_AppendA, // Same request ID again
	})
	assert.NoError(t, err, "FileAppend 'A' retry 2 RPC failed")
	assert.Equal(t, pb.Status_OK, fileRespA2.Status, "Second retry of append 'A' should return success from cache")

	// Step 6: Client 1 releases the lock
	reqID1Release := uuid.New().String()
	releaseResp, err := server.LockRelease(context.Background(), &pb.LockArgs{
		ClientId:  client1ID,
		Token:     token1,
		RequestId: reqID1Release,
	})
	assert.NoError(t, err, "LockRelease RPC failed")
	assert.Equal(t, pb.Status_OK, releaseResp.Status, "Client 1 lock release failed")

	// Step 7: Client 2 acquires the lock
	client2ID := int32(2)
	reqID2Acquire := uuid.New().String()
	lockResp2, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client2ID,
		RequestId: reqID2Acquire,
	})
	assert.NoError(t, err, "LockAcquire for client 2 failed")
	assert.Equal(t, pb.Status_OK, lockResp2.Status, "Client 2 failed to acquire lock")
	token2 := lockResp2.Token

	// Step 8: Client 2 appends 'B'
	reqID_AppendB := uuid.New().String()
	fileRespB, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client2ID,
		Filename:  filename,
		Content:   []byte("B"),
		Token:     token2,
		RequestId: reqID_AppendB,
	})
	assert.NoError(t, err, "FileAppend 'B' RPC failed")
	assert.Equal(t, pb.Status_OK, fileRespB.Status, "Client 2 should be able to append 'B'")

	// Step 9: Verify final file content is "1AB"
	content, err := server.fileManager.ReadFile(filename)
	assert.NoError(t, err, "Failed to read file content")
	assert.Contains(t, string(content), "1AB", "File should contain '1AB' pattern")

	// Optional Step 10: Examine the WAL file or processed requests log to verify that
	// the request with ID reqID_AppendA was only processed once
	// This would require additional test APIs to expose internal state
}

// TestClientStuckBeforeEditing tests the scenario where a client acquires a lock
// but gets stuck (e.g., due to GC pause) before editing the file,
// and its lease expires, allowing another client to take over
func TestClientStuckBeforeEditing(t *testing.T) {
	// Create a LockServer with a very short lease duration for testing
	CreateFiles() // Ensure test files exist before creating server
	server := NewLockServer()
	leaseDuration := 1 * time.Second
	server.lockManager.SetLeaseDuration(leaseDuration)

	// Reset the test file to ensure clean state
	filename := "file_0"
	filePath := filepath.Join("data", filename)
	err := os.WriteFile(filePath, []byte(""), 0644)
	assert.NoError(t, err, "Failed to reset test file")

	// Step 1: Client 1 acquires lock
	client1ID := int32(1)
	reqID1Acquire := uuid.New().String()
	lockResp1, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client1ID,
		RequestId: reqID1Acquire,
	})
	assert.NoError(t, err, "LockAcquire failed")
	assert.Equal(t, pb.Status_OK, lockResp1.Status, "Failed to acquire lock")
	token1 := lockResp1.Token

	// Step 2: Client 1 pauses (simulating GC pause) - longer than lease duration
	time.Sleep(leaseDuration + 500*time.Millisecond) // Add buffer to ensure expiry

	// Step 3: Client 2 acquires the now-free lock (since C1's lease expired)
	client2ID := int32(2)
	reqID2Acquire := uuid.New().String()
	lockResp2, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client2ID,
		RequestId: reqID2Acquire,
	})
	assert.NoError(t, err, "LockAcquire for client 2 failed")
	assert.Equal(t, pb.Status_OK, lockResp2.Status, "Client 2 failed to acquire lock")
	token2 := lockResp2.Token

	// Step 4: Client 2 appends 'B' to the file
	fileResp2_1, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client2ID,
		Filename:  filename,
		Content:   []byte("B"),
		Token:     token2,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp2_1.Status, "Client 2 should be able to append to file")

	// Step 5: Client 1 resumes and attempts to append 'A' using its now invalid token
	fileResp1, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("A"),
		Token:     token1,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_INVALID_TOKEN, fileResp1.Status,
		"Client 1's append with expired token should be rejected with INVALID_TOKEN")

	// Step 6: Client 2 appends 'B' again
	fileResp2_2, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client2ID,
		Filename:  filename,
		Content:   []byte("B"),
		Token:     token2,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp2_2.Status, "Client 2 should still be able to append to file")

	// Step 7: Client 2 releases lock
	releaseResp2, err := server.LockRelease(context.Background(), &pb.LockArgs{
		ClientId:  client2ID,
		Token:     token2,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "LockRelease RPC failed")
	assert.Equal(t, pb.Status_OK, releaseResp2.Status, "Client 2 lock release failed")

	// Step 8: Client 1 must re-acquire the lock
	reqID1Acquire2 := uuid.New().String()
	lockResp1_2, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client1ID,
		RequestId: reqID1Acquire2,
	})
	assert.NoError(t, err, "LockAcquire retry failed")
	assert.Equal(t, pb.Status_OK, lockResp1_2.Status, "Client 1 failed to re-acquire lock")
	token1_new := lockResp1_2.Token
	assert.NotEqual(t, token1, token1_new, "Client 1 should get a new token")

	// Step 9: Client 1 appends 'A' twice with the new token
	fileResp1_1, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("A"),
		Token:     token1_new,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp1_1.Status, "Client 1 should be able to append to file with new token")

	fileResp1_2, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("A"),
		Token:     token1_new,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp1_2.Status, "Client 1 should be able to append to file again")

	// Final cleanup - release the lock
	releaseResp1, err := server.LockRelease(context.Background(), &pb.LockArgs{
		ClientId:  client1ID,
		Token:     token1_new,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "LockRelease RPC failed")
	assert.Equal(t, pb.Status_OK, releaseResp1.Status, "Client 1 lock release failed")

	// Step 10: Verify final file content shows "BB" from Client 2 and "AA" from Client 1's retried work
	finalContent, err := server.fileManager.ReadFile(filename)
	assert.NoError(t, err, "Failed to read final file content")
	assert.Equal(t, "BBAA", string(finalContent), "File should contain the pattern 'BBAA'")
}

// TestClientStuckAfterEditing tests the scenario where a client acquires a lock
// and performs a partial edit, then gets stuck (e.g., due to GC pause),
// and its lease expires, allowing another client to take over
func TestClientStuckAfterEditing(t *testing.T) {
	// Create a LockServer with a very short lease duration for testing
	CreateFiles() // Ensure test files exist before creating server
	server := NewLockServer()
	leaseDuration := 1 * time.Second
	server.lockManager.SetLeaseDuration(leaseDuration)

	// Reset the test file to ensure clean state
	filename := "file_0"
	filePath := filepath.Join("data", filename)
	err := os.WriteFile(filePath, []byte(""), 0644)
	assert.NoError(t, err, "Failed to reset test file")

	// Step 1: Client 1 acquires lock
	client1ID := int32(1)
	reqID1Acquire := uuid.New().String()
	lockResp1, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client1ID,
		RequestId: reqID1Acquire,
	})
	assert.NoError(t, err, "LockAcquire failed")
	assert.Equal(t, pb.Status_OK, lockResp1.Status, "Failed to acquire lock")
	token1 := lockResp1.Token

	// Step 2: Client 1 appends 'A' (first part of its critical section)
	fileResp1_1, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("A"),
		Token:     token1,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp1_1.Status, "Client 1 should be able to append to file")

	// Step 3: Client 1 pauses (simulating GC pause) - longer than lease duration
	time.Sleep(leaseDuration + 500*time.Millisecond) // Add buffer to ensure expiry

	// Step 4: Client 2 acquires the now-free lock (since C1's lease expired)
	client2ID := int32(2)
	reqID2Acquire := uuid.New().String()
	lockResp2, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client2ID,
		RequestId: reqID2Acquire,
	})
	assert.NoError(t, err, "LockAcquire for client 2 failed")
	assert.Equal(t, pb.Status_OK, lockResp2.Status, "Client 2 failed to acquire lock")
	token2 := lockResp2.Token

	// Step 5: Client 2 appends 'B'
	fileResp2_1, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client2ID,
		Filename:  filename,
		Content:   []byte("B"),
		Token:     token2,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp2_1.Status, "Client 2 should be able to append to file")

	// Step 6: Client 1 resumes and attempts to append 'A' (second part) using its now invalid token
	fileResp1_2, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("A"),
		Token:     token1,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_INVALID_TOKEN, fileResp1_2.Status,
		"Client 1's append with expired token should be rejected with INVALID_TOKEN")

	// Step 7: Client 2 appends 'B' again
	fileResp2_2, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client2ID,
		Filename:  filename,
		Content:   []byte("B"),
		Token:     token2,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp2_2.Status, "Client 2 should still be able to append to file")

	// Step 8: Client 2 releases lock
	releaseResp2, err := server.LockRelease(context.Background(), &pb.LockArgs{
		ClientId:  client2ID,
		Token:     token2,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "LockRelease RPC failed")
	assert.Equal(t, pb.Status_OK, releaseResp2.Status, "Client 2 lock release failed")

	// Step 9: Client 1 must re-acquire the lock and retry its entire critical section
	reqID1Acquire2 := uuid.New().String()
	lockResp1_2, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client1ID,
		RequestId: reqID1Acquire2,
	})
	assert.NoError(t, err, "LockAcquire retry failed")
	assert.Equal(t, pb.Status_OK, lockResp1_2.Status, "Client 1 failed to re-acquire lock")
	token1_new := lockResp1_2.Token
	assert.NotEqual(t, token1, token1_new, "Client 1 should get a new token")

	// Step 10: Client 1 retries its entire critical section (two 'A' appends) with the new token
	fileResp1_3, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("A"),
		Token:     token1_new,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp1_3.Status, "Client 1 should be able to append to file with new token")

	fileResp1_4, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("A"),
		Token:     token1_new,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp1_4.Status, "Client 1 should be able to append to file again")

	// Final cleanup - release the lock
	releaseResp1, err := server.LockRelease(context.Background(), &pb.LockArgs{
		ClientId:  client1ID,
		Token:     token1_new,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "LockRelease RPC failed")
	assert.Equal(t, pb.Status_OK, releaseResp1.Status, "Client 1 lock release failed")

	// Step 11: Read and verify the final file content
	finalContent, err := server.fileManager.ReadFile(filename)
	assert.NoError(t, err, "Failed to read final file content")

	// If the server implements content rollback on lease expiry, file should contain "BBAA"
	// If not, it should contain "ABBAA"
	actualContent := string(finalContent)

	// Check and print the content for debugging
	t.Logf("Final file content is: %s", actualContent)

	if actualContent != "BBAA" && actualContent != "ABBAA" {
		// The server didn't implement either expected behavior
		t.Errorf("File content '%s' doesn't match either expected behavior ('BBAA' or 'ABBAA')", actualContent)
	}

	// We'll do a simple string check rather than using Contains()
	// since we're resetting the file at the start of the test
	if len(actualContent) == 4 {
		assert.Equal(t, "BBAA", actualContent, "File content matches server-rollback implementation")
	} else if len(actualContent) == 5 {
		assert.Equal(t, "ABBAA", actualContent, "File content matches client-retry implementation (no server rollback)")
	}
}

// TestDuplicateReleaseFullSequence implements the exact sequence for Test Case 1c:
// C1 acquires -> C1 appends 'A' -> C1 sends release (ReqID R1, delayed) -> C1 retries release (ReqID R1, processed)
// -> C2 acquires -> Delayed C1 release (ReqID R1) arrives -> Server ignores duplicate -> C2 appends 'B'
// -> C2 appends 'B' -> C2 releases -> C1 acquires -> C1 appends 'A' -> C1 appends 'A'
// Final file content should be "ABBAA"
func TestDuplicateReleaseFullSequence(t *testing.T) {
	// Create a LockServer
	server := NewLockServer()
	filename := "file_0"

	// Ensure clean test file
	CreateFiles() // Ensure test files exist
	filePath := filepath.Join("data", filename)
	err := os.WriteFile(filePath, []byte(""), 0644)
	assert.NoError(t, err, "Failed to reset test file")

	// Step 1: Client 1 acquires lock
	client1ID := int32(1)
	reqID1Acquire := uuid.New().String()
	lockResp1, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client1ID,
		RequestId: reqID1Acquire,
	})
	assert.NoError(t, err, "LockAcquire failed")
	assert.Equal(t, pb.Status_OK, lockResp1.Status, "Failed to acquire lock")
	token1 := lockResp1.Token

	// Step 2: Client 1 appends 'A'
	fileResp1, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("A"),
		Token:     token1,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp1.Status, "Client 1 should be able to append to file")

	// Step 3: Client 1 sends LockRelease (delayed) with RequestID R1
	reqID1Release := uuid.New().String() // This is R1
	releaseArgs := &pb.LockArgs{
		ClientId:  client1ID,
		Token:     token1,
		RequestId: reqID1Release,
	}

	// Step 4: Client 1 retries LockRelease (with same request ID R1) - this one gets processed
	releaseResp, err := server.LockRelease(context.Background(), releaseArgs)
	assert.NoError(t, err, "LockRelease RPC failed")
	assert.Equal(t, pb.Status_OK, releaseResp.Status, "Lock release failed")

	// Step 5: Client 2 acquires the lock
	client2ID := int32(2)
	reqID2Acquire := uuid.New().String()
	lockResp2, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client2ID,
		RequestId: reqID2Acquire,
	})
	assert.NoError(t, err, "LockAcquire for client 2 failed")
	assert.Equal(t, pb.Status_OK, lockResp2.Status, "Client 2 failed to acquire lock")
	token2 := lockResp2.Token

	// Step 6: Process the delayed/duplicated LockRelease from Client 1 (same RequestID R1)
	releaseResp2, err := server.LockRelease(context.Background(), releaseArgs)
	assert.NoError(t, err, "Duplicated LockRelease RPC failed")
	assert.Equal(t, pb.Status_OK, releaseResp2.Status, "Duplicated lock release should return success from cache")

	// Step 7: Verify Client 2 still holds the lock
	hasLock := server.lockManager.HasLockWithToken(client2ID, token2)
	assert.True(t, hasLock, "Client 2 should still hold the lock after duplicate release attempt")

	// Step 8: Client 2 appends 'B' (first time)
	fileResp2a, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client2ID,
		Filename:  filename,
		Content:   []byte("B"),
		Token:     token2,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend 'B' (first) RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp2a.Status, "Client 2 should be able to append 'B'")

	// Step 9: Client 2 appends 'B' (second time)
	fileResp2b, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client2ID,
		Filename:  filename,
		Content:   []byte("B"),
		Token:     token2,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend 'B' (second) RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp2b.Status, "Client 2 should be able to append 'B' again")

	// Step 10: Client 2 releases the lock
	reqID2Release := uuid.New().String()
	releaseResp3, err := server.LockRelease(context.Background(), &pb.LockArgs{
		ClientId:  client2ID,
		Token:     token2,
		RequestId: reqID2Release,
	})
	assert.NoError(t, err, "LockRelease RPC failed")
	assert.Equal(t, pb.Status_OK, releaseResp3.Status, "Lock release failed")

	// Step 11: Client 1 acquires the lock again
	reqID1Acquire2 := uuid.New().String()
	lockResp3, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  client1ID,
		RequestId: reqID1Acquire2,
	})
	assert.NoError(t, err, "Second LockAcquire for client 1 failed")
	assert.Equal(t, pb.Status_OK, lockResp3.Status, "Client 1 failed to reacquire lock")
	token1New := lockResp3.Token

	// Step 12: Client 1 appends 'A' (second time)
	fileResp3, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("A"),
		Token:     token1New,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend 'A' (second) RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp3.Status, "Client 1 should be able to append 'A' again")

	// Step 13: Client 1 appends 'A' (third time)
	fileResp4, err := server.FileAppend(context.Background(), &pb.FileArgs{
		ClientId:  client1ID,
		Filename:  filename,
		Content:   []byte("A"),
		Token:     token1New,
		RequestId: uuid.New().String(),
	})
	assert.NoError(t, err, "FileAppend 'A' (third) RPC failed")
	assert.Equal(t, pb.Status_OK, fileResp4.Status, "Client 1 should be able to append 'A' third time")

	// Step 14: Verify the file content is exactly "ABBAA"
	content, err := server.fileManager.ReadFile(filename)
	assert.NoError(t, err, "Failed to read file content")
	assert.Equal(t, "ABBAA", string(content), "File should contain exactly 'ABBAA'")
}
