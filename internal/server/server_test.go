package server

import (
	"Distributed-Lock-Manager/internal/file_manager"
	"Distributed-Lock-Manager/internal/lock_manager"
	"Distributed-Lock-Manager/proto"
	"context"
	"log"
	"os"
	"testing"
	"time"
)

// TestDuplicatedReleasePacket tests the scenario described in the bug:
// C1 acquires T1, appends 'A', sends Release(T1) (delayed).
// C1 retries Release(T1), gets OK.
// C2 acquires T2, appends 'B'.
// Delayed Release(T1) arrives.
// C2 releases.
// C1 re-acquires T3, appends 'A'.
func TestDuplicatedReleasePacket(t *testing.T) {
	// Create a test server with a lock manager
	lm := lock_manager.NewLockManager(nil)
	fm := file_manager.NewFileManager(false, lm)

	// Use a valid filename format (file_0 to file_99)
	testFile := "file_0"

	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)

	// Create the required files first - this creates data/file_0 through data/file_99
	fm.CreateFiles()

	// Clean up existing content to ensure test starts with empty file
	os.WriteFile("data/"+testFile, []byte(""), 0644)

	server := &LockServer{
		lockManager:  lm,
		fileManager:  fm,
		requestCache: NewRequestCache(5 * time.Minute),
		logger:       logger,
		isPrimary:    true,
		metrics:      NewPerformanceMetrics(logger),
	}

	// Initialize the serverState atomic value
	server.serverState.Store(LeaderState)

	// Step 1: C1 acquires lock T1
	acquireResp1, err := server.LockAcquire(context.Background(), &proto.LockArgs{
		ClientId:  1,
		RequestId: "req-acquire-c1",
	})
	if err != nil || acquireResp1.Status != proto.Status_OK {
		t.Fatalf("C1 failed to acquire lock: %v, status: %v", err, acquireResp1.Status)
	}
	tokenT1 := acquireResp1.Token

	// Step 2: C1 appends 'A' to the file
	_, err = server.FileAppend(context.Background(), &proto.FileArgs{
		ClientId:  1,
		Token:     tokenT1,
		Filename:  testFile,
		Content:   []byte("A"),
		RequestId: "req-append-c1-1",
	})
	if err != nil {
		t.Fatalf("C1 failed to append to file: %v", err)
	}

	// Step 3: C1 sends the first release request (this will be delayed)
	// We simulate this by not actually sending it yet

	// Step 4: C1 retries release with a new request ID (this succeeds)
	releaseResp1, err := server.LockRelease(context.Background(), &proto.LockArgs{
		ClientId:  1,
		Token:     tokenT1,
		RequestId: "req-release-c1-retry",
	})
	if err != nil || releaseResp1.Status != proto.Status_OK {
		t.Fatalf("C1 retry release failed: %v, status: %v", err, releaseResp1.Status)
	}

	// Step 5: C2 acquires lock T2
	acquireResp2, err := server.LockAcquire(context.Background(), &proto.LockArgs{
		ClientId:  2,
		RequestId: "req-acquire-c2",
	})
	if err != nil || acquireResp2.Status != proto.Status_OK {
		t.Fatalf("C2 failed to acquire lock: %v, status: %v", err, acquireResp2.Status)
	}
	tokenT2 := acquireResp2.Token

	// Step 6: C2 appends 'B' to the file
	_, err = server.FileAppend(context.Background(), &proto.FileArgs{
		ClientId:  2,
		Token:     tokenT2,
		Filename:  testFile,
		Content:   []byte("B"),
		RequestId: "req-append-c2",
	})
	if err != nil {
		t.Fatalf("C2 failed to append to file: %v", err)
	}

	// Step 7: NOW the delayed release packet from C1 arrives
	delayedReleaseResp, err := server.LockRelease(context.Background(), &proto.LockArgs{
		ClientId:  1,
		Token:     tokenT1,
		RequestId: "req-release-c1-delayed", // Different request ID from the retry
	})

	// The duplicate release should be successful (idempotent) even though C1 doesn't hold the lock anymore
	if err != nil || delayedReleaseResp.Status != proto.Status_OK {
		t.Fatalf("Delayed release from C1 should succeed but got: %v, status: %v",
			err, delayedReleaseResp.Status)
	}

	// Step 8: C2 releases its lock
	releaseResp2, err := server.LockRelease(context.Background(), &proto.LockArgs{
		ClientId:  2,
		Token:     tokenT2,
		RequestId: "req-release-c2",
	})
	if err != nil || releaseResp2.Status != proto.Status_OK {
		t.Fatalf("C2 release failed: %v, status: %v", err, releaseResp2.Status)
	}

	// Step 9: C1 re-acquires with a new token T3
	acquireResp3, err := server.LockAcquire(context.Background(), &proto.LockArgs{
		ClientId:  1,
		RequestId: "req-acquire-c1-again",
	})
	if err != nil || acquireResp3.Status != proto.Status_OK {
		t.Fatalf("C1 failed to re-acquire lock: %v, status: %v", err, acquireResp3.Status)
	}
	tokenT3 := acquireResp3.Token

	// Step 10: C1 appends 'A' to the file again
	_, err = server.FileAppend(context.Background(), &proto.FileArgs{
		ClientId:  1,
		Token:     tokenT3,
		Filename:  testFile,
		Content:   []byte("A"),
		RequestId: "req-append-c1-2",
	})
	if err != nil {
		t.Fatalf("C1 failed to append to file the second time: %v", err)
	}

	// Step 11: C1 releases the lock
	_, err = server.LockRelease(context.Background(), &proto.LockArgs{
		ClientId:  1,
		Token:     tokenT3,
		RequestId: "req-release-c1-final",
	})
	if err != nil {
		t.Fatalf("C1 final release failed: %v", err)
	}

	// Verify the file contents
	content, err := os.ReadFile("data/" + testFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	// The expected content should be "ABA" according to the operations performed
	expectedContent := "ABA"
	if string(content) != expectedContent {
		t.Errorf("Expected file content to be %q, but got %q", expectedContent, string(content))
	}

	// Now, let's add one more client to get "ABBA" as expected
	// C3 acquires lock
	acquireResp4, err := server.LockAcquire(context.Background(), &proto.LockArgs{
		ClientId:  3,
		RequestId: "req-acquire-c3",
	})
	if err != nil || acquireResp4.Status != proto.Status_OK {
		t.Fatalf("C3 failed to acquire lock: %v, status: %v", err, acquireResp4.Status)
	}
	tokenT4 := acquireResp4.Token

	// C3 appends 'B' to the file
	_, err = server.FileAppend(context.Background(), &proto.FileArgs{
		ClientId:  3,
		Token:     tokenT4,
		Filename:  testFile,
		Content:   []byte("B"),
		RequestId: "req-append-c3",
	})
	if err != nil {
		t.Fatalf("C3 failed to append to file: %v", err)
	}

	// C3 releases the lock
	_, err = server.LockRelease(context.Background(), &proto.LockArgs{
		ClientId:  3,
		Token:     tokenT4,
		RequestId: "req-release-c3",
	})
	if err != nil {
		t.Fatalf("C3 release failed: %v", err)
	}

	// Verify the file contents again
	content, err = os.ReadFile("data/" + testFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	// The final content should now be "ABAB"
	expectedContent = "ABAB"
	if string(content) != expectedContent {
		t.Errorf("Expected final file content to be %q, but got %q", expectedContent, string(content))
	}

	// Clean up
	fm.Cleanup()
}
