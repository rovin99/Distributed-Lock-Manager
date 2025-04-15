package server

import (
	"Distributed-Lock-Manager/internal/file_manager"
	"Distributed-Lock-Manager/internal/lock_manager"
	pb "Distributed-Lock-Manager/proto"
	"context"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
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
	// Set the data directory environment variable
	originalDataDir := os.Getenv("DATA_DIR")
	os.Setenv("DATA_DIR", "data_server_3")
	defer os.Setenv("DATA_DIR", originalDataDir)

	// Create a test server with a lock manager
	lm := lock_manager.NewLockManager(nil)
	fm := file_manager.NewFileManager(false, lm)

	// Use a valid filename format (file_0 to file_99)
	testFile := "file_0"

	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)

	// Create the required files first - this creates data/file_0 through data/file_99
	fm.CreateFiles()

	// Clean up existing content to ensure test starts with empty file
	os.WriteFile("data_server_3/data/"+testFile, []byte(""), 0644)

	server := &LockServer{
		lockManager:  lm,
		fileManager:  fm,
		requestCache: NewRequestCache(5 * time.Minute),
		logger:       logger,

		metrics: NewPerformanceMetrics(logger),
	}

	// Initialize the serverState atomic value
	server.serverState.Store(LeaderState)

	// Step 1: C1 acquires lock T1
	acquireResp1, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  1,
		RequestId: "req-acquire-c1",
	})
	if err != nil || acquireResp1.Status != pb.Status_OK {
		t.Fatalf("C1 failed to acquire lock: %v, status: %v", err, acquireResp1.Status)
	}
	tokenT1 := acquireResp1.Token

	// Step 2: C1 appends 'A' to the file
	_, err = server.FileAppend(context.Background(), &pb.FileArgs{
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
	releaseResp1, err := server.LockRelease(context.Background(), &pb.LockArgs{
		ClientId:  1,
		Token:     tokenT1,
		RequestId: "req-release-c1-retry",
	})
	if err != nil || releaseResp1.Status != pb.Status_OK {
		t.Fatalf("C1 retry release failed: %v, status: %v", err, releaseResp1.Status)
	}

	// Step 5: C2 acquires lock T2
	acquireResp2, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  2,
		RequestId: "req-acquire-c2",
	})
	if err != nil || acquireResp2.Status != pb.Status_OK {
		t.Fatalf("C2 failed to acquire lock: %v, status: %v", err, acquireResp2.Status)
	}
	tokenT2 := acquireResp2.Token

	// Step 6: C2 appends 'B' to the file
	_, err = server.FileAppend(context.Background(), &pb.FileArgs{
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
	delayedReleaseResp, err := server.LockRelease(context.Background(), &pb.LockArgs{
		ClientId:  1,
		Token:     tokenT1,
		RequestId: "req-release-c1-delayed", // Different request ID from the retry
	})

	// The duplicate release should be successful (idempotent) even though C1 doesn't hold the lock anymore
	if err != nil || delayedReleaseResp.Status != pb.Status_OK {
		t.Fatalf("Delayed release from C1 should succeed but got: %v, status: %v",
			err, delayedReleaseResp.Status)
	}

	// Step 8: C2 releases its lock
	releaseResp2, err := server.LockRelease(context.Background(), &pb.LockArgs{
		ClientId:  2,
		Token:     tokenT2,
		RequestId: "req-release-c2",
	})
	if err != nil || releaseResp2.Status != pb.Status_OK {
		t.Fatalf("C2 release failed: %v, status: %v", err, releaseResp2.Status)
	}

	// Step 9: C1 re-acquires with a new token T3
	acquireResp3, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  1,
		RequestId: "req-acquire-c1-again",
	})
	if err != nil || acquireResp3.Status != pb.Status_OK {
		t.Fatalf("C1 failed to re-acquire lock: %v, status: %v", err, acquireResp3.Status)
	}
	tokenT3 := acquireResp3.Token

	// Step 10: C1 appends 'A' to the file again
	_, err = server.FileAppend(context.Background(), &pb.FileArgs{
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
	_, err = server.LockRelease(context.Background(), &pb.LockArgs{
		ClientId:  1,
		Token:     tokenT3,
		RequestId: "req-release-c1-final",
	})
	if err != nil {
		t.Fatalf("C1 final release failed: %v", err)
	}

	// Verify the file contents
	content, err := os.ReadFile("data_server_3/data/" + testFile)
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
	acquireResp4, err := server.LockAcquire(context.Background(), &pb.LockArgs{
		ClientId:  3,
		RequestId: "req-acquire-c3",
	})
	if err != nil || acquireResp4.Status != pb.Status_OK {
		t.Fatalf("C3 failed to acquire lock: %v, status: %v", err, acquireResp4.Status)
	}
	tokenT4 := acquireResp4.Token

	// C3 appends 'B' to the file
	_, err = server.FileAppend(context.Background(), &pb.FileArgs{
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
	_, err = server.LockRelease(context.Background(), &pb.LockArgs{
		ClientId:  3,
		Token:     tokenT4,
		RequestId: "req-release-c3",
	})
	if err != nil {
		t.Fatalf("C3 release failed: %v", err)
	}

	// Verify the file contents again
	content, err = os.ReadFile("data_server_3/data/" + testFile)
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

// TestServerInfo tests the ServerInfo method with different address formats
func TestServerInfo(t *testing.T) {
	tests := []struct {
		name        string
		serverAddr  string
		leaderState bool
		wantHost    bool
	}{
		{
			name:        "Leader with full address",
			serverAddr:  "example.com:50051",
			leaderState: true,
			wantHost:    true,
		},
		{
			name:        "Leader with port-only address",
			serverAddr:  ":50051",
			leaderState: true,
			wantHost:    true,
		},
		{
			name:        "Follower with known leader",
			serverAddr:  "follower:50052",
			leaderState: false,
			wantHost:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a server instance for testing
			logger := log.New(io.Discard, "[TestServerInfo] ", log.LstdFlags)
			lm := lock_manager.NewLockManagerWithLeaseDuration(logger, 30*time.Second)
			fm := file_manager.NewFileManagerWithWAL(false, false, lm)

			s := &LockServer{
				lockManager:      lm,
				fileManager:      fm,
				logger:           logger,
				selfAddress:      tt.serverAddr,
				serverID:         1,
				peerAddresses:    []string{"peer1:50051", "peer2:50052"},
				currentEpoch:     atomic.Int64{},
				serverState:      atomic.Value{},
				replicationQueue: make([]*pb.ReplicatedState, 0),
				peerClients:      make(map[string]pb.LockServiceClient),
			}

			// Set the server state
			if tt.leaderState {
				s.serverState.Store(LeaderState)
			} else {
				s.serverState.Store(FollowerState)
				// Set a leader address for follower tests
				s.setLeaderAddress("knownleader:50053")
			}

			// Call ServerInfo
			resp, err := s.ServerInfo(context.Background(), &pb.ServerInfoRequest{})

			// Check the response
			if err != nil {
				t.Errorf("ServerInfo() error = %v", err)
				return
			}

			if resp.ServerId != s.serverID {
				t.Errorf("ServerInfo() ServerId = %v, want %v", resp.ServerId, s.serverID)
			}

			if tt.leaderState && resp.Role != string(LeaderState) {
				t.Errorf("ServerInfo() Role = %v, want %v", resp.Role, LeaderState)
			}

			if !tt.leaderState && resp.Role != string(FollowerState) {
				t.Errorf("ServerInfo() Role = %v, want %v", resp.Role, FollowerState)
			}

			// Check leader address format
			if resp.LeaderAddress == "" {
				t.Errorf("ServerInfo() LeaderAddress is empty")
			}

			if tt.wantHost {
				if !strings.Contains(resp.LeaderAddress, ":") {
					t.Errorf("ServerInfo() LeaderAddress = %v, missing host:port format", resp.LeaderAddress)
				}

				host, _, err := net.SplitHostPort(resp.LeaderAddress)
				if err != nil {
					t.Errorf("ServerInfo() LeaderAddress = %v is not valid host:port format: %v", resp.LeaderAddress, err)
				}

				if tt.leaderState && tt.serverAddr == ":50051" && host == "" {
					t.Errorf("ServerInfo() LeaderAddress = %v has empty host, expected non-empty host", resp.LeaderAddress)
				}
			}

			t.Logf("ServerInfo() returned LeaderAddress = %v", resp.LeaderAddress)
		})
	}
}
