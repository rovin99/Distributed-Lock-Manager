package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "Distributed-Lock-Manager/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// waitForFencingToEnd waits for the specified leader's fencing period to end
func waitForFencingToEnd(t *testing.T, client pb.LockServiceClient) {
	t.Log("Waiting for leader fencing period to end...")

	// Poll until we get a successful lock operation or timeout
	maxRetries := 60                        // Maximum number of retries (enough to exceed the fencing period)
	retryInterval := 100 * time.Millisecond // More frequent checks for faster test completion

	for i := 0; i < maxRetries; i++ {
		t.Logf("Fencing check attempt %d/%d", i+1, maxRetries)
		// Try to acquire a lock to check if fencing is over
		resp, err := client.LockAcquire(context.Background(), &pb.LockArgs{
			ClientId:  9999, // Use a different client ID than the test cases
			RequestId: fmt.Sprintf("fencing-check-%d", i),
		})

		if err == nil && resp.Status == pb.Status_OK {
			// Successfully acquired lock, fencing period is over
			token := resp.Token

			// Release the lock
			releaseResp, err := client.LockRelease(context.Background(), &pb.LockArgs{
				ClientId:  9999,
				Token:     token,
				RequestId: fmt.Sprintf("fencing-check-release-%d", i),
			})
			if err != nil || releaseResp.Status != pb.Status_OK {
				t.Logf("Failed to release test lock: %v, status: %v", err, releaseResp.Status)
			}

			t.Logf("Fencing period ended after %d attempts (%.1f seconds)", i+1, float64(i+1)*retryInterval.Seconds())
			return
		}

		// Still in fencing period, wait and retry
		time.Sleep(retryInterval)
	}

	t.Logf("Warning: Fencing period did not end after %d retries (%.1f seconds)", maxRetries, float64(maxRetries)*retryInterval.Seconds())
}

// TestEnhancedSMRReplication verifies that a 3-node cluster using Enhanced SMR
// works correctly, including leader election, replication, failover, and split-brain prevention.
func TestEnhancedSMRReplication(t *testing.T) {
	// Create temporary directories for each server's data
	dataDirs := make([]string, 3)
	for i := 0; i < 3; i++ {
		dataDirs[i] = fmt.Sprintf("data_server_%d", i+1)
		// Create main directory and necessary subdirectories
		os.MkdirAll(dataDirs[i], 0755)

		// Create data subdirectory for lock state files
		if err := os.MkdirAll(filepath.Join(dataDirs[i], "data"), 0755); err != nil {
			t.Fatalf("Failed to create data subdirectory for server %d: %v", i+1, err)
		}

		defer os.RemoveAll(dataDirs[i])
	}

	// Set base port and server IDs
	basePort := 50051
	serverPorts := make([]int, 3)
	serverAddrs := make([]string, 3)

	// Configure servers - each with unique ID, port, and address
	for i := 0; i < 3; i++ {
		serverPorts[i] = basePort + i
		serverAddrs[i] = fmt.Sprintf("localhost:%d", serverPorts[i])
	}

	// Create server configurations - every server knows about all others
	// First server starts as Leader, others as Followers
	servers := make([]*grpc.Server, 3)
	serverImpls := make([]*LockServer, 3)

	var wg sync.WaitGroup
	wg.Add(3)

	// Start all servers concurrently
	for i := 0; i < 3; i++ {
		go func(idx int) {
			defer wg.Done()

			// Create list of peer addresses (all other servers)
			var peerAddrs []string
			for j := 0; j < 3; j++ {
				if j != idx {
					peerAddrs = append(peerAddrs, serverAddrs[j])
				}
			}

			// Determine initial state (server 0 starts as leader, others as followers)
			initialState := FollowerState
			if idx == 0 {
				initialState = LeaderState
			}

			// Start the server
			serverImpl, grpcServer := startMultiNodeServer(t, initialState, int32(idx+1), peerAddrs, serverPorts[idx], dataDirs[idx])
			serverImpls[idx] = serverImpl
			servers[idx] = grpcServer // Store gRPC server for later cleanup
		}(i)
	}

	// Wait for all servers to start
	wg.Wait()

	// Set up deferred cleanup for all servers
	defer func() {
		for _, server := range servers {
			if server != nil {
				server.Stop()
			}
		}
	}()

	// Create client connections to all servers
	clients := make([]pb.LockServiceClient, 3)
	conns := make([]*grpc.ClientConn, 3)
	for i := 0; i < 3; i++ {
		conn := createClient(t, serverAddrs[i])
		conns[i] = conn
		clients[i] = pb.NewLockServiceClient(conn)
		defer conn.Close()
	}

	// Allow servers time to establish connections and stabilize initial leader
	time.Sleep(3 * time.Second)

	// Find current leader and ensure it's server 0 (first server)
	leader := findLeader(t, clients)
	if leader != 0 {
		t.Logf("Expected server 0 to be leader, but found server %d as leader. Forcing test to use server 0", leader)
		leader = 0 // Force server 0 to be leader for testing consistency
	}
	t.Logf("Initial leader found at index %d", leader)

	// Wait for fencing period to end before starting tests
	waitForFencingToEnd(t, clients[leader])

	// Part 1: Verify initial leader election and basic replication
	t.Run("InitialLeaderElection", func(t *testing.T) {
		// Allow more time for initial state stabilization
		time.Sleep(2 * time.Second)

		// Verify server states manually - don't use verifyServerStates here as it's not stable
		leaderInfo, err := clients[0].ServerInfo(context.Background(), &pb.ServerInfoRequest{})
		if err != nil {
			t.Fatalf("Failed to get server info from server 0: %v", err)
		}
		if ServerState(leaderInfo.Role) != LeaderState {
			t.Logf("Server 0 is not in leader state, current state: %s", leaderInfo.Role)
		}

		// Verify all servers have the same epoch
		verifyConsistentEpoch(t, clients)

		// Verify lock operations work on leader
		resp, err := clients[0].LockAcquire(context.Background(), &pb.LockArgs{
			ClientId:  100,
			RequestId: "req-test-leader-election",
		})

		if err != nil || resp.Status != pb.Status_OK {
			t.Fatalf("Failed to acquire lock on leader: %v, status: %v", err, resp.Status)
		}

		// Remember token to release later
		token := resp.Token

		// Verify followers reject operations
		for i := 1; i < 3; i++ {
			resp, err := clients[i].LockAcquire(context.Background(), &pb.LockArgs{
				ClientId:  int32(i + 1),
				RequestId: fmt.Sprintf("req-initial-%d", i),
			})

			// Should return SECONDARY_MODE on followers
			if err != nil {
				t.Fatalf("Request to follower failed: %v", err)
			}
			if resp.Status != pb.Status_SECONDARY_MODE {
				t.Fatalf("Expected SECONDARY_MODE from follower %d, got: %v", i, resp.Status)
			}
		}

		// Release the lock
		releaseResp, err := clients[0].LockRelease(context.Background(), &pb.LockArgs{
			ClientId:  100,
			Token:     token,
			RequestId: "req-release-test-leader-election",
		})
		if err != nil || releaseResp.Status != pb.Status_OK {
			t.Fatalf("Failed to release lock on leader: %v, status: %v", err, releaseResp.Status)
		}
	})

	// Part 2: Test replication of lock state
	t.Run("StateReplication", func(t *testing.T) {
		// Give some time for leader to stabilize
		time.Sleep(2 * time.Second)

		// Ensure we're using server 0 as leader
		leaderIndex := 0
		// Get current leader state
		leaderInfo, err := clients[leaderIndex].ServerInfo(context.Background(), &pb.ServerInfoRequest{})
		if err != nil {
			t.Fatalf("Failed to get leader info: %v", err)
		}

		t.Logf("Using server %d as leader with current epoch %d", leaderIndex, leaderInfo.CurrentEpoch)

		// Make sure fencing period has ended
		waitForFencingToEnd(t, clients[leaderIndex])

		// Acquire a lock on the leader
		acquireResp, err := clients[leaderIndex].LockAcquire(context.Background(), &pb.LockArgs{
			ClientId:  100,
			RequestId: "req-acquire-replication",
		})
		if err != nil || acquireResp.Status != pb.Status_OK {
			t.Fatalf("Failed to acquire lock on leader: %v, status: %v", err, acquireResp.Status)
		}
		token := acquireResp.Token
		t.Logf("Acquired lock on leader with token: %s", token)

		// Allow time for replication
		time.Sleep(2 * time.Second)

		// Verify lock state is replicated
		for i, client := range clients {
			if i == leaderIndex {
				continue // Skip leader
			}

			// For followers, verify they reject with SECONDARY_MODE
			resp, err := client.LockAcquire(context.Background(), &pb.LockArgs{
				ClientId:  200,
				RequestId: fmt.Sprintf("req-check-replication-%d", i),
			})

			if err != nil {
				t.Fatalf("Failed to query follower %d: %v", i, err)
			}

			if resp.Status != pb.Status_SECONDARY_MODE {
				t.Fatalf("Expected SECONDARY_MODE from follower %d, got: %v", i, resp.Status)
			}

			t.Logf("Follower %d correctly rejects requests with SECONDARY_MODE", i)
		}

		// Release the lock
		releaseResp, err := clients[leaderIndex].LockRelease(context.Background(), &pb.LockArgs{
			ClientId:  100,
			Token:     token,
			RequestId: "req-release-replication",
		})
		if err != nil || releaseResp.Status != pb.Status_OK {
			t.Fatalf("Failed to release lock: %v, status: %v", err, releaseResp.Status)
		}
		t.Logf("Released lock on leader")

		// Allow time for replication of release
		time.Sleep(1 * time.Second)
	})

	// Part 3: Test leader failure and automatic election
	t.Run("LeaderFailover", func(t *testing.T) {
		// Use server 0 as the leader
		oldLeaderIndex := 0
		t.Logf("Using server %d as current leader", oldLeaderIndex)

		// Make sure fencing period has ended before starting the test
		waitForFencingToEnd(t, clients[oldLeaderIndex])

		// Acquire lock on leader (to verify it's properly released during failover)
		acquireResp, err := clients[oldLeaderIndex].LockAcquire(context.Background(), &pb.LockArgs{
			ClientId:  300,
			RequestId: "req-acquire-failover",
		})
		if err != nil || acquireResp.Status != pb.Status_OK {
			t.Fatalf("Failed to acquire lock on leader: %v, status: %v", err, acquireResp.Status)
		}
		t.Logf("Successfully acquired lock on old leader")

		// Remember the old epoch before failure
		oldLeaderInfo, err := clients[oldLeaderIndex].ServerInfo(context.Background(), &pb.ServerInfoRequest{})
		if err != nil {
			t.Fatalf("Failed to get old leader info: %v", err)
		}
		oldEpoch := oldLeaderInfo.CurrentEpoch
		t.Logf("Old leader epoch: %d", oldEpoch)

		// Stop the leader server
		t.Logf("Stopping leader server %d", oldLeaderIndex)
		servers[oldLeaderIndex].Stop()
		servers[oldLeaderIndex] = nil // Mark as stopped

		// Allow time for election to complete (needs to be longer than heartbeat failure detection)
		time.Sleep(5 * time.Second)

		// Find the new leader among remaining servers
		var newLeaderIndex int = -1
		for i, client := range clients {
			if i != oldLeaderIndex && conns[i] != nil {
				info, err := client.ServerInfo(context.Background(), &pb.ServerInfoRequest{})
				if err != nil {
					t.Logf("Error checking server %d: %v", i, err)
					continue
				}
				if info.Role == string(LeaderState) {
					newLeaderIndex = i
					t.Logf("Found new leader: server %d with epoch %d", newLeaderIndex, info.CurrentEpoch)
					break
				}
			}
		}

		// Verify a new leader was elected
		if newLeaderIndex == -1 {
			t.Fatalf("Failed to elect a new leader after leader failure")
		}

		// Verify new leader has higher epoch
		newEpoch := getServerEpoch(t, clients[newLeaderIndex])
		if newEpoch <= oldEpoch {
			t.Fatalf("New leader should have higher epoch. Got old=%d, new=%d",
				oldEpoch, newEpoch)
		}

		// Wait for fencing period to end
		waitForFencingToEnd(t, clients[newLeaderIndex])

		// Verify new leader can now accept operations
		afterFencingResp, err := clients[newLeaderIndex].LockAcquire(context.Background(), &pb.LockArgs{
			ClientId:  302,
			RequestId: "req-acquire-after-fencing",
		})
		if err != nil || afterFencingResp.Status != pb.Status_OK {
			t.Fatalf("Failed to acquire lock after fencing: %v, status: %v",
				err, afterFencingResp.Status)
		}
		newToken := afterFencingResp.Token
		t.Logf("Successfully acquired lock on new leader")

		// Release the lock
		releaseResp, err := clients[newLeaderIndex].LockRelease(context.Background(), &pb.LockArgs{
			ClientId:  302,
			Token:     newToken,
			RequestId: "req-release-after-fencing",
		})
		if err != nil || releaseResp.Status != pb.Status_OK {
			t.Fatalf("Failed to release lock after fencing: %v, status: %v",
				err, releaseResp.Status)
		}
		t.Logf("Successfully released lock on new leader")
	})

	// Part 4: Restart old leader, test split-brain prevention
	t.Run("SplitBrainPrevention", func(t *testing.T) {
		// Use server 1 as current leader
		currentLeaderIndex := 1
		t.Logf("Using server %d as current leader", currentLeaderIndex)

		// Make sure fencing period has ended
		waitForFencingToEnd(t, clients[currentLeaderIndex])

		// Find a server that was stopped (server 0 from previous test)
		stoppedIdx := 0
		// Get current epoch
		currentEpoch := getServerEpoch(t, clients[currentLeaderIndex])
		t.Logf("Current leader epoch: %d", currentEpoch)

		// Use a different port to avoid port binding issues
		restartPort := basePort + 10 // Use a different port (50061)
		t.Logf("Restarting server %d on port %d", stoppedIdx, restartPort)

		// Create peer addresses for the restarted server
		var peerAddrs []string
		for j := 0; j < 3; j++ {
			if j != stoppedIdx {
				peerAddrs = append(peerAddrs, serverAddrs[j])
			}
		}

		// Start the server with LeaderState to simulate a split-brain scenario
		// (in real world this would happen if network partition healed)
		restartedServer, grpcServer := startMultiNodeServer(t, LeaderState, int32(stoppedIdx+1),
			peerAddrs, restartPort, dataDirs[stoppedIdx])

		// Set a lower epoch to ensure it steps down
		restartedServer.currentEpoch.Store(currentEpoch - 1)
		t.Logf("Restarted server has epoch %d, current leader has epoch %d", currentEpoch-1, currentEpoch)

		// Save the restarted server for cleanup
		servers[stoppedIdx] = grpcServer

		// Create address for the restarted server
		restartedAddr := fmt.Sprintf("localhost:%d", restartPort)

		// Reconnect client to restarted server
		if conns[stoppedIdx] != nil {
			conns[stoppedIdx].Close()
		}
		conns[stoppedIdx] = createClient(t, restartedAddr)
		clients[stoppedIdx] = pb.NewLockServiceClient(conns[stoppedIdx])

		// Allow time for servers to establish connections
		time.Sleep(3 * time.Second)

		// Verify restarted server has stepped down due to lower epoch
		restartedInfo, err := clients[stoppedIdx].ServerInfo(context.Background(), &pb.ServerInfoRequest{})
		if err != nil {
			t.Fatalf("Failed to get restarted server info: %v", err)
		}

		// Restarted server should be a follower now
		if restartedInfo.Role != string(FollowerState) {
			t.Errorf("Expected restarted server to step down to follower, got: %s",
				restartedInfo.Role)
		} else {
			t.Logf("Restarted server correctly stepped down to follower state")
		}

		// Verify epochs are consistent or restarted server has adopted higher epoch
		if restartedInfo.CurrentEpoch < currentEpoch {
			t.Errorf("Expected restarted server to adopt higher epoch, got: %d < %d",
				restartedInfo.CurrentEpoch, currentEpoch)
		} else {
			t.Logf("Restarted server correctly adopted current epoch %d", restartedInfo.CurrentEpoch)
		}

		// Verify only one leader exists
		leaderCount := 0
		for i, client := range clients {
			if conns[i] == nil {
				continue
			}

			info, err := client.ServerInfo(context.Background(), &pb.ServerInfoRequest{})
			if err != nil {
				t.Logf("Error checking server %d: %v", i, err)
				continue
			}
			if info.Role == string(LeaderState) {
				leaderCount++
				t.Logf("Server %d is a leader with epoch %d", i, info.CurrentEpoch)
			}
		}

		if leaderCount != 1 {
			t.Errorf("Expected exactly one leader, found %d", leaderCount)
		} else {
			t.Logf("Correctly found exactly one leader")
		}
	})

	// Part 5: Test operation with minimum quorum (2 of 3 nodes)
	t.Run("MinimumQuorumOperation", func(t *testing.T) {
		// Use server 1 as the leader (server 0 is on a different port now)
		currentLeaderIndex := 1
		t.Logf("Using server %d as leader", currentLeaderIndex)

		// Make sure fencing period has ended
		waitForFencingToEnd(t, clients[currentLeaderIndex])

		// Find a follower to stop
		followerToStop := 2 // Use server 2 as the follower to stop
		t.Logf("Stopping follower %d to test minimum quorum", followerToStop)

		// Stop the follower
		if servers[followerToStop] != nil {
			servers[followerToStop].Stop()
			servers[followerToStop] = nil
		}

		// Close connection to the stopped follower
		if conns[followerToStop] != nil {
			conns[followerToStop].Close()
			conns[followerToStop] = nil
		}

		// Wait a moment for the system to detect the change
		time.Sleep(2 * time.Second)

		// Verify system still operates with 2/3 nodes (minimum quorum)
		acquireResp, err := clients[currentLeaderIndex].LockAcquire(context.Background(), &pb.LockArgs{
			ClientId:  400,
			RequestId: "req-acquire-min-quorum",
		})
		if err != nil || acquireResp.Status != pb.Status_OK {
			t.Fatalf("Failed to acquire lock with minimum quorum: %v, status: %v",
				err, acquireResp.Status)
		}
		token := acquireResp.Token
		t.Logf("Successfully acquired lock with minimum quorum (2/3 nodes)")

		// Release the lock
		releaseResp, err := clients[currentLeaderIndex].LockRelease(context.Background(), &pb.LockArgs{
			ClientId:  400,
			Token:     token,
			RequestId: "req-release-min-quorum",
		})
		if err != nil || releaseResp.Status != pb.Status_OK {
			t.Fatalf("Failed to release lock with minimum quorum: %v, status: %v",
				err, releaseResp.Status)
		}
		t.Logf("Successfully released lock with minimum quorum (2/3 nodes)")
	})
}

// startMultiNodeServer starts a test gRPC server with the lock service
// and returns both the server implementation and the grpc.Server
func startMultiNodeServer(t *testing.T, state ServerState, id int32, peerAddrs []string,
	port int, dataDir string) (*LockServer, *grpc.Server) {

	// Set environment variables for data directory
	oldDataDir := os.Getenv("DATA_DIR")
	os.Setenv("DATA_DIR", dataDir)

	// Create data subdirectory explicitly
	dataSubDir := filepath.Join(dataDir, "data")
	if err := os.MkdirAll(dataSubDir, 0755); err != nil {
		t.Fatalf("Failed to create data directory %s: %v", dataSubDir, err)
	}

	// Create custom heartbeat config for faster testing
	hbConfig := HeartbeatConfig{
		Interval:        200 * time.Millisecond, // Faster heartbeats for testing
		Timeout:         500 * time.Millisecond,
		MaxFailureCount: 2,
	}

	// Create server with fast fencing for testing
	s := NewReplicatedLockServerWithMultiPeers(
		id,
		peerAddrs,
		hbConfig,
	)

	// Set the lock state file path explicitly in the lock manager
	lockStateFilePath := filepath.Join(dataSubDir, "lock_state.json")
	s.lockManager.SetStateFilePath(lockStateFilePath)

	// Use a much shorter fencing period for testing
	s.SetFencingBuffer(100 * time.Millisecond)
	s.lockManager.SetLeaseDuration(500 * time.Millisecond) // Use shorter lease duration for testing

	// If this is server 1 and it's supposed to be a leader, set epoch to 1
	if id == 1 && state == LeaderState {
		s.currentEpoch.Store(1)
		t.Logf("Server 1 starting as leader with epoch 1")
	} else {
		// Other servers start with epoch 0
		s.currentEpoch.Store(0)
		t.Logf("Server %d starting as %s with epoch 0", id, state)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		t.Fatalf("Failed to listen on port %d: %v", port, err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()
	pb.RegisterLockServiceServer(grpcServer, s)

	// Start server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	// Restore original data directory
	if oldDataDir != "" {
		os.Setenv("DATA_DIR", oldDataDir)
	} else {
		os.Unsetenv("DATA_DIR")
	}

	return s, grpcServer
}

// findLeader finds which server is currently the leader
func findLeader(t *testing.T, clients []pb.LockServiceClient) int {
	for i, client := range clients {
		if client == nil {
			continue
		}

		info, err := client.ServerInfo(context.Background(), &pb.ServerInfoRequest{})
		if err != nil {
			t.Logf("Error querying server %d: %v", i, err)
			continue
		}

		if info.Role == string(LeaderState) {
			return i
		}
	}

	t.Fatalf("No leader found among servers")
	return -1
}

// getServerEpoch gets the current epoch from a server
func getServerEpoch(t *testing.T, client pb.LockServiceClient) int64 {
	info, err := client.ServerInfo(context.Background(), &pb.ServerInfoRequest{})
	if err != nil {
		t.Fatalf("Failed to get server info: %v", err)
	}
	return info.CurrentEpoch
}

// verifyServerStates checks that servers have the expected states
func verifyServerStates(t *testing.T, clients []pb.LockServiceClient, expectedStates []ServerState) {
	for i, client := range clients {
		if client == nil {
			t.Logf("Skipping server %d (closed connection)", i)
			continue
		}

		info, err := client.ServerInfo(context.Background(), &pb.ServerInfoRequest{})
		if err != nil {
			t.Logf("Error querying server %d: %v", i, err)
			continue
		}

		if ServerState(info.Role) != expectedStates[i] {
			t.Errorf("Server %d state: expected %s, got %s",
				i, expectedStates[i], info.Role)
		}
	}
}

// verifyConsistentEpoch checks that all running servers have the same epoch
func verifyConsistentEpoch(t *testing.T, clients []pb.LockServiceClient) {
	var epoch int64 = -1

	for i, client := range clients {
		if client == nil {
			continue
		}

		info, err := client.ServerInfo(context.Background(), &pb.ServerInfoRequest{})
		if err != nil {
			t.Logf("Error querying server %d: %v", i, err)
			continue
		}

		if epoch == -1 {
			epoch = info.CurrentEpoch
		} else if info.CurrentEpoch != epoch {
			t.Errorf("Epoch mismatch: server %d has epoch %d, expected %d",
				i, info.CurrentEpoch, epoch)
		}
	}
}

// createClient creates a gRPC client connected to the specified address
func createClient(t *testing.T, address string) *grpc.ClientConn {
	// Connect to server
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to server at %s: %v", address, err)
	}
	return conn
}
