package server

import (
	"context"
	"fmt"
	"net"
	"os"
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
	maxRetries := 60 // Maximum number of retries (enough to exceed the fencing period)
	retryInterval := 500 * time.Millisecond

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
		os.MkdirAll(dataDirs[i], 0755)
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

	// Allow servers time to establish connections and elect initial leader
	time.Sleep(3 * time.Second)

	// Find current leader
	leader := findLeader(t, clients)
	t.Logf("Initial leader found at index %d", leader)

	// Wait for fencing period to end before starting tests
	waitForFencingToEnd(t, clients[leader])

	// Part 1: Verify initial leader election and basic replication
	t.Run("InitialLeaderElection", func(t *testing.T) {
		// Check server states - server 0 should be leader, others followers
		verifyServerStates(t, clients, []ServerState{LeaderState, FollowerState, FollowerState})

		// Verify all servers have the same epoch
		verifyConsistentEpoch(t, clients)

		// Verify only the leader accepts lock requests
		leader := findLeader(t, clients)
		t.Logf("Found leader at index %d", leader)

		// Try acquiring lock on each server, should succeed only on leader
		for i, client := range clients {
			resp, err := client.LockAcquire(context.Background(), &pb.LockArgs{
				ClientId:  int32(i + 1),
				RequestId: fmt.Sprintf("req-initial-%d", i),
			})

			if i == leader {
				// Should succeed on leader
				if err != nil || resp.Status != pb.Status_OK {
					t.Fatalf("Failed to acquire lock on leader: %v, status: %v", err, resp.Status)
				}

				// Remember token to release later
				token := resp.Token

				// Release the lock
				releaseResp, err := client.LockRelease(context.Background(), &pb.LockArgs{
					ClientId:  int32(i + 1),
					Token:     token,
					RequestId: fmt.Sprintf("req-release-initial-%d", i),
				})
				if err != nil || releaseResp.Status != pb.Status_OK {
					t.Fatalf("Failed to release lock on leader: %v, status: %v", err, releaseResp.Status)
				}
			} else {
				// Should return SECONDARY_MODE on followers
				if err != nil {
					t.Fatalf("Request to follower failed: %v", err)
				}
				if resp.Status != pb.Status_SECONDARY_MODE {
					t.Fatalf("Expected SECONDARY_MODE from follower %d, got: %v", i, resp.Status)
				}
			}
		}
	})

	// Part 2: Test replication of lock state
	t.Run("StateReplication", func(t *testing.T) {
		// Find current leader
		leader := findLeader(t, clients)
		t.Logf("Found leader at index %d", leader)

		// Acquire a lock on the leader
		acquireResp, err := clients[leader].LockAcquire(context.Background(), &pb.LockArgs{
			ClientId:  100,
			RequestId: "req-acquire-replication",
		})
		if err != nil || acquireResp.Status != pb.Status_OK {
			t.Fatalf("Failed to acquire lock on leader: %v, status: %v", err, acquireResp.Status)
		}
		token := acquireResp.Token

		// Allow time for replication
		time.Sleep(2 * time.Second)

		// Verify lock state is replicated to followers
		for i, client := range clients {
			if i == leader {
				continue // Skip leader
			}

			// Try to acquire the same lock - should be rejected with SECONDARY_MODE
			// (and not ERROR which would indicate state wasn't synced)
			resp, err := client.LockAcquire(context.Background(), &pb.LockArgs{
				ClientId:  int32(200 + i),
				RequestId: fmt.Sprintf("req-check-replication-%d", i),
			})

			if err != nil {
				t.Fatalf("Request to follower %d failed: %v", i, err)
			}
			if resp.Status != pb.Status_SECONDARY_MODE {
				t.Fatalf("Expected SECONDARY_MODE from follower %d, got: %v", i, resp.Status)
			}
		}

		// Release the lock
		releaseResp, err := clients[leader].LockRelease(context.Background(), &pb.LockArgs{
			ClientId:  100,
			Token:     token,
			RequestId: "req-release-replication",
		})
		if err != nil || releaseResp.Status != pb.Status_OK {
			t.Fatalf("Failed to release lock: %v, status: %v", err, releaseResp.Status)
		}

		// Allow time for replication
		time.Sleep(1 * time.Second)
	})

	// Part 3: Test leader failure and automatic election
	t.Run("LeaderFailover", func(t *testing.T) {
		// Find current leader
		oldLeader := findLeader(t, clients)
		t.Logf("Current leader is server %d", oldLeader)

		// Acquire lock on leader (to verify it's properly released during failover)
		acquireResp, err := clients[oldLeader].LockAcquire(context.Background(), &pb.LockArgs{
			ClientId:  300,
			RequestId: "req-acquire-failover",
		})
		if err != nil || acquireResp.Status != pb.Status_OK {
			t.Fatalf("Failed to acquire lock on leader: %v, status: %v", err, acquireResp.Status)
		}

		// Remember the old epoch before failure
		oldEpoch := getServerEpoch(t, clients[oldLeader])

		// Stop the leader server
		t.Logf("Stopping leader server %d", oldLeader)
		servers[oldLeader].Stop()
		servers[oldLeader] = nil // Mark as stopped

		// Allow time for election to complete (needs to be longer than heartbeat failure detection)
		time.Sleep(5 * time.Second)

		// Find the new leader among remaining servers
		var newLeader int
		for i, client := range clients {
			if i != oldLeader && conns[i] != nil {
				info, err := client.ServerInfo(context.Background(), &pb.ServerInfoRequest{})
				if err != nil {
					t.Logf("Error checking server %d: %v", i, err)
					continue
				}
				if info.Role == string(LeaderState) {
					newLeader = i
					t.Logf("Found new leader: server %d", newLeader)
					break
				}
			}
		}

		// Verify a new leader was elected
		if newLeader == oldLeader {
			t.Fatalf("Failed to elect a new leader after leader failure")
		}

		// Verify new leader has higher epoch
		newEpoch := getServerEpoch(t, clients[newLeader])
		if newEpoch <= oldEpoch {
			t.Fatalf("New leader should have higher epoch. Got old=%d, new=%d",
				oldEpoch, newEpoch)
		}

		// Wait for fencing period to end
		waitForFencingToEnd(t, clients[newLeader])

		// Verify new leader can now accept operations
		afterFencingResp, err := clients[newLeader].LockAcquire(context.Background(), &pb.LockArgs{
			ClientId:  302,
			RequestId: "req-acquire-after-fencing",
		})
		if err != nil || afterFencingResp.Status != pb.Status_OK {
			t.Fatalf("Failed to acquire lock after fencing: %v, status: %v",
				err, afterFencingResp.Status)
		}
		newToken := afterFencingResp.Token

		// Release the lock
		releaseResp, err := clients[newLeader].LockRelease(context.Background(), &pb.LockArgs{
			ClientId:  302,
			Token:     newToken,
			RequestId: "req-release-after-fencing",
		})
		if err != nil || releaseResp.Status != pb.Status_OK {
			t.Fatalf("Failed to release lock after fencing: %v, status: %v",
				err, releaseResp.Status)
		}
	})

	// Part 4: Restart old leader, test split-brain prevention
	t.Run("SplitBrainPrevention", func(t *testing.T) {
		// Find current leader
		currentLeader := findLeader(t, clients)
		t.Logf("Current leader is server %d", currentLeader)

		// Find a stopped server to restart (the old leader)
		var stoppedIdx int
		for i, server := range servers {
			if server == nil {
				stoppedIdx = i
				break
			}
		}

		t.Logf("Restarting server %d", stoppedIdx)

		// Restart the stopped server
		var peerAddrs []string
		for j := 0; j < 3; j++ {
			if j != stoppedIdx {
				peerAddrs = append(peerAddrs, serverAddrs[j])
			}
		}

		// Start the server with LeaderState to simulate a split-brain scenario
		// (in real world this would happen if network partition healed)
		_, grpcServer := startMultiNodeServer(t, LeaderState, int32(stoppedIdx+1),
			peerAddrs, serverPorts[stoppedIdx], dataDirs[stoppedIdx])
		servers[stoppedIdx] = grpcServer

		// Reconnect client to restarted server
		if conns[stoppedIdx] != nil {
			conns[stoppedIdx].Close()
		}
		conns[stoppedIdx] = createClient(t, serverAddrs[stoppedIdx])
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
		}

		// Verify epochs are consistent
		verifyConsistentEpoch(t, clients)

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
			}
		}

		if leaderCount != 1 {
			t.Errorf("Expected exactly one leader, found %d", leaderCount)
		}
	})

	// Part 5: Test operation with minimum quorum (2 of 3 nodes)
	t.Run("MinimumQuorumOperation", func(t *testing.T) {
		// Find current leader
		currentLeader := findLeader(t, clients)
		t.Logf("Current leader is server %d", currentLeader)

		// Find a follower to stop
		var followerToStop int
		for i := 0; i < 3; i++ {
			if i != currentLeader && servers[i] != nil {
				followerToStop = i
				break
			}
		}

		// Stop the follower
		t.Logf("Stopping follower %d to test minimum quorum", followerToStop)
		if servers[followerToStop] != nil {
			servers[followerToStop].Stop()
			servers[followerToStop] = nil
		}

		// Wait a moment
		time.Sleep(2 * time.Second)

		// Wait for any fencing to end if the leader changed
		leader := findLeader(t, clients)
		if leader != currentLeader {
			t.Logf("Leader changed to %d, waiting for fencing to end", leader)
			waitForFencingToEnd(t, clients[leader])
		}

		// Verify system still operates with 2/3 nodes (minimum quorum)
		acquireResp, err := clients[leader].LockAcquire(context.Background(), &pb.LockArgs{
			ClientId:  400,
			RequestId: "req-acquire-min-quorum",
		})
		if err != nil || acquireResp.Status != pb.Status_OK {
			t.Fatalf("Failed to acquire lock with minimum quorum: %v, status: %v",
				err, acquireResp.Status)
		}
		token := acquireResp.Token

		// Release the lock
		releaseResp, err := clients[leader].LockRelease(context.Background(), &pb.LockArgs{
			ClientId:  400,
			Token:     token,
			RequestId: "req-release-min-quorum",
		})
		if err != nil || releaseResp.Status != pb.Status_OK {
			t.Fatalf("Failed to release lock with minimum quorum: %v, status: %v",
				err, releaseResp.Status)
		}
	})
}

// startMultiNodeServer starts a test gRPC server with the lock service
// and returns both the server implementation and the grpc.Server
func startMultiNodeServer(t *testing.T, state ServerState, id int32, peerAddrs []string,
	port int, dataDir string) (*LockServer, *grpc.Server) {

	// Set environment variables for data directory
	oldDataDir := os.Getenv("DATA_DIR")
	os.Setenv("DATA_DIR", dataDir)

	// Create custom heartbeat config for faster testing
	hbConfig := HeartbeatConfig{
		Interval:        200 * time.Millisecond, // Faster heartbeats for testing
		Timeout:         500 * time.Millisecond,
		MaxFailureCount: 2,
	}

	// Create server with fast fencing for testing
	s := NewReplicatedLockServerWithMultiPeers(
		ServerRole(state),
		id,
		peerAddrs,
		hbConfig,
	)
	s.SetFencingBuffer(500 * time.Millisecond) // Smaller buffer for testing

	// If this is server 1 and it's supposed to be a leader, set epoch to 1
	if id == 1 && state == LeaderState {
		s.currentEpoch.Store(1)
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
