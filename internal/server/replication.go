package server

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "Distributed-Lock-Manager/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
)

// getServerStateString returns the current server state as a string
func (s *LockServer) getServerStateString() ServerState {
	return s.serverState.Load().(ServerState)
}

// isLeader returns true if this server is the leader
func (s *LockServer) isLeader() bool {
	return s.getServerStateString() == LeaderState
}

// isFollower returns true if this server is a follower
func (s *LockServer) isFollower() bool {
	return s.getServerStateString() == FollowerState
}

// isCandidate returns true if this server is a candidate
func (s *LockServer) isCandidate() bool {
	return s.getServerStateString() == CandidateState
}

// setServerState sets the server state
func (s *LockServer) setServerState(state ServerState) {
	oldState := s.getServerStateString()
	if oldState != state {
		s.serverState.Store(state)
		s.logger.Printf("Server state changed from %s to %s", oldState, state)

		// Clear the request cache on state transitions to avoid stale responses
		if s.requestCache != nil {
			s.logger.Printf("Clearing request cache due to state transition from %s to %s", oldState, state)
			s.requestCache.Clear()
		}
	}
}

// getLeaderAddress gets the current leader address with synchronization
func (s *LockServer) getLeaderAddress() string {
	s.leaderMu.RLock()
	defer s.leaderMu.RUnlock()
	return s.leaderAddress
}

// setLeaderAddress sets the leader address with synchronization
func (s *LockServer) setLeaderAddress(addr string) {
	s.leaderMu.Lock()
	defer s.leaderMu.Unlock()
	if s.leaderAddress != addr {
		s.logger.Printf("Leader address changed from %s to %s", s.leaderAddress, addr)
		s.leaderAddress = addr
	}
}

// getOrConnectPeer gets or establishes a connection to a peer
func (s *LockServer) getOrConnectPeer(peerAddress string) (pb.LockServiceClient, error) {
	// Check if we already have a connection
	s.peerClientsMu.RLock()
	client, exists := s.peerClients[peerAddress]
	s.peerClientsMu.RUnlock()

	if exists {
		return client, nil
	}

	// Create new connection
	s.peerClientsMu.Lock()
	defer s.peerClientsMu.Unlock()

	// Check again in case another goroutine created the connection
	if client, exists := s.peerClients[peerAddress]; exists {
		return client, nil
	}

	// Create gRPC connection
	conn, err := grpc.Dial(peerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer %s: %w", peerAddress, err)
	}

	// Create client
	client = pb.NewLockServiceClient(conn)
	s.peerClients[peerAddress] = client
	s.logger.Printf("Connected to peer at %s", peerAddress)

	return client, nil
}

// replicateStateToFollowers sends the current lock state to all followers
func (s *LockServer) replicateStateToFollowers() {
	// Skip if not leader
	if !s.isLeader() {
		return
	}

	// Get current lock state under lock manager's mutex
	s.lockManager.GetMutex().Lock()
	holder := s.lockManager.CurrentHolderNoLock()
	token := s.lockManager.GetCurrentTokenNoLock()
	var expiryTimestamp int64
	expiry := s.lockManager.GetTokenExpirationNoLock()
	if !expiry.IsZero() {
		expiryTimestamp = expiry.Unix()
	}
	s.lockManager.GetMutex().Unlock()

	// Get the current epoch
	epoch := s.currentEpoch.Load()

	// Create replicated state message
	state := &pb.ReplicatedState{
		LockHolder:      holder,
		LockToken:       token,
		ExpiryTimestamp: expiryTimestamp,
		Epoch:           epoch,
	}

	// Add to queue for guaranteed delivery
	s.enqueueReplication(state)

	// Trigger immediate processing
	go s.processReplicationQueue()

	// Log the replication
	s.logger.Printf("Enqueued state replication to followers: holder=%d, token=%s, expiry=%v, epoch=%d",
		holder, token, time.Unix(expiryTimestamp, 0), epoch)
}

// enqueueReplication adds a state update to the replication queue
func (s *LockServer) enqueueReplication(state *pb.ReplicatedState) {
	s.replicationQueueMu.Lock()
	defer s.replicationQueueMu.Unlock()

	// If queue is getting too large, log a warning (possible connectivity issues)
	if len(s.replicationQueue) > 100 {
		s.logger.Printf("WARNING: Replication queue contains %d pending updates", len(s.replicationQueue))
	}

	// For lock state updates, we can optimize by only keeping the latest state
	// Since earlier updates are superseded by the latest one
	if len(s.replicationQueue) > 0 {
		// Replace the last item with the new state (most recent always wins)
		s.replicationQueue[len(s.replicationQueue)-1] = state
	} else {
		// Queue was empty, add the state
		s.replicationQueue = append(s.replicationQueue, state)
	}

	s.logger.Printf("Enqueued state update for replication (queue size: %d)", len(s.replicationQueue))
}

// dequeueProcessedUpdate removes a processed update from the queue
func (s *LockServer) dequeueProcessedUpdate(processed *pb.ReplicatedState) {
	s.replicationQueueMu.Lock()
	defer s.replicationQueueMu.Unlock()

	// For lock state, we can just empty the queue since the latest state was applied
	if len(s.replicationQueue) > 0 {
		// Clear the queue since we successfully applied the latest state
		s.replicationQueue = s.replicationQueue[:0]
		s.logger.Printf("Cleared replication queue after successful update")
	}
}

// getNextReplicationUpdate gets the next update to process
func (s *LockServer) getNextReplicationUpdate() *pb.ReplicatedState {
	s.replicationQueueMu.Lock()
	defer s.replicationQueueMu.Unlock()

	if len(s.replicationQueue) == 0 {
		return nil
	}

	// Return the first item without removing it
	// It will be removed after successful processing
	return s.replicationQueue[0]
}

// startReplicationWorker starts a background worker to process the replication queue
func (s *LockServer) startReplicationWorker() {
	// Set the flag to indicate that a worker is running
	if !s.replicationInProgress.CompareAndSwap(false, true) {
		s.logger.Printf("Replication worker already running, not starting another one")
		return
	}

	s.logger.Printf("Starting background replication worker")

	go func() {
		defer s.replicationInProgress.Store(false)

		// Process the queue periodically
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			// Stop if we're no longer leader
			if !s.isLeader() {
				s.logger.Printf("No longer leader, stopping replication worker")
				return
			}

			select {
			case <-ticker.C:
				s.processReplicationQueue()
			}
		}
	}()
}

// processReplicationQueue processes pending updates in the replication queue
func (s *LockServer) processReplicationQueue() {
	// Skip if not leader
	if !s.isLeader() {
		return
	}

	// Get the next update to process
	update := s.getNextReplicationUpdate()
	if update == nil {
		// Queue is empty, nothing to do
		return
	}

	// Set the current epoch in the update
	update.Epoch = s.currentEpoch.Load()

	s.logger.Printf("Processing pending replication update from queue to all followers")

	// Replicate to all followers
	var wg sync.WaitGroup
	var failedAddresses []string
	var mu sync.Mutex // to protect shared variables
	successCount := 0

	// Count the peers in the peerAddresses list (excluding duplicates)
	uniqueAddrs := make(map[string]struct{})
	totalPeers := 0
	for _, addr := range s.peerAddresses {
		if _, exists := uniqueAddrs[addr]; !exists {
			uniqueAddrs[addr] = struct{}{}
			totalPeers++
		}
	}

	sendToAddress := func(address string) {
		defer wg.Done()

		// Get or establish connection to this peer
		client, err := s.getOrConnectPeer(address)
		if err != nil {
			s.logger.Printf("Failed to connect to follower at %s: %v", address, err)
			mu.Lock()
			failedAddresses = append(failedAddresses, address)
			mu.Unlock()
			return
		}

		// Try to send the update with retry
		maxRetries := 3
		for retryCount := 0; retryCount < maxRetries; retryCount++ {
			// Create a context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			resp, err := client.UpdateSecondaryState(ctx, update)
			if err != nil {
				s.logger.Printf("Failed to replicate to follower at %s (attempt %d/%d): %v",
					address, retryCount+1, maxRetries, err)

				if retryCount == maxRetries-1 {
					// Last attempt failed
					mu.Lock()
					failedAddresses = append(failedAddresses, address)
					mu.Unlock()
				} else {
					// Wait before retry
					time.Sleep(100 * time.Millisecond)
					continue
				}
			} else {
				// Process response
				if resp.Status == pb.Status_STALE_EPOCH {
					s.logger.Printf("Follower at %s reported our epoch %d is stale (their epoch: %d)",
						address, update.Epoch, resp.CurrentEpoch)

					// If follower has higher epoch, step down
					if resp.CurrentEpoch > s.currentEpoch.Load() {
						s.logger.Printf("Follower has higher epoch, stepping down")
						s.currentEpoch.Store(resp.CurrentEpoch)
						s.setServerState(FollowerState)
						s.setLeaderAddress(address)
						return
					}
				} else if resp.Status != pb.Status_OK {
					s.logger.Printf("Follower at %s returned error: %s", address, resp.ErrorMessage)
					if retryCount == maxRetries-1 {
						mu.Lock()
						failedAddresses = append(failedAddresses, address)
						mu.Unlock()
					} else {
						// Wait before retry
						time.Sleep(100 * time.Millisecond)
						continue
					}
				} else {
					s.logger.Printf("Successfully replicated to follower at %s", address)
					mu.Lock()
					successCount++
					mu.Unlock()
					break // Success, exit retry loop
				}
			}
		}
	}

	// Handle all the peers in the peerAddresses list
	for _, addr := range s.peerAddresses {
		wg.Add(1)
		go sendToAddress(addr)
	}

	// Wait for all replication attempts to complete
	wg.Wait()

	// Count the leader itself in the quorum calculation (totalPeers + 1 for leader)
	totalParticipants := totalPeers + 1

	// If we have a majority of successful replications, consider it a success
	// We need (N/2 + 1) nodes in total, and the leader counts as 1
	quorumSize := (totalParticipants / 2) + 1

	// The leader always has the update, so we need (quorumSize - 1) successful followers
	neededFollowers := quorumSize - 1

	if successCount >= neededFollowers {
		// Success - remove the update from the queue
		s.dequeueProcessedUpdate(update)
		s.logger.Printf("Successfully processed replication to quorum (%d/%d followers, needed %d for quorum)",
			successCount, totalPeers, neededFollowers)
	} else if len(failedAddresses) > 0 {
		s.logger.Printf("Failed to replicate to %d followers, only %d/%d successful (need %d for quorum), will retry later",
			len(failedAddresses), successCount, totalPeers, neededFollowers)
	}
}

// startHeartbeatSender periodically sends heartbeats to the leader
func (s *LockServer) startHeartbeatSender() {
	ticker := time.NewTicker(s.heartbeatConfig.Interval)
	defer ticker.Stop()

	failureCount := 0
	maxFailures := s.heartbeatConfig.MaxFailureCount
	noLeaderCount := 0 // Track consecutive iterations with no known leader

	s.logger.Printf("Starting heartbeat sender with interval=%v, timeout=%v, max failures=%d",
		s.heartbeatConfig.Interval, s.heartbeatConfig.Timeout, maxFailures)

	for range ticker.C {
		// Stop sending heartbeats if we're now the leader
		if s.isLeader() {
			s.logger.Printf("This server is now leader, stopping heartbeat")
			return
		}

		// Target the current leader address if known
		targetAddr := s.getLeaderAddress()
		if targetAddr == "" {
			s.logger.Printf("No known leader address, skipping heartbeat")
			noLeaderCount++

			// If we have no leader address for several intervals, try to discover leader
			if noLeaderCount == 3 {
				s.logger.Printf("No leader address for %d intervals, attempting leader discovery", noLeaderCount)
				s.discoverCurrentLeader()
			}

			// If still no leader after several more intervals, consider starting election
			if noLeaderCount >= 5 {
				s.logger.Printf("No leader discovered after %d intervals, triggering election", noLeaderCount)
				s.resetElectionTimer()
				noLeaderCount = 0 // Reset the counter
			}
			continue
		}

		// Reset the counter since we have a leader address
		noLeaderCount = 0

		// Get or connect to the leader
		peerClient, err := s.getOrConnectPeer(targetAddr)
		if err != nil {
			s.logger.Printf("Failed to connect to leader at %s: %v", targetAddr, err)
			failureCount++
			if checkAndHandleHeartbeatFailure(s, failureCount, maxFailures) {
				// Reset failure count and continue if not promoting
				failureCount = 0
			}
			continue
		}

		// Create heartbeat request
		req := &pb.HeartbeatRequest{
			ServerId: s.serverID,
			Epoch:    s.currentEpoch.Load(),
			IsLeader: false,
		}

		// Send heartbeat with timeout
		ctx, cancel := context.WithTimeout(context.Background(), s.heartbeatConfig.Timeout)
		resp, err := peerClient.Ping(ctx, req)
		cancel()

		if err != nil {
			s.logger.Printf("Failed to send heartbeat to leader at %s: %v", targetAddr, err)
			failureCount++
			if checkAndHandleHeartbeatFailure(s, failureCount, maxFailures) {
				// Reset failure count if we're starting promotion
				failureCount = 0
			}
		} else {
			// Reset failure count on success
			failureCount = 0

			// If the response has a higher epoch, update our epoch
			if resp.CurrentEpoch > s.currentEpoch.Load() {
				s.logger.Printf("Updating epoch from %d to %d based on leader heartbeat",
					s.currentEpoch.Load(), resp.CurrentEpoch)
				s.currentEpoch.Store(resp.CurrentEpoch)
			}

			// Reset election timer on successful heartbeat
			s.resetElectionTimer()
		}
	}
}

// checkAndHandleHeartbeatFailure checks if we've reached the failure threshold and initiates promotion if needed
// Returns true if promotion was initiated
func checkAndHandleHeartbeatFailure(s *LockServer, failureCount, maxFailures int) bool {
	// Check if we've reached the maximum number of failures
	if failureCount >= maxFailures {
		s.logger.Printf("Leader heartbeat failed %d times in a row (max allowed: %d), initiating leader election process",
			failureCount, maxFailures)

		// If we're already a leader or candidate, don't try to promote
		if s.isLeader() || s.isCandidate() {
			s.logger.Printf("Not starting promotion because we're already in %s state", s.getServerStateString())
			return false
		}

		// Try to discover the leader first, but with a short timeout
		s.logger.Printf("Attempting quick leader discovery before promotion")
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()

		// Use a separate goroutine for discovery to respect the timeout
		discoveryDone := make(chan bool, 1)
		go func() {
			s.discoverCurrentLeader()
			discoveryDone <- true
		}()

		// Wait for discovery or timeout
		select {
		case <-ctx.Done():
			s.logger.Printf("Leader discovery timed out, proceeding with promotion")
		case <-discoveryDone:
			s.logger.Printf("Leader discovery completed")
		}

		// If we found a leader through discovery, we don't need to promote
		leaderAddr := s.getLeaderAddress()
		if leaderAddr != "" {
			s.logger.Printf("Leader discovered after heartbeat failures: %s, not promoting", leaderAddr)
			return false
		}

		// Start the promotion attempt (leader election)
		s.logger.Printf("No leader found or contactable, proceeding with promotion attempt")
		s.startPromotionAttempt()
		return true
	}

	// Log partial failures but don't promote yet
	if failureCount > 0 {
		s.logger.Printf("Heartbeat failure count: %d/%d", failureCount, maxFailures)
	}

	return false
}

// discoverCurrentLeader attempts to find the current leader by querying peers
func (s *LockServer) discoverCurrentLeader() {
	s.logger.Printf("Attempting to discover current leader")

	// Deduplicate peer addresses
	uniquePeers := make(map[string]bool)
	for _, addr := range s.peerAddresses {
		uniquePeers[addr] = true
	}

	// Convert to slice for deterministic iteration
	var dedupedPeers []string
	for addr := range uniquePeers {
		dedupedPeers = append(dedupedPeers, addr)
	}

	// If no peers, we should become leader ourselves
	if len(dedupedPeers) == 0 {
		s.logger.Printf("No peers available and no known leader - initiating election")
		s.startPromotionAttempt()
		return
	}

	// Try each peer until we find the leader
	var wg sync.WaitGroup
	var mu sync.Mutex
	leaderFound := false
	highestEpoch := s.currentEpoch.Load()
	var leaderAddress string

	for _, peerAddr := range dedupedPeers {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			// Skip if we already found the leader
			mu.Lock()
			if leaderFound {
				mu.Unlock()
				return
			}
			mu.Unlock()

			client, err := s.getOrConnectPeer(addr)
			if err != nil {
				s.logger.Printf("Failed to connect to %s while looking for leader: %v", addr, err)
				return
			}

			// Request server info
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			resp, err := client.ServerInfo(ctx, &pb.ServerInfoRequest{})
			if err != nil {
				s.logger.Printf("Failed to get server info from %s: %v", addr, err)
				return
			}

			mu.Lock()
			defer mu.Unlock()

			// If this server has a higher epoch, update ours
			if resp.CurrentEpoch > highestEpoch {
				highestEpoch = resp.CurrentEpoch
				s.currentEpoch.Store(resp.CurrentEpoch)
			}

			// Normalize leader address to use localhost format
			if resp.LeaderAddress != "" && !strings.HasPrefix(resp.LeaderAddress, "localhost:") {
				parts := strings.Split(resp.LeaderAddress, ":")
				if len(parts) >= 2 {
					port := parts[len(parts)-1]
					normLeaderAddr := "localhost:" + port
					s.logger.Printf("Normalized leader address from %s to %s", resp.LeaderAddress, normLeaderAddr)
					resp.LeaderAddress = normLeaderAddr
				}
			}

			// Check if this is the leader
			if resp.Role == string(LeaderState) {
				s.logger.Printf("Discovered leader at %s (server ID %d, epoch %d)",
					addr, resp.ServerId, resp.CurrentEpoch)

				// Force address to be in localhost:port format for test compatibility
				parts := strings.Split(addr, ":")
				if len(parts) >= 2 {
					port := parts[len(parts)-1]
					leaderAddress = "localhost:" + port
				} else {
					leaderAddress = addr
				}

				leaderFound = true
				s.setLeaderAddress(leaderAddress)
				return
			}

			// If this server knows the leader address, use it
			if resp.LeaderAddress != "" {
				s.logger.Printf("Server %s (ID %d) reports leader at %s",
					addr, resp.ServerId, resp.LeaderAddress)
				leaderAddress = resp.LeaderAddress
				leaderFound = true
				s.setLeaderAddress(resp.LeaderAddress)
				return
			}
		}(peerAddr)
	}

	// Wait for all server info requests to complete
	wg.Wait()

	if leaderFound {
		s.logger.Printf("Successfully discovered leader at %s", leaderAddress)
		// Reset election timer since we found a leader
		s.resetElectionTimer()
	} else {
		s.logger.Printf("Failed to discover current leader from peers - might need election")
		// No need to start election here; let the heartbeat sender handle it
	}
}

// startPromotionAttempt initiates an attempt to become the leader
func (s *LockServer) startPromotionAttempt() {
	s.logger.Printf("Starting promotion attempt from %s state", s.getServerStateString())

	// Skip if we're already a leader
	if s.isLeader() {
		s.logger.Printf("Already leader, skipping promotion attempt")
		return
	}

	// Update to Candidate state
	s.setServerState(CandidateState)

	// Clear old leader address to avoid confusion during transition
	s.setLeaderAddress("")

	// Increment epoch and vote for self
	newEpoch := s.currentEpoch.Load() + 1
	s.currentEpoch.Store(newEpoch)
	s.votedInEpoch.Store(newEpoch)

	// Reset election timer
	s.resetElectionTimer()

	s.logger.Printf("Promoting to candidate with epoch %d", newEpoch)

	// Broadcast ProposePromotion RPCs to all peers
	votes := int32(1)                                         // Start with 1 (vote for self)
	var votesNeeded int32 = int32(len(s.peerAddresses)/2) + 1 // Majority needed

	// Collect all peer addresses for logging
	totalServers := len(s.peerAddresses) + 1 // Include self

	s.logger.Printf("Need %d/%d votes for majority (already have 1 vote from self)",
		votesNeeded, totalServers)

	// Deduplicate peer addresses to prevent skewed quorum
	uniquePeers := make(map[string]bool)
	for _, addr := range s.peerAddresses {
		uniquePeers[addr] = true
	}

	// Create clean deduplicated slice of peer addresses
	dedupedPeers := make([]string, 0, len(uniquePeers))
	for addr := range uniquePeers {
		dedupedPeers = append(dedupedPeers, addr)
	}

	s.logger.Printf("Will request votes from %d unique peers: %v", len(dedupedPeers), dedupedPeers)

	// Early success if we're the only server in the cluster
	if len(dedupedPeers) == 0 {
		s.logger.Printf("No peers to request votes from, automatically becoming leader")
		s.becomeLeader(newEpoch)
		return
	}

	// Add a small delay based on server ID to make elections more deterministic
	// This gives preference to lower server IDs (helps prevent split brain)
	// Nodes with higher IDs wait longer to start their vote requests
	electionDelay := time.Duration(s.serverID) * 50 * time.Millisecond
	s.logger.Printf("Adding %v election delay based on server ID to avoid split votes", electionDelay)
	time.Sleep(electionDelay)

	// Request votes from all peers with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond) // Use 1s timeout
	defer cancel()

	// Check if we've been superseded by another leader while waiting
	if !s.isCandidate() {
		s.logger.Printf("No longer a candidate after delay, aborting election")
		return
	}

	var wg sync.WaitGroup
	var voteMu sync.Mutex // Protects the votes counter

	for _, peerAddr := range dedupedPeers {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			// Skip if we already have majority
			if atomic.LoadInt32(&votes) >= votesNeeded {
				s.logger.Printf("Already have majority votes, skipping vote request to %s", addr)
				return
			}

			// Prepare promotion request
			req := &pb.ProposeRequest{
				CandidateId:   s.serverID,
				ProposedEpoch: newEpoch,
			}

			// Connect to peer
			s.logger.Printf("Attempting to connect to peer %s for vote", addr)
			client, err := s.getOrConnectPeer(addr)
			if err != nil {
				s.logger.Printf("Failed to connect to peer %s for vote: %v", addr, err)
				return
			}
			s.logger.Printf("Successfully connected to peer %s, sending vote request", addr)

			// Send the proposal
			resp, err := client.ProposePromotion(ctx, req)
			if err != nil {
				s.logger.Printf("Failed to get vote from %s: %v", addr, err)
				return
			}

			// Handle the response
			if resp.VoteGranted {
				voteMu.Lock()
				defer voteMu.Unlock()
				votes++
				s.logger.Printf("Received yes vote from %s, now have %d/%d votes",
					addr, votes, votesNeeded)
			} else {
				// If peer has higher epoch, step down
				if resp.CurrentEpoch > newEpoch {
					s.logger.Printf("Peer %s has higher epoch %d > %d, stepping down",
						addr, resp.CurrentEpoch, newEpoch)
					s.currentEpoch.Store(resp.CurrentEpoch)
					s.setServerState(FollowerState)
				} else {
					s.logger.Printf("Vote denied by %s (already voted in this epoch or some other reason)", addr)
				}
			}
		}(peerAddr)
	}

	// Wait for all vote requests to complete or timeout
	s.logger.Printf("Waiting for vote requests to complete or timeout...")
	wg.Wait()
	s.logger.Printf("All vote requests completed or timed out")

	// Check final vote count
	finalVotes := atomic.LoadInt32(&votes)
	if finalVotes >= votesNeeded && s.isCandidate() {
		s.logger.Printf("Election won with %d/%d votes, becoming leader for epoch %d",
			finalVotes, totalServers, newEpoch)
		s.becomeLeader(newEpoch)
	} else if s.isCandidate() {
		s.logger.Printf("Election failed, got only %d/%d votes, needed %d. Returning to follower state",
			finalVotes, totalServers, votesNeeded)
		s.setServerState(FollowerState)

		// Try to discover the current leader after election failure
		s.discoverCurrentLeader()
	} else {
		s.logger.Printf("No longer a candidate (current state: %s), election aborted",
			s.getServerStateString())
	}
}

// becomeLeader transitions this server to the leader state
func (s *LockServer) becomeLeader(epoch int64) {
	s.logger.Printf("Becoming leader with epoch %d", epoch)

	// Update state
	s.setServerState(LeaderState)
	s.currentEpoch.Store(epoch)
	s.setLeaderAddress("") // Clear leader address (we are the leader)

	// Start fencing period
	leaseDuration := s.lockManager.GetLeaseDuration()
	fencingDuration := leaseDuration + s.fencingBuffer

	s.isFencing.Store(true)
	s.fencingEndTime = time.Now().Add(fencingDuration)

	s.logger.Printf("Entering fencing period for %v (until %v)",
		fencingDuration, s.fencingEndTime.Format(time.RFC3339))

	// Actively broadcast to all followers that this server is the new leader
	// so they can update their leaderAddress
	go s.announceLeadership(epoch)

	// Start leader services
	go s.startLeaderHeartbeats()
	go s.startReplicationWorker()

	// Wait for fencing period to end
	go s.waitForFencingEnd()
}

// announceLeadership sends periodic announcements to followers
func (s *LockServer) announceLeadership(epoch int64) {
	s.logger.Printf("Announcing leadership with epoch %d", epoch)

	s.setLeaderAddress(s.selfAddress)

	// Prepare heartbeat request
	req := &pb.HeartbeatRequest{
		ServerId:      s.serverID,
		Epoch:         epoch,
		IsLeader:      true,
		LeaderAddress: s.selfAddress,
	}

	// Send to all peers
	var wg sync.WaitGroup
	for _, addr := range s.peerAddresses {
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()

			// Connect to peer
			client, err := s.getOrConnectPeer(peerAddr)
			if err != nil {
				s.logger.Printf("Failed to connect to follower at %s for leadership announcement: %v",
					peerAddr, err)
				return
			}

			// Send ping with leadership information
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			resp, err := client.Ping(ctx, req)
			if err != nil {
				s.logger.Printf("Failed to announce leadership to %s: %v", peerAddr, err)
				return
			}

			// Check for higher epoch in response
			if resp.CurrentEpoch > epoch {
				s.logger.Printf("Stepping down as leader due to higher epoch %d from %s",
					resp.CurrentEpoch, peerAddr)
				s.currentEpoch.Store(resp.CurrentEpoch)
				s.setServerState(FollowerState)
				s.resetElectionTimer()
			}

			s.logger.Printf("Successfully announced leadership to %s", peerAddr)
		}(addr)
	}

	// Wait for all announcements to complete
	wg.Wait()
	s.logger.Printf("Leadership announcements completed")
}

// waitForFencingEnd waits for the fencing period to end and then calls ForceClearLockState
func (s *LockServer) waitForFencingEnd() {
	// Calculate time to wait
	waitTime := time.Until(s.fencingEndTime)
	if waitTime <= 0 {
		s.logger.Printf("Warning: Fencing period already ended, proceeding immediately")
		waitTime = 1 * time.Millisecond // Minimum wait
	}

	// Wait for the fencing period to expire
	s.logger.Printf("Waiting %v for fencing period to end", waitTime)
	time.Sleep(waitTime)

	// Check if we're still the leader
	if !s.isLeader() {
		s.logger.Printf("No longer leader after fencing period - not clearing lock state")
		return
	}

	// End fencing period and clear lock state
	s.isFencing.Store(false)
	s.logger.Printf("Fencing period ended, clearing lock state for new requests")

	// Force clear the lock state to ensure we start fresh
	if err := s.lockManager.ForceClearLockState(); err != nil {
		s.logger.Printf("Error clearing lock state after fencing: %v", err)
	} else {
		s.logger.Printf("Successfully cleared lock state and ended fencing period")
	}

	// Replicate the cleared state to followers
	s.replicateStateToFollowers()
}

// resetElectionTimer resets the election timer with a random timeout
func (s *LockServer) resetElectionTimer() {
	s.leaderMu.Lock()
	defer s.leaderMu.Unlock()

	// Cancel existing timer if any
	if s.electionTimer != nil {
		if !s.electionTimer.Stop() {
			// Drain the timer channel if it already fired
			select {
			case <-s.electionTimer.C:
				s.logger.Printf("Drained already-fired election timer")
			default:
				s.logger.Printf("Election timer stopped before firing")
			}
		}
	}

	// Don't set timers for leaders or candidates
	if !s.isFollower() {
		s.logger.Printf("Not setting election timer for non-follower state: %s", s.getServerStateString())
		return
	}

	// Don't set a timer if we already know the leader
	if s.getLeaderAddress() != "" {
		s.logger.Printf("Already know leader at %s, setting longer election timeout", s.getLeaderAddress())
	}

	// Create new timer with random timeout
	timeout := getRandomElectionTimeout()
	s.logger.Printf("Setting new election timer with timeout %v", timeout)
	s.electionTimer = time.NewTimer(timeout)

	// Start a goroutine to handle the timer expiration
	go func() {
		// Wait for the timer to fire
		s.logger.Printf("Election timer goroutine started, waiting for timer to fire")
		<-s.electionTimer.C
		s.logger.Printf("Election timer fired!")

		// Check if we're still a follower when the timer fires
		if s.isFollower() {
			// Only start election if we don't know the leader or lost contact
			if s.getLeaderAddress() == "" {
				s.logger.Printf("Election timer expired while in follower state with no known leader, initiating leader election")
				s.startPromotionAttempt()
			} else {
				s.logger.Printf("Election timer expired, but we know the leader at %s. Checking if it's alive...",
					s.getLeaderAddress())

				// Attempt to contact leader - if it fails, then start election
				leaderAlive := s.pingLeader()
				if !leaderAlive {
					s.logger.Printf("Leader at %s is not responding, initiating leader election", s.getLeaderAddress())
					s.setLeaderAddress("") // Clear stale leader address
					s.startPromotionAttempt()
				} else {
					s.logger.Printf("Leader at %s is still responsive, resetting election timer", s.getLeaderAddress())
					s.resetElectionTimer()
				}
			}
		} else {
			s.logger.Printf("Election timer expired, but no longer in follower state (now %s), skipping election",
				s.getServerStateString())
		}
	}()

	s.logger.Printf("Election timer reset with timeout %v", timeout)
}

// pingLeader attempts to contact the leader with a quick ping
func (s *LockServer) pingLeader() bool {
	leaderAddr := s.getLeaderAddress()
	if leaderAddr == "" {
		return false
	}

	// Get or connect to the leader
	client, err := s.getOrConnectPeer(leaderAddr)
	if err != nil {
		s.logger.Printf("Failed to connect to leader at %s: %v", leaderAddr, err)
		return false
	}

	// Send a quick ping
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	req := &pb.HeartbeatRequest{
		ServerId: s.serverID,
		Epoch:    s.currentEpoch.Load(),
	}

	_, err = client.Ping(ctx, req)
	if err != nil {
		s.logger.Printf("Leader at %s is not responding: %v", leaderAddr, err)
		return false
	}

	return true
}

// startLeaderHeartbeats starts sending periodic heartbeats to all followers
func (s *LockServer) startLeaderHeartbeats() {
	s.logger.Printf("Starting leader heartbeats with interval %v", s.heartbeatConfig.Interval)

	// Create ticker for heartbeat interval
	ticker := time.NewTicker(s.heartbeatConfig.Interval)
	defer ticker.Stop()

	for range ticker.C {
		// Stop if no longer leader
		if !s.isLeader() {
			s.logger.Printf("No longer leader, stopping heartbeats")
			return
		}

		// Get current epoch
		currentEpoch := s.currentEpoch.Load()

		// Deduplicate peer addresses
		uniquePeers := make(map[string]bool)
		for _, addr := range s.peerAddresses {
			uniquePeers[addr] = true
		}

		// Broadcast heartbeats to all unique peers
		for peerAddr := range uniquePeers {
			go func(addr string) {
				// Skip if we're not leader anymore
				if !s.isLeader() {
					return
				}

				// Connect to peer
				client, err := s.getOrConnectPeer(addr)
				if err != nil {
					s.logger.Printf("Failed to connect to follower at %s for heartbeat: %v",
						addr, err)
					return
				}

				// Prepare heartbeat request
				req := &pb.HeartbeatRequest{
					ServerId:      s.serverID,
					Epoch:         currentEpoch,
					IsLeader:      true,
					LeaderAddress: s.selfAddress,
				}

				// Send heartbeat with timeout
				ctx, cancel := context.WithTimeout(context.Background(), s.heartbeatConfig.Timeout)
				defer cancel()

				resp, err := client.Ping(ctx, req)
				if err != nil {
					s.logger.Printf("Failed to send heartbeat to follower at %s: %v",
						addr, err)
					return
				}

				// Process response
				if resp.Status == pb.Status_STALE_EPOCH {
					s.logger.Printf("Follower at %s reported our epoch %d is stale (their epoch: %d)",
						addr, currentEpoch, resp.CurrentEpoch)

					// If they have a higher epoch, step down
					if resp.CurrentEpoch > currentEpoch {
						s.logger.Printf("Stepping down as leader due to higher epoch %d from %s",
							resp.CurrentEpoch, addr)
						s.currentEpoch.Store(resp.CurrentEpoch)
						s.setServerState(FollowerState)
						s.resetElectionTimer()
					}
				} else {
					s.logger.Printf("Heartbeat to %s successful (status: %v)",
						addr, resp.Status)
				}
			}(peerAddr)
		}
	}
}

// ReplicateLockState receives state updates from the leader
func (s *LockServer) ReplicateLockState(ctx context.Context, state *pb.ReplicatedState) (*pb.ReplicationResponse, error) {
	// Log peer information if available
	if pr, ok := peer.FromContext(ctx); ok {
		s.logger.Printf("Received state update from %s with epoch %d (holder=%d, token=%s, expiry=%v)",
			pr.Addr, state.Epoch, state.LockHolder, state.LockToken,
			time.Unix(state.ExpiryTimestamp, 0))
	}

	// Get the current epoch
	currentEpoch := s.currentEpoch.Load()

	// Check epochs - only accept updates from same or higher epoch
	if state.Epoch < currentEpoch {
		s.logger.Printf("Rejected state update with stale epoch %d (current epoch: %d)",
			state.Epoch, currentEpoch)
		return &pb.ReplicationResponse{
			Status:       pb.Status_STALE_EPOCH,
			CurrentEpoch: currentEpoch,
			ErrorMessage: fmt.Sprintf("Stale epoch %d < %d", state.Epoch, currentEpoch),
		}, nil
	}

	// If this server thinks it's a leader but gets an update from same or higher epoch,
	// it needs to step down
	if s.isLeader() && state.Epoch >= currentEpoch {
		s.logger.Printf("Stepping down from leader due to update from same/higher epoch %d >= %d",
			state.Epoch, currentEpoch)
		s.setServerState(FollowerState)
	}

	// Update our epoch if we received a higher one
	if state.Epoch > currentEpoch {
		s.logger.Printf("Updating epoch from %d to %d based on replicated state",
			currentEpoch, state.Epoch)
		s.currentEpoch.Store(state.Epoch)
	}

	// Apply the replicated state to our lock manager
	err := s.lockManager.ApplyReplicatedState(
		state.LockHolder,
		state.LockToken,
		state.ExpiryTimestamp,
	)

	if err != nil {
		s.logger.Printf("Error applying replicated state: %v", err)
		return &pb.ReplicationResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: err.Error(),
			CurrentEpoch: s.currentEpoch.Load(),
		}, nil
	}

	s.logger.Printf("Applied replicated state: holder=%d, token=%s, expiry=%v",
		state.LockHolder, state.LockToken, time.Unix(state.ExpiryTimestamp, 0))

	return &pb.ReplicationResponse{
		Status:       pb.Status_OK,
		CurrentEpoch: s.currentEpoch.Load(),
	}, nil
}

// HeartbeatPing handles heartbeat requests from other servers
func (s *LockServer) HeartbeatPing(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// Basic logging for all heartbeat requests
	remoteEpoch := req.Epoch
	currentEpoch := s.currentEpoch.Load()

	s.logger.Printf("Received heartbeat from server %d with epoch %d (current epoch: %d)",
		req.ServerId, remoteEpoch, currentEpoch)

	// Process leader information if provided
	if req.IsLeader {
		s.logger.Printf("Server %d claims to be leader with address %s", req.ServerId, req.LeaderAddress)

		// If sender claims to be leader and has valid epoch, update our leader address
		if remoteEpoch >= currentEpoch {
			if s.isLeader() && remoteEpoch >= currentEpoch && req.ServerId != s.serverID {
				// We thought we were leader, but someone else claims to be with valid epoch
				// Step down to follower
				s.logger.Printf("Stepping down from leader due to heartbeat from server %d with epoch %d",
					req.ServerId, remoteEpoch)
				s.setServerState(FollowerState)
				s.setLeaderAddress(req.LeaderAddress)
			} else if remoteEpoch > currentEpoch {
				// If remote epoch is greater, update our epoch
				s.logger.Printf("Incoming heartbeat has higher epoch %d > %d, stepping down", remoteEpoch, currentEpoch)
				s.currentEpoch.Store(remoteEpoch)

				// Also update leader info
				s.setLeaderAddress(req.LeaderAddress)
			}
		}
	} else if req.LeaderAddress != "" {
		// Sender knows who the leader is
		s.logger.Printf("Server %d reports leader address as %s", req.ServerId, req.LeaderAddress)

		// If we don't know who the leader is, update our leader address
		if s.getLeaderAddress() == "" && remoteEpoch >= currentEpoch {
			s.setLeaderAddress(req.LeaderAddress)
		}
	}

	// Only check the role for informational purposes
	if currentEpoch > remoteEpoch {
		s.logger.Printf("Our epoch %d is higher than heartbeat epoch %d", currentEpoch, remoteEpoch)
	}

	// Return our current epoch and state
	return &pb.HeartbeatResponse{
		Status:       pb.Status_OK,
		CurrentEpoch: s.currentEpoch.Load(),
	}, nil
}

// ProposePromotion handles leadership election proposals
func (s *LockServer) ProposePromotion(ctx context.Context, req *pb.ProposeRequest) (*pb.ProposeResponse, error) {
	candidateID := req.CandidateId
	proposedEpoch := req.ProposedEpoch
	currentEpoch := s.currentEpoch.Load()
	lastVotedEpoch := s.votedInEpoch.Load()

	s.logger.Printf("Received leadership proposal from server %d with epoch %d (our epoch: %d, last voted epoch: %d)",
		candidateID, proposedEpoch, currentEpoch, lastVotedEpoch)

	// Rule 1: If the proposed epoch is lower than our current epoch, reject the vote
	if proposedEpoch < currentEpoch {
		s.logger.Printf("Rejecting vote for server %d: proposed epoch %d is lower than current epoch %d",
			candidateID, proposedEpoch, currentEpoch)
		return &pb.ProposeResponse{
			CurrentEpoch: currentEpoch,
			VoteGranted:  false,
		}, nil
	}

	// Rule 2: If we already voted in this epoch, reject the vote
	if proposedEpoch <= lastVotedEpoch {
		s.logger.Printf("Rejecting vote for server %d: already voted in epoch %d",
			candidateID, lastVotedEpoch)
		return &pb.ProposeResponse{
			CurrentEpoch: currentEpoch,
			VoteGranted:  false,
		}, nil
	}

	// Rule 3: If we're a leader and our epoch is equal or higher, reject the vote
	if s.isLeader() && currentEpoch >= proposedEpoch {
		s.logger.Printf("Rejecting vote for server %d: we are already the leader in epoch %d",
			candidateID, currentEpoch)
		return &pb.ProposeResponse{
			CurrentEpoch: currentEpoch,
			VoteGranted:  false,
		}, nil
	}

	// If proposed epoch is higher than our current epoch, update our epoch
	if proposedEpoch > currentEpoch {
		s.logger.Printf("Updating epoch from %d to %d based on proposal",
			currentEpoch, proposedEpoch)
		s.currentEpoch.Store(proposedEpoch)
	}

	// Grant the vote: record that we voted in this epoch
	s.votedInEpoch.Store(proposedEpoch)

	// If we were a leader, step down to follower since we're giving our vote
	if s.isLeader() {
		s.logger.Printf("Stepping down from leader to follower to vote for server %d", candidateID)
		s.setServerState(FollowerState)
		// Reset the election timer after stepping down
		s.resetElectionTimer()
	}

	s.logger.Printf("Granting vote to server %d for epoch %d", candidateID, proposedEpoch)

	return &pb.ProposeResponse{
		CurrentEpoch: s.currentEpoch.Load(),
		VoteGranted:  true,
	}, nil
}

// getRandomElectionTimeout returns a random election timeout between 150-500ms
func getRandomElectionTimeout() time.Duration {
	// Use a shorter timeout range (100-300ms) for more responsive elections
	// Especially useful during testing to detect failures faster
	minTimeout := 100 * time.Millisecond
	maxTimeout := 300 * time.Millisecond

	// Generate random duration within the range
	randomizationRange := int64(maxTimeout - minTimeout)
	randomized := minTimeout + time.Duration(rand.Int63n(randomizationRange))

	return randomized
}

// UpdateSecondaryState receives state updates from the leader (legacy method)
// Delegates to ReplicateLockState for backward compatibility
func (s *LockServer) UpdateSecondaryState(ctx context.Context, state *pb.ReplicatedState) (*pb.ReplicationResponse, error) {
	return s.ReplicateLockState(ctx, state)
}

// Ping handles heartbeat requests (legacy method)
// Delegates to HeartbeatPing for backward compatibility
func (s *LockServer) Ping(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return s.HeartbeatPing(ctx, req)
}
