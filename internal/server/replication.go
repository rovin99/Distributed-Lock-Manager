package server

import (
	"context"
	"fmt"
	"math/rand"
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

// connectToPeer establishes a connection to the peer (legacy method)
func (s *LockServer) connectToPeer() error {
	if s.peerAddress == "" {
		return fmt.Errorf("no peer address configured")
	}

	// Create gRPC connection
	conn, err := grpc.Dial(s.peerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to peer: %w", err)
	}

	// Create client
	client := pb.NewLockServiceClient(conn)

	s.peerConn = conn
	s.peerClient = client
	s.logger.Printf("Connected to peer at %s", s.peerAddress)

	return nil
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
			// Stop if we're no longer leader/primary
			if (!s.isPrimary && !s.isLeader()) || s.isFollower() {
				s.logger.Printf("No longer leader/primary, stopping replication worker")
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
	// Skip if not leader/primary
	if (!s.isPrimary && !s.isLeader()) || s.isFollower() {
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
	var mu sync.Mutex // to protect failedAddresses

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

		// Try to send the update
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := client.UpdateSecondaryState(ctx, update)
		if err != nil {
			s.logger.Printf("Failed to replicate to follower at %s: %v", address, err)
			mu.Lock()
			failedAddresses = append(failedAddresses, address)
			mu.Unlock()
			return
		}

		// Process response
		if resp.Status == pb.Status_STALE_EPOCH {
			s.logger.Printf("Follower at %s reported our epoch %d is stale (their epoch: %d)",
				address, update.Epoch, resp.CurrentEpoch)

			// If follower has higher epoch, step down
			if resp.CurrentEpoch > s.currentEpoch.Load() {
				s.logger.Printf("Follower has higher epoch, stepping down")
				s.currentEpoch.Store(resp.CurrentEpoch)
				s.setServerState(FollowerState)
				s.isPrimary = false
				s.setLeaderAddress(address)
				return
			}
		} else if resp.Status != pb.Status_OK {
			s.logger.Printf("Follower at %s returned error: %s", address, resp.ErrorMessage)
			mu.Lock()
			failedAddresses = append(failedAddresses, address)
			mu.Unlock()
		} else {
			s.logger.Printf("Successfully replicated to follower at %s", address)
		}
	}

	// First, handle the legacy path if we have a peerClient
	if s.peerClient != nil && s.peerAddress != "" {
		wg.Add(1)
		go sendToAddress(s.peerAddress)
	}

	// Then handle all the peers in the peerAddresses list
	for _, addr := range s.peerAddresses {
		// Skip if it's the same as the legacy peerAddress
		if addr == s.peerAddress {
			continue
		}
		wg.Add(1)
		go sendToAddress(addr)
	}

	// Wait for all replication attempts to complete
	wg.Wait()

	// If we failed to replicate to some followers, leave update in the queue
	if len(failedAddresses) > 0 {
		s.logger.Printf("Failed to replicate to %d followers, will retry later", len(failedAddresses))
		return
	}

	// Success - remove the update from the queue
	s.dequeueProcessedUpdate(update)
	s.logger.Printf("Successfully processed pending replication update to all followers")
}

// startHeartbeatSender periodically sends heartbeats to the leader
func (s *LockServer) startHeartbeatSender() {
	ticker := time.NewTicker(s.heartbeatConfig.Interval)
	defer ticker.Stop()

	failureCount := 0
	maxFailures := s.heartbeatConfig.MaxFailureCount

	s.logger.Printf("Starting heartbeat sender with interval=%v, timeout=%v, max failures=%d",
		s.heartbeatConfig.Interval, s.heartbeatConfig.Timeout, maxFailures)

	for range ticker.C {
		// Stop sending heartbeats if we're now the leader
		if s.isLeader() {
			s.logger.Printf("This server is now leader, stopping heartbeat")
			return
		}

		// Target the current leader address if known, otherwise use the legacy peerAddress
		targetAddr := s.getLeaderAddress()
		if targetAddr == "" {
			targetAddr = s.peerAddress
		}

		var peerClient pb.LockServiceClient
		var err error

		// Get or establish connection to the target address
		if targetAddr == s.peerAddress && s.peerClient != nil {
			// Use legacy connection
			peerClient = s.peerClient
		} else {
			// Use the new getOrConnectPeer method
			peerClient, err = s.getOrConnectPeer(targetAddr)
			if err != nil {
				s.logger.Printf("Failed to connect to leader at %s: %v", targetAddr, err)
				failureCount++
				// Process failure
				if checkAndHandleHeartbeatFailure(s, failureCount, maxFailures) {
					return // Exit the heartbeat loop after promotion
				}
				continue
			}
		}

		// Create heartbeat request with current epoch
		req := &pb.HeartbeatRequest{
			ServerId: s.serverID,
			Epoch:    s.currentEpoch.Load(),
		}

		// Send the heartbeat
		ctx, cancel := context.WithTimeout(context.Background(), s.heartbeatConfig.Timeout)
		resp, err := peerClient.Ping(ctx, req)
		cancel()

		if err != nil {
			s.logger.Printf("Heartbeat to leader failed: %v", err)
			failureCount++
			// Process failure
			if checkAndHandleHeartbeatFailure(s, failureCount, maxFailures) {
				return // Exit the heartbeat loop after promotion
			}
			continue
		}

		// Successfully connected to the leader
		failureCount = 0 // Reset failure counter

		// Check if the response contains a higher epoch
		if resp.CurrentEpoch > s.currentEpoch.Load() {
			s.logger.Printf("Received higher epoch from leader: %d > %d, updating",
				resp.CurrentEpoch, s.currentEpoch.Load())
			s.currentEpoch.Store(resp.CurrentEpoch)
		} else if resp.Status == pb.Status_STALE_EPOCH {
			s.logger.Printf("Our epoch %d is stale according to leader (epoch %d)",
				s.currentEpoch.Load(), resp.CurrentEpoch)
			// Update our epoch to match the leader's
			s.currentEpoch.Store(resp.CurrentEpoch)
		}

		// Log heartbeat success with reduced frequency
		if time.Now().Second()%10 == 0 { // Log only every ~10 seconds
			s.logger.Printf("Heartbeat to leader successful (epoch: %d)", s.currentEpoch.Load())
		}
	}
}

// checkAndHandleHeartbeatFailure checks if we've reached the failure threshold and initiates promotion if needed
// Returns true if promotion was initiated
func checkAndHandleHeartbeatFailure(s *LockServer, failureCount, maxFailures int) bool {
	// Check if we've reached the maximum number of failures
	if failureCount >= maxFailures {
		s.logger.Printf("Leader heartbeat failed %d times in a row, initiating promotion", failureCount)

		// For backward compatibility, if we're in legacy mode, use the legacy promotion approach
		if s.getServerStateString() == "" {
			// Legacy promotion
			s.promoteToPrimary()
		} else {
			// New promotion approach - start an election
			s.startPromotionAttempt()
		}

		return true // Promotion initiated
	} else {
		s.logger.Printf("Heartbeat failure count: %d/%d", failureCount, maxFailures)
		return false // Continue sending heartbeats
	}
}

// startPromotionAttempt begins the process of attempting to become the leader
func (s *LockServer) startPromotionAttempt() {
	s.logger.Printf("Starting promotion attempt for server ID %d", s.serverID)

	// 1. Transition to Candidate state
	s.setServerState(CandidateState)

	// 2. Increment current epoch
	newEpoch := s.currentEpoch.Add(1)
	s.logger.Printf("Incremented epoch from %d to %d", newEpoch-1, newEpoch)

	// 3. Vote for self
	s.votedInEpoch.Store(newEpoch)

	// 4. Reset election timer in case we don't get majority
	s.resetElectionTimer()

	// 5. Calculate majority needed
	totalServers := len(s.peerAddresses) + 1 // +1 for self
	majority := (totalServers / 2) + 1
	s.logger.Printf("Need %d votes out of %d servers for majority", majority, totalServers)

	// 6. Create the promotion request
	req := &pb.ProposeRequest{
		CandidateId:   s.serverID,
		ProposedEpoch: newEpoch,
	}

	// 7. Track vote responses
	var votesMu sync.Mutex
	var grantedVotes int32 = 1 // Already voted for self
	var highestEpochSeen int64 = newEpoch

	// 8. Function to process a vote response
	processVoteResponse := func(resp *pb.ProposeResponse, peerAddr string) {
		votesMu.Lock()
		defer votesMu.Unlock()

		// Check if response has a higher epoch
		if resp.CurrentEpoch > highestEpochSeen {
			highestEpochSeen = resp.CurrentEpoch
			s.logger.Printf("Discovered higher epoch %d from %s", resp.CurrentEpoch, peerAddr)
		}

		// Count vote if granted
		if resp.VoteGranted {
			atomic.AddInt32(&grantedVotes, 1)
			s.logger.Printf("Received vote from %s, total votes: %d/%d",
				peerAddr, atomic.LoadInt32(&grantedVotes), majority)

			// Check if we have majority
			if atomic.LoadInt32(&grantedVotes) >= int32(majority) {
				// We've won the election!
				s.becomeLeader(newEpoch)
			}
		} else {
			s.logger.Printf("Vote denied by %s (their epoch: %d)", peerAddr, resp.CurrentEpoch)
		}
	}

	// 9. Broadcast vote requests to all peers
	for _, peerAddr := range s.peerAddresses {
		go func(addr string) {
			// Get or establish connection
			client, err := s.getOrConnectPeer(addr)
			if err != nil {
				s.logger.Printf("Failed to connect to %s for vote request: %v", addr, err)
				return
			}

			// Send vote request
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			resp, err := client.ProposePromotion(ctx, req)
			if err != nil {
				s.logger.Printf("Failed to send vote request to %s: %v", addr, err)
				return
			}

			// Process response
			processVoteResponse(resp, addr)

			// If they had a higher epoch, adopt it and become follower
			if resp.CurrentEpoch > newEpoch {
				s.logger.Printf("Stepping down due to higher epoch %d from %s",
					resp.CurrentEpoch, addr)
				s.currentEpoch.Store(resp.CurrentEpoch)
				s.setServerState(FollowerState)
				s.resetElectionTimer()
			}
		}(peerAddr)
	}

	// 10. For backward compatibility, also request vote from legacy peer
	if s.peerClient != nil && s.peerAddress != "" {
		// Skip if it's already in peerAddresses
		found := false
		for _, addr := range s.peerAddresses {
			if addr == s.peerAddress {
				found = true
				break
			}
		}

		if !found {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				resp, err := s.peerClient.ProposePromotion(ctx, req)
				if err != nil {
					s.logger.Printf("Failed to send vote request to legacy peer %s: %v",
						s.peerAddress, err)
					return
				}

				processVoteResponse(resp, s.peerAddress)

				if resp.CurrentEpoch > newEpoch {
					s.logger.Printf("Stepping down due to higher epoch %d from legacy peer",
						resp.CurrentEpoch)
					s.currentEpoch.Store(resp.CurrentEpoch)
					s.setServerState(FollowerState)
					s.resetElectionTimer()
				}
			}()
		}
	}
}

// promoteToPrimary promotes this server from secondary to primary
// Legacy function for backward compatibility
func (s *LockServer) promoteToPrimary() {
	s.logger.Printf("Promoting server %d from secondary to primary (legacy method)", s.serverID)

	s.isPrimary = true
	s.role = PrimaryRole

	// Start fencing period with detailed logging
	leaseDuration := s.lockManager.GetLeaseDuration()
	fencingDuration := leaseDuration + s.fencingBuffer
	s.fencingEndTime = time.Now().Add(fencingDuration)
	s.isFencing.Store(true)

	// Log all the timing details
	now := time.Now()
	s.logger.Printf("DEBUG FENCING SETUP: Now=%v, FencingEndTime=%v, Duration=%v, LeaseDuration=%v, Buffer=%v",
		now, s.fencingEndTime, fencingDuration, leaseDuration, s.fencingBuffer)
	s.logger.Printf("Entering fencing period for %v (until %v)",
		fencingDuration, s.fencingEndTime)

	// Attempt to log the current lock state before fencing
	s.logCurrentLockState("Before fencing")

	// Start a goroutine to end the fencing period
	go s.waitForFencingEnd()
}

// becomeLeader transitions this server to the leader state after winning an election
func (s *LockServer) becomeLeader(epoch int64) {
	// Check if we're already leader or have seen a higher epoch
	if s.isLeader() || s.currentEpoch.Load() > epoch {
		return
	}

	s.logger.Printf("Won election for epoch %d! Becoming leader", epoch)

	// Update state
	s.setServerState(LeaderState)
	s.isPrimary = true
	s.role = PrimaryRole

	// Clear the request cache to avoid stale responses
	if s.requestCache != nil {
		s.logger.Printf("Clearing request cache on leadership transition")
		s.requestCache.Clear()
	}

	// Enter fencing period
	leaseDuration := s.lockManager.GetLeaseDuration()
	fencingDuration := leaseDuration + s.fencingBuffer
	s.fencingEndTime = time.Now().Add(fencingDuration)
	s.isFencing.Store(true)

	// Log timing details
	s.logger.Printf("Entering fencing period for %v (until %v)",
		fencingDuration, s.fencingEndTime)

	// Log current state
	s.logCurrentLockState("Before fencing")

	// Register file manager's lease expiry callback to ensure it's properly set up
	if s.fileManager != nil {
		s.logger.Printf("Registering file manager lease expiry callbacks for new leader")
		s.fileManager.RegisterForLeaseExpiry()
	}

	// Start a goroutine to end the fencing period
	go s.waitForFencingEnd()

	// Start replicating to followers
	go s.startReplicationWorker()

	// Start periodic heartbeats to followers to maintain leadership
	go s.startLeaderHeartbeats()

	// Start the split-brain checker to detect potential split-brain scenarios
	go s.startSplitBrainChecker()
}

// startLeaderHeartbeats sends periodic heartbeats to followers to maintain leadership
func (s *LockServer) startLeaderHeartbeats() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	s.logger.Printf("Starting leader heartbeats")

	for range ticker.C {
		// Stop if we're no longer leader
		if !s.isLeader() {
			s.logger.Printf("No longer leader, stopping heartbeats")
			return
		}

		// Current epoch to send in heartbeats
		currentEpoch := s.currentEpoch.Load()

		// Send heartbeats to all peers
		for _, peerAddr := range s.peerAddresses {
			go func(addr string) {
				client, err := s.getOrConnectPeer(addr)
				if err != nil {
					s.logger.Printf("Failed to connect to follower %s for heartbeat: %v", addr, err)
					return
				}

				req := &pb.HeartbeatRequest{
					ServerId: s.serverID,
					Epoch:    currentEpoch,
				}

				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()

				resp, err := client.Ping(ctx, req)
				if err != nil {
					s.logger.Printf("Failed to send heartbeat to %s: %v", addr, err)
					return
				}

				// If follower reports higher epoch, step down
				if resp.CurrentEpoch > currentEpoch {
					s.logger.Printf("Follower %s has higher epoch %d > %d, stepping down",
						addr, resp.CurrentEpoch, currentEpoch)
					s.currentEpoch.Store(resp.CurrentEpoch)
					s.setServerState(FollowerState)
					s.isPrimary = false
					s.setLeaderAddress(addr)
					s.resetElectionTimer()
				}
			}(peerAddr)
		}
	}
}

// waitForFencingEnd waits for the fencing period to end
func (s *LockServer) waitForFencingEnd() {
	timeToWait := time.Until(s.fencingEndTime)
	if timeToWait <= 0 {
		timeToWait = 1 * time.Millisecond // Minimum wait
	}

	s.logger.Printf("Scheduling fencing end in %v", timeToWait)
	time.Sleep(timeToWait)

	s.logger.Printf("Fencing period ended, clearing lock state")
	s.isFencing.Store(false)

	// Log the lock state before clearing
	s.logCurrentLockState("Before forced clear")

	// Clear lock state to ensure safe operation
	if err := s.lockManager.ForceClearLockState(); err != nil {
		s.logger.Printf("Warning: Failed to clear lock state after fencing: %v", err)
	}

	// Re-register file manager's lease expiry callback to ensure it's properly set up
	if s.fileManager != nil {
		s.logger.Printf("Re-registering file manager lease expiry callbacks")
		s.fileManager.RegisterForLeaseExpiry()
	}

	// Clear the request cache to avoid stale responses after leadership change
	if s.requestCache != nil {
		s.logger.Printf("Clearing request cache after fencing period")
		s.requestCache.Clear()
	}

	// Log the lock state after clearing
	s.logCurrentLockState("After forced clear")

	s.logger.Printf("Server is now fully operational as primary")
}

// checkPeerRole checks the role of all peers to detect split-brain scenarios
func (s *LockServer) checkPeerRole() bool {
	// Skip if we're not a leader
	if !s.isLeader() {
		return false
	}

	// Create a copy of peer addresses to avoid holding the lock during RPC calls
	var peerAddresses []string
	s.peerClientsMu.RLock()
	for addr := range s.peerClients {
		peerAddresses = append(peerAddresses, addr)
	}
	// Also check the addresses from the peerAddresses slice
	for _, addr := range s.peerAddresses {
		peerAddresses = append(peerAddresses, addr)
	}
	s.peerClientsMu.RUnlock()

	// Remove duplicates
	addrSet := make(map[string]struct{})
	var uniqueAddresses []string
	for _, addr := range peerAddresses {
		if _, exists := addrSet[addr]; !exists {
			addrSet[addr] = struct{}{}
			uniqueAddresses = append(uniqueAddresses, addr)
		}
	}

	// If no peers configured, nothing to check
	if len(uniqueAddresses) == 0 {
		return false
	}

	splitBrainDetected := false

	// Check each peer's role
	for _, peerAddr := range uniqueAddresses {
		// Get or connect to the peer
		client, err := s.getOrConnectPeer(peerAddr)
		if err != nil {
			s.logger.Printf("Unable to connect to peer %s: %v", peerAddr, err)
			continue
		}

		// Create a timeout context
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		// Call ServerInfo RPC to check peer's role
		resp, err := client.ServerInfo(ctx, &pb.ServerInfoRequest{})
		cancel()

		if err != nil {
			s.logger.Printf("Unable to check peer role for %s: %v", peerAddr, err)
			continue
		}

		// Check if the peer reports itself as leader
		if resp.Role == string(LeaderState) || resp.Role == string(PrimaryRole) {
			peerEpoch := resp.CurrentEpoch
			currentEpoch := s.currentEpoch.Load()

			s.logger.Printf("SPLIT-BRAIN DETECTION: Peer server %s (ID %d) reports itself as leader with epoch %d (our epoch: %d)!",
				peerAddr, resp.ServerId, peerEpoch, currentEpoch)

			// If the peer has a higher or equal epoch, demote ourselves
			if peerEpoch >= currentEpoch {
				splitBrainDetected = true
				s.logger.Printf("SPLIT-BRAIN RESOLVED: Demoting self to follower due to peer with higher/equal epoch %d", peerEpoch)

				// Demote this server to follower
				s.setServerState(FollowerState)
				s.isPrimary = false
				if s.role == PrimaryRole {
					s.role = SecondaryRole
				}

				// Update epoch if peer's is higher
				if peerEpoch > currentEpoch {
					s.currentEpoch.Store(peerEpoch)
				}

				// Set leader address to this peer
				s.setLeaderAddress(peerAddr)

				// Start heartbeat sender to monitor the new leader
				go s.startHeartbeatSender()

				return true
			}
		}
	}

	return splitBrainDetected
}

// startSplitBrainChecker periodically checks if any peers have been promoted
func (s *LockServer) startSplitBrainChecker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	s.logger.Printf("Starting multi-peer split-brain checker")

	for range ticker.C {
		// Stop if we're no longer leader/primary
		if (!s.isPrimary && !s.isLeader()) || s.isFollower() {
			s.logger.Printf("No longer leader/primary, stopping split-brain checker")
			return
		}

		// Check all peers' roles
		if s.checkPeerRole() {
			// If split-brain was detected and resolved, stop the checker
			s.logger.Printf("Split-brain was detected and resolved, stopping checker")
			return
		}
	}
}

// getRandomElectionTimeout returns a random election timeout with jitter
// to prevent multiple servers from starting elections simultaneously
func getRandomElectionTimeout() time.Duration {
	// Base timeout between 150-300ms with jitter
	minTimeout := 150 * time.Millisecond
	jitter := time.Duration(rand.Intn(150)) * time.Millisecond
	return minTimeout + jitter
}

// resetElectionTimer resets the election timer with a random timeout
func (s *LockServer) resetElectionTimer() {
	s.leaderMu.Lock()
	defer s.leaderMu.Unlock()

	// Cancel existing timer if any
	if s.electionTimer != nil {
		s.electionTimer.Stop()
	}

	// Create new timer with random timeout
	timeout := getRandomElectionTimeout()
	s.electionTimer = time.AfterFunc(timeout, func() {
		// Only start promotion if we're still a follower
		if s.isFollower() {
			s.startPromotionAttempt()
		}
	})

	s.logger.Printf("Election timer reset with timeout %v", timeout)
}

// RPC Handlers

// UpdateSecondaryState handles replication from primary to secondary
func (s *LockServer) UpdateSecondaryState(ctx context.Context, state *pb.ReplicatedState) (*pb.ReplicationResponse, error) {
	// This should only be called on the secondary
	if s.isPrimary {
		s.logger.Printf("Warning: Primary received UpdateSecondaryState call")
		return &pb.ReplicationResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: "Cannot update state on primary server",
			CurrentEpoch: s.currentEpoch.Load(),
		}, nil
	}

	remoteEpoch := state.Epoch
	currentEpoch := s.currentEpoch.Load()

	s.logger.Printf("Received state update from leader: holder=%d, expiry=%v, remote epoch=%d, current epoch=%d",
		state.LockHolder, time.Unix(state.ExpiryTimestamp, 0), remoteEpoch, currentEpoch)

	// Check epochs
	if remoteEpoch < currentEpoch {
		// Reject updates from lower epochs
		s.logger.Printf("Rejecting state update with stale epoch %d < %d", remoteEpoch, currentEpoch)
		return &pb.ReplicationResponse{
			Status:       pb.Status_STALE_EPOCH,
			ErrorMessage: fmt.Sprintf("Stale epoch: remote=%d, local=%d", remoteEpoch, currentEpoch),
			CurrentEpoch: currentEpoch,
		}, nil
	} else if remoteEpoch > currentEpoch {
		// Update our epoch and transition to follower
		s.logger.Printf("Received higher epoch %d > %d, updating and applying state", remoteEpoch, currentEpoch)
		s.currentEpoch.Store(remoteEpoch)
		s.setServerState(FollowerState)
		s.isPrimary = false

		// Get leader address from context metadata if available
		if peer, ok := peer.FromContext(ctx); ok {
			leaderAddr := peer.Addr.String()
			s.setLeaderAddress(leaderAddr)
			s.logger.Printf("Updated leader address to %s", leaderAddr)
		}
	}
	// If epochs match, just apply the state

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

	return &pb.ReplicationResponse{
		Status:       pb.Status_OK,
		ErrorMessage: "",
		CurrentEpoch: s.currentEpoch.Load(),
	}, nil
}

// Ping handles heartbeat requests from secondary
func (s *LockServer) Ping(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// Basic logging for all Ping requests
	remoteEpoch := req.Epoch
	currentEpoch := s.currentEpoch.Load()

	s.logger.Printf("Received heartbeat from server %d with epoch %d (current epoch: %d)",
		req.ServerId, remoteEpoch, currentEpoch)

	// Check epochs
	if remoteEpoch > currentEpoch {
		// If incoming epoch is higher, we should step down and let the other server lead
		s.logger.Printf("Incoming heartbeat has higher epoch %d > %d, stepping down", remoteEpoch, currentEpoch)
		s.currentEpoch.Store(remoteEpoch)

		// If we're currently the primary, step down
		if s.isPrimary || s.isLeader() {
			s.setServerState(FollowerState)
			s.isPrimary = false
			// Get sender address from context metadata if available
			if peer, ok := peer.FromContext(ctx); ok {
				leaderAddr := peer.Addr.String()
				s.setLeaderAddress(leaderAddr)
				s.logger.Printf("Updated leader address to %s", leaderAddr)
			}
		}

		return &pb.HeartbeatResponse{
			Status:       pb.Status_OK,
			ErrorMessage: "Accepted higher epoch",
			CurrentEpoch: remoteEpoch, // Now adopt the higher epoch
		}, nil
	} else if remoteEpoch < currentEpoch {
		// If incoming epoch is lower, inform the other server about our higher epoch
		s.logger.Printf("Incoming heartbeat has stale epoch %d < %d", remoteEpoch, currentEpoch)
		return &pb.HeartbeatResponse{
			Status:       pb.Status_STALE_EPOCH,
			ErrorMessage: fmt.Sprintf("Stale epoch %d < %d", remoteEpoch, currentEpoch),
			CurrentEpoch: currentEpoch,
		}, nil
	}

	// If epochs are equal and we're a follower, reset our election timer
	if s.isFollower() {
		// Reset election timer to prevent timeout
		s.resetElectionTimer()
		s.logger.Printf("Received heartbeat with matching epoch, reset election timeout")
	}

	// Only check the role for informational purposes
	if !s.isPrimary && !s.isLeader() {
		s.logger.Printf("Warning: Non-primary/non-leader received heartbeat from server %d", req.ServerId)
	}

	// Return our current status and epoch
	return &pb.HeartbeatResponse{
		Status:       pb.Status_OK,
		ErrorMessage: "",
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
