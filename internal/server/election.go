package server

import (
	"context"
	"sync"
	"time"

	pb "Distributed-Lock-Manager/proto"
)

// tryStartElection initiates a leader election process
func (s *LockServer) tryStartElection() {
	// Use CompareAndSwap to ensure only one election process starts
	if !s.electionInProgress.CompareAndSwap(false, true) {
		s.logger.Printf("Election already in progress, skipping.")
		return
	}
	defer s.electionInProgress.Store(false) // Ensure flag is reset

	s.logger.Printf("Primary suspected down. Attempting to start election.")

	// --- Quorum Check ---
	liveNodes := make(map[int32]bool) // Track live node IDs
	liveNodes[s.serverID] = true      // Self is alive
	var wg sync.WaitGroup
	var quorumMu sync.Mutex

	s.mu.Lock() // Lock to access peerClients
	peerClientsSnapshot := make(map[int32]pb.LockServiceClient)
	for id, client := range s.peerClients {
		peerClientsSnapshot[id] = client
	}
	s.mu.Unlock()

	for peerID, client := range peerClientsSnapshot {
		if client == nil {
			continue
		}
		wg.Add(1)
		go func(id int32, c pb.LockServiceClient) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			// Ping to check liveness
			_, err := c.Ping(ctx, &pb.HeartbeatRequest{ServerId: s.serverID})
			cancel()
			if err == nil {
				quorumMu.Lock()
				liveNodes[id] = true
				quorumMu.Unlock()
			} else {
				s.logger.Printf("Election quorum check: Ping failed for peer %d: %v", id, err)
			}
		}(peerID, client)
	}
	wg.Wait()

	liveCount := len(liveNodes)
	requiredMajority := (s.clusterSize / 2) + 1

	if liveCount < requiredMajority {
		s.logger.Printf("Election failed: Quorum not met (%d/%d live nodes). Retrying heartbeat later.", liveCount, requiredMajority)
		// Optionally: Schedule another election attempt later or rely on next heartbeat failure
		// For simplicity, we just log and let the heartbeat trigger it again if needed.
		return
	}

	s.logger.Printf("Election quorum met (%d/%d live nodes). Proceeding with leader selection.", liveCount, requiredMajority)

	// --- Leader Election (Lowest ID among live nodes) ---
	var potentialLeaderID int32 = -1
	for id := range liveNodes {
		if potentialLeaderID == -1 || id < potentialLeaderID {
			potentialLeaderID = id
		}
	}

	if potentialLeaderID == -1 {
		s.logger.Printf("CRITICAL ERROR: Election quorum met but couldn't determine lowest ID.")
		return // Should not happen
	}

	s.logger.Printf("Election result: New potential primary is Server ID %d", potentialLeaderID)

	s.mu.Lock()
	s.knownPrimaryID = potentialLeaderID // Update knowledge of primary
	s.mu.Unlock()

	// --- Become Primary or Follow ---
	if potentialLeaderID == s.serverID {
		// This node won the election
		s.promoteToPrimary()
	} else {
		// Another node won, remain replica and start heartbeating the new primary
		s.logger.Printf("Following new primary ID %d", potentialLeaderID)
		s.isPrimary = false // Ensure role is replica
		s.role = SecondaryRole
		go s.startHeartbeatSender() // Start sending heartbeats to the new primary
	}
}

// promoteToPrimary promotes this server to primary role
func (s *LockServer) promoteToPrimary() {
	s.mu.Lock()
	s.logger.Printf("Promoting self (Server ID %d) to Primary", s.serverID)
	s.isPrimary = true
	s.role = PrimaryRole
	s.knownPrimaryID = s.serverID
	s.mu.Unlock()

	// Enter fencing period to prevent interference with potentially lingering old primary
	s.isFencing.Store(true)

	// Calculate fencing end time based on lease duration plus buffer
	leaseDuration := s.lockManager.GetLeaseDuration()
	s.fencingEndTime = time.Now().Add(leaseDuration).Add(s.fencingBuffer)

	s.logger.Printf("Entering fencing period until %v", s.fencingEndTime)

	// Start background goroutine to wait for fencing to complete
	go s.waitForFencingEnd()

	// Start replication worker for primary
	go s.startReplicationWorker()
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

	// Log the lock state after clearing
	s.logCurrentLockState("After forced clear")

	s.logger.Printf("Server is now fully operational as primary")
}
