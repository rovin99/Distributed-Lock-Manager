package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "Distributed-Lock-Manager/proto"
)

// checkQuorum pings all other peers and returns the count of live nodes (including self)
func (s *LockServer) checkQuorum() (int, error) {
	s.mu.Lock() // Lock to safely access peerClients
	peerClientsSnapshot := make(map[int32]pb.LockServiceClient)
	for id, client := range s.peerClients {
		peerClientsSnapshot[id] = client // Create a snapshot
	}
	s.mu.Unlock()

	liveCount := 1 // Start with self
	var wg sync.WaitGroup
	var mu sync.Mutex // To protect liveCount increment

	for peerID, client := range peerClientsSnapshot {
		if client == nil {
			continue // Skip disconnected peers
		}
		wg.Add(1)
		go func(id int32, c pb.LockServiceClient) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second) // Short timeout for check
			_, err := c.Ping(ctx, &pb.HeartbeatRequest{ServerId: s.serverID})
			cancel()
			if err == nil {
				mu.Lock()
				liveCount++
				mu.Unlock()
			} else {
				s.logger.Printf("Quorum check: Ping failed for peer %d: %v", id, err)
			}
		}(peerID, client)
	}
	wg.Wait()

	requiredMajority := (s.clusterSize / 2) + 1
	s.logger.Printf("Quorum check: Live nodes = %d, Required majority = %d", liveCount, requiredMajority)

	if liveCount < requiredMajority {
		return liveCount, fmt.Errorf("quorum lost: only %d nodes live, need %d", liveCount, requiredMajority)
	}
	return liveCount, nil // Quorum exists
}

// demoteToReplica changes the server from primary to replica role
func (s *LockServer) demoteToReplica() {
	s.mu.Lock()
	if !s.isPrimary { // Already replica, nothing to do
		s.mu.Unlock()
		return
	}
	s.logger.Printf("Demoting self (Server ID %d) to Replica due to lost quorum or other reason.", s.serverID)
	s.isPrimary = false
	s.role = SecondaryRole
	s.knownPrimaryID = -1 // Don't know who the primary is now
	s.mu.Unlock()

	// Stop replication worker (if applicable)
	// Signal the worker to stop if it's designed to be stoppable.
	// For simplicity, the worker loop already checks `isPrimary`.

	// Start heartbeating to find the new primary eventually
	// The heartbeat sender will fail initially until an election succeeds
	go s.startHeartbeatSender()
}
