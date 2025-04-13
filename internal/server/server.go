package server

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"Distributed-Lock-Manager/internal/file_manager"
	"Distributed-Lock-Manager/internal/lock_manager"
	pb "Distributed-Lock-Manager/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
)

// ServerRole defines the role of the server (legacy)
type ServerRole string

const (
	// PrimaryRole indicates a primary server that accepts client requests
	PrimaryRole ServerRole = "primary"
	// SecondaryRole indicates a secondary server that replicates from primary
	SecondaryRole ServerRole = "secondary"
)

// ServerState defines the state of the server in the cluster
type ServerState string

const (
	// LeaderState indicates a leader server that accepts client requests
	LeaderState ServerState = "leader"
	// FollowerState indicates a follower server that replicates from leader
	FollowerState ServerState = "follower"
	// CandidateState indicates a server trying to become the leader
	CandidateState ServerState = "candidate"
)

// HeartbeatConfig stores configuration for heartbeat mechanism
type HeartbeatConfig struct {
	Interval        time.Duration // Interval between heartbeats
	Timeout         time.Duration // Timeout for each heartbeat
	MaxFailureCount int           // Number of consecutive failures before failover
}

// Default heartbeat configuration
var DefaultHeartbeatConfig = HeartbeatConfig{
	Interval:        2 * time.Second,
	Timeout:         5 * time.Second,
	MaxFailureCount: 3,
}

// LockServer implements the LockServiceServer interface
type LockServer struct {
	pb.UnimplementedLockServiceServer
	lockManager  *lock_manager.LockManager
	fileManager  *file_manager.FileManager
	requestCache *RequestCache
	logger       *log.Logger
	recoveryDone bool                // Indicates if WAL recovery is complete
	metrics      *PerformanceMetrics // Added metrics for monitoring

	// Replication-related fields
	role        ServerRole           // Legacy: Current role (primary or secondary)
	isPrimary   bool                 // Legacy: Is this server the primary?
	serverID    int32                // ID of this server
	peerAddress string               // Legacy: Address of the peer server
	peerClient  pb.LockServiceClient // Legacy: gRPC client for peer communication
	peerConn    *grpc.ClientConn     // Legacy: gRPC connection to peer

	// Enhanced replication fields
	serverState   atomic.Value                    // Current state (Leader/Follower/Candidate)
	currentEpoch  atomic.Int64                    // Current epoch number
	votedInEpoch  atomic.Int64                    // Last epoch this server voted in
	leaderAddress string                          // Address of the current leader
	leaderMu      sync.RWMutex                    // Protects leaderAddress
	peerAddresses []string                        // Addresses of all peer servers
	peerClients   map[string]pb.LockServiceClient // gRPC clients for peer communication
	peerClientsMu sync.RWMutex                    // Protects peerClients map
	electionTimer *time.Timer                     // Timer for leader election

	// Replication queue for reliable state updates
	replicationQueue      []*pb.ReplicatedState // Queue of updates to be sent to secondary
	replicationQueueMu    sync.Mutex            // Protects the replication queue
	replicationInProgress atomic.Bool           // Flag to prevent multiple concurrent replication workers

	// Heartbeat configuration
	heartbeatConfig HeartbeatConfig // Configuration for heartbeat mechanism

	// Fencing-related fields
	isFencing      atomic.Bool   // Is the server in fencing period?
	fencingEndTime time.Time     // When does the fencing period end?
	fencingBuffer  time.Duration // Additional buffer time after lease duration
}

// NewLockServer initializes a new lock server
func NewLockServer() *LockServer {
	logger := log.New(os.Stdout, "[LockServer] ", log.LstdFlags)

	// Initialize lock manager with lease duration
	lm := lock_manager.NewLockManagerWithLeaseDuration(logger, 30*time.Second)

	// Initialize file manager with lock manager for token validation
	fm := file_manager.NewFileManagerWithWAL(true, true, lm)

	// Register file manager for lease expiry callbacks
	fm.RegisterForLeaseExpiry()

	s := &LockServer{
		lockManager:  lm,
		fileManager:  fm,
		requestCache: NewRequestCacheWithSize(10*time.Minute, 10000),
		logger:       logger,
		recoveryDone: fm.IsRecoveryComplete(),
		role:         PrimaryRole, // Default to primary role
		isPrimary:    true,        // Default to primary
		serverID:     1,           // Default ID
		metrics:      NewPerformanceMetrics(logger),
		peerClients:  make(map[string]pb.LockServiceClient),
	}

	// Initialize atomic values
	s.serverState.Store(LeaderState)
	s.currentEpoch.Store(0)
	s.votedInEpoch.Store(-1)

	return s
}

// NewReplicatedLockServer initializes a lock server with replication configuration
func NewReplicatedLockServer(role ServerRole, serverID int32, peerAddress string) *LockServer {
	return NewReplicatedLockServerWithConfig(role, serverID, peerAddress, DefaultHeartbeatConfig)
}

// NewReplicatedLockServerWithConfig initializes a lock server with replication and custom heartbeat config
func NewReplicatedLockServerWithConfig(role ServerRole, serverID int32, peerAddress string, hbConfig HeartbeatConfig) *LockServer {
	logger := log.New(os.Stdout, fmt.Sprintf("[LockServer-%d] ", serverID), log.LstdFlags)

	// Initialize lock manager with lease duration
	lm := lock_manager.NewLockManagerWithLeaseDuration(logger, 30*time.Second)

	// Initialize file manager with lock manager for token validation
	fm := file_manager.NewFileManagerWithWAL(true, true, lm)

	// Register file manager for lease expiry callbacks
	fm.RegisterForLeaseExpiry()

	isPrimary := role == PrimaryRole

	// Set initial server state based on role
	var initialState ServerState
	if isPrimary {
		initialState = LeaderState
	} else {
		initialState = FollowerState
	}

	s := &LockServer{
		lockManager:      lm,
		fileManager:      fm,
		requestCache:     NewRequestCacheWithSize(10*time.Minute, 10000),
		logger:           logger,
		recoveryDone:     fm.IsRecoveryComplete(),
		role:             role,
		isPrimary:        isPrimary,
		serverID:         serverID,
		peerAddress:      peerAddress,
		heartbeatConfig:  hbConfig,
		fencingBuffer:    5 * time.Second, // Default 5s buffer for fencing
		replicationQueue: make([]*pb.ReplicatedState, 0),
		metrics:          NewPerformanceMetrics(logger),
		peerClients:      make(map[string]pb.LockServiceClient),
	}

	// Initialize atomic values
	s.serverState.Store(initialState)
	s.currentEpoch.Store(0)
	s.votedInEpoch.Store(-1)

	// If peerAddress is provided, add it to peerAddresses
	if peerAddress != "" {
		s.peerAddresses = []string{peerAddress}

		// Legacy: Connect to peer if address is provided
		if err := s.connectToPeer(); err != nil {
			s.logger.Printf("Warning: Failed to connect to peer at %s: %v", peerAddress, err)
		}
	}

	// Start heartbeat sender if this is a secondary/follower
	if !isPrimary && peerAddress != "" {
		go s.startHeartbeatSender()
	}

	// Start background replication worker for reliable updates
	if isPrimary && peerAddress != "" {
		go s.startReplicationWorker()

		// Start periodic split-brain check for primaries
		go s.startSplitBrainChecker()
	}

	return s
}

// NewReplicatedLockServerWithMultiPeers initializes a lock server with multiple peer configuration
func NewReplicatedLockServerWithMultiPeers(role ServerRole, serverID int32, peerAddresses []string, hbConfig HeartbeatConfig) *LockServer {
	logger := log.New(os.Stdout, fmt.Sprintf("[LockServer-%d] ", serverID), log.LstdFlags)

	// Initialize lock manager with lease duration
	lm := lock_manager.NewLockManagerWithLeaseDuration(logger, 30*time.Second)

	// Initialize file manager with lock manager for token validation
	fm := file_manager.NewFileManagerWithWAL(true, true, lm)

	// Register file manager for lease expiry callbacks
	fm.RegisterForLeaseExpiry()

	isPrimary := role == PrimaryRole

	// Set initial server state based on role
	var initialState ServerState
	if isPrimary {
		initialState = LeaderState
	} else {
		initialState = FollowerState
	}

	s := &LockServer{
		lockManager:      lm,
		fileManager:      fm,
		requestCache:     NewRequestCacheWithSize(10*time.Minute, 10000),
		logger:           logger,
		recoveryDone:     fm.IsRecoveryComplete(),
		role:             role,
		isPrimary:        isPrimary,
		serverID:         serverID,
		peerAddresses:    peerAddresses,
		heartbeatConfig:  hbConfig,
		fencingBuffer:    5 * time.Second, // Default 5s buffer for fencing
		replicationQueue: make([]*pb.ReplicatedState, 0),
		metrics:          NewPerformanceMetrics(logger),
		peerClients:      make(map[string]pb.LockServiceClient),
	}

	// Initialize atomic values
	s.serverState.Store(initialState)
	s.currentEpoch.Store(0)
	s.votedInEpoch.Store(-1)

	// Note: We don't immediately connect to peers; we'll do so dynamically as needed via getOrConnectPeer
	s.logger.Printf("Initialized server with %d peer addresses", len(peerAddresses))

	// For backward compatibility, if this is a secondary, start a heartbeat sender to the first peer
	// in the list (which is assumed to be the current leader)
	if !isPrimary && len(peerAddresses) > 0 {
		// Set the leader address to the first peer
		s.setLeaderAddress(peerAddresses[0])
		go s.startHeartbeatSender()
	}

	// For backward compatibility, if this is a primary, start the replication worker
	if isPrimary && len(peerAddresses) > 0 {
		go s.startReplicationWorker()
	}

	return s
}

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
	s.serverState.Store(state)
	s.logger.Printf("Server state changed from %s to %s", oldState, state)
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

// Legacy: keep the original connectToPeer for backward compatibility
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
	// votes := 1 // Start with self-vote (removed unused variable)

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

	// Start a goroutine to end the fencing period
	go s.waitForFencingEnd()

	// Start replicating to followers
	go s.startReplicationWorker()

	// Start periodic heartbeats to followers to maintain leadership
	go s.startLeaderHeartbeats()
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

	// Log the lock state after clearing
	s.logCurrentLockState("After forced clear")

	s.logger.Printf("Server is now fully operational as primary")
}

// logCurrentLockState logs the current lock state for debugging purposes
func (s *LockServer) logCurrentLockState(context string) {
	holder := s.lockManager.CurrentHolder()
	expiry := s.lockManager.GetTokenExpiration()

	var expiryStr string
	if expiry.IsZero() {
		expiryStr = "not set"
	} else {
		expiryStr = expiry.Format(time.RFC3339)
	}

	s.logger.Printf("Lock state (%s): holder=%d, expiry=%s",
		context, holder, expiryStr)
}

// SetFencingBuffer sets the buffer time to add to lease duration for fencing
func (s *LockServer) SetFencingBuffer(buffer time.Duration) {
	s.fencingBuffer = buffer
	s.logger.Printf("Fencing buffer set to %v", buffer)
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

// IsRecoveryComplete returns whether WAL recovery is complete
func (s *LockServer) IsRecoveryComplete() bool {
	return s.recoveryDone && s.fileManager.IsRecoveryComplete()
}

// GetRecoveryError returns any error that occurred during recovery
func (s *LockServer) GetRecoveryError() error {
	return s.fileManager.GetRecoveryError()
}

// WaitForRecovery blocks until WAL recovery is complete or timeout is reached
func (s *LockServer) WaitForRecovery(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		if s.IsRecoveryComplete() {
			if err := s.GetRecoveryError(); err != nil {
				return fmt.Errorf("recovery completed with error: %v", err)
			}
			return nil
		}
		<-ticker.C
	}

	return fmt.Errorf("timeout waiting for WAL recovery")
}

// ClientInit handles the client initialization RPC
func (s *LockServer) ClientInit(ctx context.Context, args *pb.ClientInitArgs) (*pb.ClientInitResponse, error) {
	// Allow client initialization even if not primary
	clientID := args.ClientId
	s.logger.Printf("Client %d initialized or reconnected", clientID)

	return &pb.ClientInitResponse{
		Status:       pb.Status_OK,
		ErrorMessage: "",
	}, nil
}

// checkPeerRole verifies if the peer server has been promoted
func (s *LockServer) checkPeerRole() bool {
	// If this server doesn't have a peer client configured, nothing to check
	if s.peerClient == nil {
		return false
	}

	// Create a timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Call ServerInfo RPC to check peer's role
	resp, err := s.peerClient.ServerInfo(ctx, &pb.ServerInfoRequest{})
	if err != nil {
		s.logger.Printf("Unable to check peer role: %v", err)
		return false // Assume peer is still a follower if we can't contact it
	}

	// Check if the peer reports itself as leader
	if resp.Role == string(LeaderState) || resp.Role == string(PrimaryRole) {
		s.logger.Printf("SPLIT-BRAIN DETECTION: Peer server (ID %d) reports itself as leader!", resp.ServerId)

		// If we think we're leader but the peer also thinks it's leader,
		// we have a split-brain scenario
		if s.isLeader() {
			s.logger.Printf("SPLIT-BRAIN RESOLVED: Demoting self to follower to avoid split-brain")

			// Demote this server to follower
			s.setServerState(FollowerState)
			s.isPrimary = false
			s.role = SecondaryRole

			// Start heartbeat sender to monitor the new leader
			go s.startHeartbeatSender()

			return true
		}
	}

	return false
}

// LockAcquire handles lock acquisition requests
func (s *LockServer) LockAcquire(ctx context.Context, args *pb.LockArgs) (*pb.LockResponse, error) {
	// Check for split-brain condition if this server thinks it's leader
	if s.isLeader() && s.checkPeerRole() {
		// We've been demoted to follower, reject the request
		return &pb.LockResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "This server was demoted to follower to avoid split-brain",
		}, nil
	}

	// Check if this server is in follower mode
	if !s.isLeader() && !s.isFencing.Load() {
		return &pb.LockResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "This server is in follower mode and cannot grant locks",
		}, nil
	}

	// Add detailed logging for fencing check
	isFencingNow := s.isFencing.Load()
	nowTime := time.Now()
	fencingEnds := s.fencingEndTime
	isBeforeEnd := nowTime.Before(fencingEnds)

	s.logger.Printf("DEBUG FENCING CHECK: ClientID=%d RequestID=%s isFencing=%t, Now=%v, End=%v, IsBeforeEnd=%t",
		args.ClientId, args.RequestId, isFencingNow, nowTime, fencingEnds, isBeforeEnd)

	// Check if the server is in fencing period
	if isFencingNow {
		s.logger.Printf("FENCING: Rejecting lock acquisition from client %d during fencing period", args.ClientId)
		return &pb.LockResponse{
			Status:       pb.Status_SERVER_FENCING,
			ErrorMessage: "Server is in fencing period and cannot grant locks",
		}, nil
	}

	s.logger.Printf("DEBUG FENCING CHECK: Proceeding with acquire for client %d", args.ClientId)

	// Check if request has been processed already
	if cachedResp, exists := s.requestCache.Get(args.RequestId); exists {
		s.logger.Printf("Detected repeated request %s from client %d", args.RequestId, args.ClientId)
		if cachedResp != nil {
			return cachedResp.(*pb.LockResponse), nil
		}
	}

	// Check request validity
	if args.ClientId <= 0 {
		errMsg := "Invalid client ID"
		s.logger.Printf("Lock acquisition failed: %s", errMsg)
		s.metrics.TrackOperationFailure(OpLockAcquire)
		return &pb.LockResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: errMsg,
		}, nil
	}

	// Before acquiring lock, track the queue length for monitoring
	queueLength := s.lockManager.GetQueueLength()
	s.metrics.RecordQueueLength(queueLength)

	// Log current state for debugging
	s.logCurrentLockState("Before lock acquire")

	// Attempt to acquire the lock
	waitStart := time.Now()
	success, token := s.lockManager.Acquire(args.ClientId)
	waitTime := time.Since(waitStart)
	if waitTime > 10*time.Millisecond {
		s.metrics.RecordQueueWaitTime(waitTime)
	}

	if success {
		// Record successful acquisition in metrics
		s.metrics.RecordLockAcquisition(args.ClientId)

		s.logger.Printf("Lock acquired by client %d with token %s", args.ClientId, token)
		// Create response
		resp := &pb.LockResponse{
			Status: pb.Status_OK,
			Token:  token,
		}

		// Cache the response for idempotence
		s.requestCache.Set(args.RequestId, resp)

		// Replicate the state to the secondary
		if s.peerClient != nil {
			s.replicateStateToFollowers()
		}

		// Log current state for debugging
		s.logCurrentLockState("After lock acquire")

		return resp, nil
	}

	// Lock acquisition failed
	s.metrics.TrackOperationFailure(OpLockAcquire)
	s.metrics.RecordLockContention()
	s.logger.Printf("Failed to acquire lock for client %d", args.ClientId)

	return &pb.LockResponse{
		Status:       pb.Status_ERROR,
		ErrorMessage: "Lock is currently held by another client",
	}, nil
}

// LockRelease handles the lock release RPC
func (s *LockServer) LockRelease(ctx context.Context, args *pb.LockArgs) (*pb.LockResponse, error) {
	// Check for split-brain condition if this server thinks it's leader
	if s.isLeader() && s.checkPeerRole() {
		// We've been demoted to follower, reject the request
		return &pb.LockResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "This server was demoted to follower to avoid split-brain",
			Token:        "",
		}, nil
	}

	// Check if we're in follower mode
	if !s.isLeader() {
		return &pb.LockResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "Server is in follower mode and cannot process client requests",
			Token:        "",
		}, nil
	}

	// For LockRelease, we allow operations during fencing period if token is valid
	// This helps clients clean up gracefully

	clientID := args.ClientId
	token := args.Token
	requestID := args.RequestId

	s.logger.Printf("Client %d attempting to release lock with token %s (request: %s)", clientID, token, requestID)

	// Check cache for duplicate request
	if cachedResp, exists := s.requestCache.Get(requestID); exists {
		s.logger.Printf("Found cached response for request %s", requestID)
		return cachedResp.(*pb.LockResponse), nil
	}

	// Mark request as in progress
	if !s.requestCache.MarkInProgress(requestID) {
		s.logger.Printf("Request %s already in progress", requestID)
		return &pb.LockResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: "Request already in progress",
		}, nil
	}

	// Validate token and check lock ownership
	if !s.lockManager.HasLockWithToken(clientID, token) {
		// This could be a duplicate from a previous successful release that was already processed
		// Treat this as an idempotent success since the goal (client doesn't have the lock) is achieved
		s.logger.Printf("Client %d trying to release lock with token %s they don't hold - treating as idempotent success", clientID, token)
		resp := &pb.LockResponse{
			Status: pb.Status_OK,
			Token:  "",
		}
		s.requestCache.Set(requestID, resp)
		return resp, nil
	}

	success := s.lockManager.Release(clientID, token)

	var resp *pb.LockResponse
	if success {
		s.logger.Printf("Lock released successfully by client %d", clientID)
		resp = &pb.LockResponse{
			Status:       pb.Status_OK,
			ErrorMessage: "",
			Token:        "",
		}

		// Replicate state to secondary
		s.replicateStateToFollowers()
	} else {
		s.logger.Printf("Lock release failed for client %d: permission denied", clientID)
		resp = &pb.LockResponse{
			Status:       pb.Status_PERMISSION_DENIED,
			ErrorMessage: "Permission denied",
			Token:        "",
		}
	}

	s.requestCache.Set(requestID, resp)
	return resp, nil
}

// FileAppend handles file append requests
func (s *LockServer) FileAppend(ctx context.Context, args *pb.FileArgs) (*pb.FileResponse, error) {
	// Check for split-brain condition if this server thinks it's leader
	if s.isLeader() && s.checkPeerRole() {
		// We've been demoted to follower, reject the request
		return &pb.FileResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "This server was demoted to follower to avoid split-brain",
		}, nil
	}

	// Check if we're in follower mode
	if !s.isLeader() {
		return &pb.FileResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "Server is in follower mode and cannot process client requests",
		}, nil
	}

	// Check if we're in fencing period first, before any other checks
	isFencingNow := s.isFencing.Load()
	nowTime := time.Now()
	fencingEnds := s.fencingEndTime
	isBeforeEnd := nowTime.Before(fencingEnds)

	s.logger.Printf("DEBUG FileAppend: ClientID=%d RequestID=%s isFencing=%t Now=%v End=%v IsBeforeEnd=%t",
		args.ClientId, args.RequestId, isFencingNow, nowTime, fencingEnds, isBeforeEnd)

	// Reject if in fencing period
	if isFencingNow && isBeforeEnd {
		s.logger.Printf("FENCING: Rejecting file append from client %d during fencing period", args.ClientId)
		return &pb.FileResponse{
			Status:       pb.Status_SERVER_FENCING,
			ErrorMessage: "Server is in fencing period and cannot process file operations",
		}, nil
	}

	// Check cache for duplicate request
	if cachedResp, exists := s.requestCache.Get(args.RequestId); exists {
		s.logger.Printf("Found cached response for request %s", args.RequestId)
		return cachedResp.(*pb.FileResponse), nil
	}

	// Mark request as in progress
	if !s.requestCache.MarkInProgress(args.RequestId) {
		s.logger.Printf("Request %s already in progress", args.RequestId)
		return &pb.FileResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: "Request already in progress",
		}, nil
	}

	// Validate token and check lock ownership
	if !s.lockManager.HasLockWithToken(args.ClientId, args.Token) {
		s.logger.Printf("FileAppend rejected: client %d doesn't hold lock with token %s", args.ClientId, args.Token)
		resp := &pb.FileResponse{
			Status:       pb.Status_INVALID_TOKEN,
			ErrorMessage: "Invalid token or lock not held",
		}
		s.requestCache.Set(args.RequestId, resp)
		return resp, nil
	}

	// Process the append operation
	err := s.fileManager.AppendToFile(args.Filename, args.Content, args.ClientId, args.Token)
	if err != nil {
		s.logger.Printf("FileAppend error: %v", err)
		resp := &pb.FileResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: err.Error(),
		}
		s.requestCache.Set(args.RequestId, resp)
		return resp, nil
	}

	s.logger.Printf("FileAppend successful for client %d", args.ClientId)
	resp := &pb.FileResponse{
		Status: pb.Status_OK,
	}
	s.requestCache.Set(args.RequestId, resp)
	return resp, nil
}

// RenewLease handles lease renewal RPC
func (s *LockServer) RenewLease(ctx context.Context, args *pb.LeaseArgs) (*pb.LeaseResponse, error) {
	// Check for split-brain condition if this server thinks it's leader
	if s.isLeader() && s.checkPeerRole() {
		// We've been demoted to follower, reject the request
		return &pb.LeaseResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "This server was demoted to follower to avoid split-brain",
		}, nil
	}

	// Check if we're in follower mode
	if !s.isLeader() {
		return &pb.LeaseResponse{
			Status:       pb.Status_SECONDARY_MODE,
			ErrorMessage: "Server is in follower mode and cannot process client requests",
		}, nil
	}

	// For RenewLease, we allow operations during fencing period if token is valid
	// This helps clients with valid leases maintain their leases during failover

	clientID := args.ClientId
	token := args.Token
	requestID := args.RequestId

	s.logger.Printf("Client %d attempting to renew lease with token %s (request: %s)", clientID, token, requestID)

	// Check cache for duplicate request
	if cachedResp, exists := s.requestCache.Get(requestID); exists {
		s.logger.Printf("Found cached response for request %s", requestID)
		return cachedResp.(*pb.LeaseResponse), nil
	}

	// Mark request as in progress
	if !s.requestCache.MarkInProgress(requestID) {
		s.logger.Printf("Request %s already in progress", requestID)
		return &pb.LeaseResponse{
			Status:       pb.Status_ERROR,
			ErrorMessage: "Request already in progress",
		}, nil
	}

	// Validate token and renew lease
	success := s.lockManager.RenewLease(clientID, token)

	var resp *pb.LeaseResponse
	if success {
		s.logger.Printf("Lease renewed for client %d", clientID)
		resp = &pb.LeaseResponse{
			Status:       pb.Status_OK,
			ErrorMessage: "",
		}

		// Replicate state to secondary after lease renewal
		s.replicateStateToFollowers()
	} else {
		s.logger.Printf("Lease renewal failed for client %d: invalid token or not lock holder", clientID)
		resp = &pb.LeaseResponse{
			Status:       pb.Status_INVALID_TOKEN,
			ErrorMessage: "Invalid token or not lock holder",
		}
	}

	s.requestCache.Set(requestID, resp)
	return resp, nil
}

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

// ServerInfo provides information about this server
func (s *LockServer) ServerInfo(ctx context.Context, req *pb.ServerInfoRequest) (*pb.ServerInfoResponse, error) {
	// Include information about server state, role, and current epoch
	var role string
	if s.isLeader() {
		role = string(LeaderState) // Use the new state terms
	} else if s.isFollower() {
		role = string(FollowerState)
	} else if s.isCandidate() {
		role = string(CandidateState)
	} else {
		// Legacy role mapping
		if s.isPrimary {
			role = string(PrimaryRole)
		} else {
			role = string(SecondaryRole)
		}
	}

	return &pb.ServerInfoResponse{
		ServerId:     s.serverID,
		Role:         role,
		CurrentEpoch: s.currentEpoch.Load(),
	}, nil
}

// ClientClose handles the client close RPC
func (s *LockServer) ClientClose(ctx context.Context, args *pb.ClientInitArgs) (*pb.ClientInitResponse, error) {
	clientID := args.ClientId
	s.logger.Printf("Client %d closing connection", clientID)

	// If this client holds the lock, release it
	s.lockManager.ReleaseLockIfHeld(clientID)

	// Return success response
	return &pb.ClientInitResponse{
		Status:       pb.Status_OK,
		ErrorMessage: "Disconnected!",
	}, nil
}

// CreateFiles ensures the 100 files exist - now delegates to file manager
func CreateFiles() {
	// Create a temporary lock manager for file creation
	lm := lock_manager.NewLockManagerWithLeaseDuration(log.New(os.Stdout, "[FileCreation] ", log.LstdFlags), 30*time.Second)
	fm := file_manager.NewFileManager(false, lm)
	fm.CreateFiles()
}

// Cleanup closes any open files and performs other cleanup tasks
func (s *LockServer) Cleanup() {
	s.fileManager.Cleanup()
	s.logger.Println("Server cleanup complete")
}

// VerifyUniqueServerID checks for duplicate server IDs with retry
func (s *LockServer) VerifyUniqueServerID() error {
	if s.peerAddress == "" {
		// No peer to check against
		return nil
	}

	// Retry a few times since the peer might still be starting up
	maxRetries := 5
	retryDelay := 1 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Ensure we have a connection to the peer
		if s.peerClient == nil {
			if err := s.connectToPeer(); err != nil {
				lastErr = fmt.Errorf("failed to connect to peer for ID verification: %w", err)
				s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
				time.Sleep(retryDelay)
				continue
			}
		}

		// Use ServerInfo RPC to check peer's ID
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := s.peerClient.ServerInfo(ctx, &pb.ServerInfoRequest{})
		cancel()

		if err != nil {
			lastErr = fmt.Errorf("unable to verify peer server ID: %w", err)
			s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
			time.Sleep(retryDelay)
			continue
		}

		// Successfully got peer info - check for duplicate ID
		if resp.ServerId == s.serverID {
			errMsg := fmt.Sprintf("Duplicate server ID detected! This server and peer both have ID %d", s.serverID)
			s.logger.Printf("ERROR: %s", errMsg)
			return fmt.Errorf(errMsg)
		}

		// Success!
		s.logger.Printf("Verified unique server ID: local=%d, peer=%d", s.serverID, resp.ServerId)
		return nil
	}

	// If we reach here, we failed after all retries
	s.logger.Printf("WARNING: Could not verify server ID uniqueness after %d attempts: %v", maxRetries, lastErr)
	s.logger.Printf("Continuing startup, but be aware that duplicate IDs may cause issues")
	return nil // Allow startup to continue despite verification failure
}

// VerifySharedFilesystem checks if the shared filesystem is properly accessible with retry
func (s *LockServer) VerifySharedFilesystem() error {
	if s.peerAddress == "" {
		// No peer to verify with
		return nil
	}

	// Retry a few times since the peer might still be starting up
	maxRetries := 5
	retryDelay := 1 * time.Second

	// Try to validate direct filesystem access first
	lockStatePath := "./data/lock_state.json"
	testContent := []byte(fmt.Sprintf("Filesystem test from server %d at %s\n",
		s.serverID, time.Now().Format(time.RFC3339)))

	// Test if we can write to the path
	if err := os.WriteFile(lockStatePath+".test", testContent, 0644); err != nil {
		return fmt.Errorf("cannot write to data directory: %w", err)
	}
	defer os.Remove(lockStatePath + ".test")

	// Verify through peer with retries
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Ensure we have a connection to the peer
		if s.peerClient == nil {
			if err := s.connectToPeer(); err != nil {
				lastErr = fmt.Errorf("failed to connect to peer for filesystem verification: %w", err)
				s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
				time.Sleep(retryDelay)
				continue
			}
		}

		// Create a test file with random content
		testFileName := fmt.Sprintf("./data/fs_verify_%d.tmp", time.Now().UnixNano())
		testContent := fmt.Sprintf("Filesystem verification from server %d at %s",
			s.serverID, time.Now().Format(time.RFC3339))

		// Write test content to file
		if err := os.WriteFile(testFileName, []byte(testContent), 0644); err != nil {
			return fmt.Errorf("failed to write test file for filesystem verification: %w", err)
		}
		defer os.Remove(testFileName) // Clean up the test file when done

		// Ask peer to read the file
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := s.peerClient.VerifyFileAccess(ctx, &pb.FileAccessRequest{
			FilePath:        testFileName,
			ExpectedContent: testContent,
		})
		cancel()

		if err != nil {
			lastErr = fmt.Errorf("filesystem verification failed - error calling peer: %w", err)
			s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
			time.Sleep(retryDelay)
			continue
		}

		if resp.Status != pb.Status_OK {
			lastErr = fmt.Errorf("filesystem verification failed: %s", resp.ErrorMessage)
			s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
			time.Sleep(retryDelay)
			continue
		}

		if resp.ActualContent != testContent {
			lastErr = fmt.Errorf("filesystem verification failed - content mismatch: expected '%s', got '%s'",
				testContent, resp.ActualContent)
			s.logger.Printf("Attempt %d: %v, retrying in %v", attempt+1, lastErr, retryDelay)
			time.Sleep(retryDelay)
			continue
		}

		// Success!
		s.logger.Printf("Shared filesystem verification successful")
		return nil
	}

	// If we reach here, we failed after all retries
	s.logger.Printf("WARNING: Could not verify shared filesystem after %d attempts: %v", maxRetries, lastErr)
	s.logger.Printf("Continuing startup, but be aware that filesystem sharing may not be working correctly")
	return nil // Allow startup to continue despite verification failure
}

// VerifyFileAccess implements the RPC to check file access from a peer
func (s *LockServer) VerifyFileAccess(ctx context.Context, req *pb.FileAccessRequest) (*pb.FileAccessResponse, error) {
	s.logger.Printf("Received filesystem verification request for file: %s", req.FilePath)

	// Attempt to read the specified file
	content, err := os.ReadFile(req.FilePath)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to read file %s: %v", req.FilePath, err)
		s.logger.Printf("Filesystem verification failed: %s", errMsg)
		return &pb.FileAccessResponse{
			Status:       pb.Status_FILESYSTEM_ERROR,
			ErrorMessage: errMsg,
		}, nil
	}

	actualContent := string(content)

	// Check if content matches what's expected
	if actualContent != req.ExpectedContent {
		errMsg := fmt.Sprintf("Content mismatch in file %s", req.FilePath)
		s.logger.Printf("Filesystem verification failed: %s", errMsg)
		return &pb.FileAccessResponse{
			Status:        pb.Status_FILESYSTEM_ERROR,
			ErrorMessage:  errMsg,
			ActualContent: actualContent,
		}, nil
	}

	s.logger.Printf("Filesystem verification successful for file: %s", req.FilePath)
	return &pb.FileAccessResponse{
		Status:        pb.Status_OK,
		ActualContent: actualContent,
	}, nil
}

// GetMetrics returns the server's performance metrics tracker
func (s *LockServer) GetMetrics() *PerformanceMetrics {
	return s.metrics
}

// startSplitBrainChecker periodically checks if the peer has been promoted
func (s *LockServer) startSplitBrainChecker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	s.logger.Printf("Starting split-brain checker")

	for range ticker.C {
		// Stop if we're no longer primary
		if !s.isPrimary {
			s.logger.Printf("No longer primary, stopping split-brain checker")
			return
		}

		// Attempt to connect to peer if not connected
		if s.peerClient == nil {
			if err := s.connectToPeer(); err != nil {
				s.logger.Printf("Split-brain checker: Failed to connect to peer: %v", err)
				continue
			}
		}

		// Check if peer reports itself as primary
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := s.peerClient.ServerInfo(ctx, &pb.ServerInfoRequest{})
		cancel()

		if err != nil {
			s.logger.Printf("Split-brain checker: Unable to check peer role: %v", err)
			continue
		}

		// If peer reports itself as primary while we also think we're primary,
		// demote ourselves to avoid split-brain
		if resp.Role == "primary" {
			s.logger.Printf("SPLIT-BRAIN DETECTED: Both this server and peer (ID %d) think they are primary!", resp.ServerId)
			s.logger.Printf("SPLIT-BRAIN RESOLVED: Demoting self to secondary to avoid split-brain")

			// Demote this server to secondary
			s.isPrimary = false
			s.role = SecondaryRole

			// Start heartbeat sender to reconnect with the new primary
			go s.startHeartbeatSender()

			return
		}
	}
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
