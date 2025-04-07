package replication

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// OperationType represents the type of replication operation
type OperationType string

const (
	// OperationPrepare represents the prepare phase of a two-phase commit
	OperationPrepare OperationType = "prepare"
	// OperationCommit represents the commit phase of a two-phase commit
	OperationCommit OperationType = "commit"
	// OperationAbort represents the abort phase of a two-phase commit
	OperationAbort OperationType = "abort"
)

// ReplicationRequest represents a request to replicate state
type ReplicationRequest struct {
	OperationType OperationType
	ClientID      string
	LockID        string
	Token         string
	Timestamp     int64
}

// ReplicationResponse represents a response to a replication request
type ReplicationResponse struct {
	Success      bool
	ErrorMessage string
	Timestamp    int64
}

// ReplicationProtocol handles the two-phase commit protocol for state replication
type ReplicationProtocol struct {
	mu sync.RWMutex
	// peers is a map of peer ID to peer connection
	peers map[string]*Peer
	// preparedOperations tracks operations in prepare phase
	preparedOperations map[string]*ReplicationRequest
	// committedOperations tracks operations in commit phase
	committedOperations map[string]*ReplicationRequest
	// operationTimeouts tracks timeouts for operations
	operationTimeouts map[string]*time.Timer
	// logger for protocol events
	logger Logger
}

// NewReplicationProtocol creates a new replication protocol instance
func NewReplicationProtocol(logger Logger) *ReplicationProtocol {
	return &ReplicationProtocol{
		peers:               make(map[string]*Peer),
		preparedOperations:  make(map[string]*ReplicationRequest),
		committedOperations: make(map[string]*ReplicationRequest),
		operationTimeouts:   make(map[string]*time.Timer),
		logger:              logger,
	}
}

// AddPeer adds a peer to the protocol
func (p *ReplicationProtocol) AddPeer(peer *Peer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers[peer.ID] = peer
	p.logger.Infof("Added peer %s to replication protocol", peer.ID)
}

// RemovePeer removes a peer from the protocol
func (p *ReplicationProtocol) RemovePeer(peerID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.peers, peerID)
	p.logger.Infof("Removed peer %s from replication protocol", peerID)
}

// Prepare initiates the prepare phase of a two-phase commit
func (p *ReplicationProtocol) Prepare(ctx context.Context, req *ReplicationRequest) (*ReplicationResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if operation is already prepared or committed
	if _, exists := p.preparedOperations[req.LockID]; exists {
		return &ReplicationResponse{
			Success:   true,
			Timestamp: time.Now().UnixNano(),
		}, nil
	}

	if _, exists := p.committedOperations[req.LockID]; exists {
		return &ReplicationResponse{
			Success:   true,
			Timestamp: time.Now().UnixNano(),
		}, nil
	}

	// Store the prepared operation
	p.preparedOperations[req.LockID] = req

	// Set a timeout for the operation
	p.operationTimeouts[req.LockID] = time.AfterFunc(30*time.Second, func() {
		p.handleTimeout(req.LockID)
	})

	p.logger.Infof("Prepared operation for lock %s", req.LockID)
	return &ReplicationResponse{
		Success:   true,
		Timestamp: time.Now().UnixNano(),
	}, nil
}

// Commit initiates the commit phase of a two-phase commit
func (p *ReplicationProtocol) Commit(ctx context.Context, req *ReplicationRequest) (*ReplicationResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if operation is prepared
	if _, exists := p.preparedOperations[req.LockID]; !exists {
		return &ReplicationResponse{
			Success:      false,
			ErrorMessage: "Operation not prepared",
			Timestamp:    time.Now().UnixNano(),
		}, status.Error(codes.FailedPrecondition, "Operation not prepared")
	}

	// Move operation from prepared to committed
	delete(p.preparedOperations, req.LockID)
	p.committedOperations[req.LockID] = req

	// Clear the timeout
	if timer, exists := p.operationTimeouts[req.LockID]; exists {
		timer.Stop()
		delete(p.operationTimeouts, req.LockID)
	}

	p.logger.Infof("Committed operation for lock %s", req.LockID)
	return &ReplicationResponse{
		Success:   true,
		Timestamp: time.Now().UnixNano(),
	}, nil
}

// Abort aborts a prepared operation
func (p *ReplicationProtocol) Abort(ctx context.Context, req *ReplicationRequest) (*ReplicationResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if operation is prepared
	if _, exists := p.preparedOperations[req.LockID]; !exists {
		return &ReplicationResponse{
			Success:      false,
			ErrorMessage: "Operation not prepared",
			Timestamp:    time.Now().UnixNano(),
		}, status.Error(codes.FailedPrecondition, "Operation not prepared")
	}

	// Remove the prepared operation
	delete(p.preparedOperations, req.LockID)

	// Clear the timeout
	if timer, exists := p.operationTimeouts[req.LockID]; exists {
		timer.Stop()
		delete(p.operationTimeouts, req.LockID)
	}

	p.logger.Infof("Aborted operation for lock %s", req.LockID)
	return &ReplicationResponse{
		Success:   true,
		Timestamp: time.Now().UnixNano(),
	}, nil
}

// handleTimeout handles timeout for an operation
func (p *ReplicationProtocol) handleTimeout(lockID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if operation is still prepared
	if _, exists := p.preparedOperations[lockID]; exists {
		// Abort the operation
		delete(p.preparedOperations, lockID)
		delete(p.operationTimeouts, lockID)
		p.logger.Warnf("Operation for lock %s timed out and was aborted", lockID)
	}
}

// ReplicateState replicates state to all peers using two-phase commit
func (p *ReplicationProtocol) ReplicateState(ctx context.Context, req *ReplicationRequest) (*ReplicationResponse, error) {
	// Prepare phase
	prepareResp, err := p.Prepare(ctx, req)
	if err != nil || !prepareResp.Success {
		return prepareResp, err
	}

	// Commit phase
	commitResp, err := p.Commit(ctx, req)
	if err != nil || !commitResp.Success {
		// If commit fails, abort the operation
		_, abortErr := p.Abort(ctx, req)
		if abortErr != nil {
			p.logger.Errorf("Failed to abort operation after commit failure: %v", abortErr)
		}
		return commitResp, err
	}

	return commitResp, nil
}

// GetOperationStatus returns the status of an operation
func (p *ReplicationProtocol) GetOperationStatus(lockID string) (OperationType, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if _, exists := p.preparedOperations[lockID]; exists {
		return OperationPrepare, true
	}

	if _, exists := p.committedOperations[lockID]; exists {
		return OperationCommit, true
	}

	return "", false
}

// CleanupOperation cleans up an operation
func (p *ReplicationProtocol) CleanupOperation(lockID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.preparedOperations, lockID)
	delete(p.committedOperations, lockID)
	if timer, exists := p.operationTimeouts[lockID]; exists {
		timer.Stop()
		delete(p.operationTimeouts, lockID)
	}
}
