package replication

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// LockState represents the state of a lock
type LockState struct {
	OwnerID     string
	LockID      string
	Timestamp   time.Time
	ExpiresAt   time.Time
	IsExclusive bool
}

// LockReplicationManager handles the replication of lock operations
type LockReplicationManager struct {
	mu           sync.RWMutex
	locks        map[string]*LockState
	protocol     *ReplicationProtocol
	logger       Logger
	serverID     string
	lockTimeouts map[string]*time.Timer
}

// NewLockReplicationManager creates a new lock replication manager
func NewLockReplicationManager(protocol *ReplicationProtocol, logger Logger, serverID string) *LockReplicationManager {
	return &LockReplicationManager{
		locks:        make(map[string]*LockState),
		protocol:     protocol,
		logger:       logger,
		serverID:     serverID,
		lockTimeouts: make(map[string]*time.Timer),
	}
}

// AcquireLock attempts to acquire a lock with replication
func (m *LockReplicationManager) AcquireLock(ctx context.Context, lockID string, isExclusive bool, timeout time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if lock exists and is owned by another server
	if existingLock, exists := m.locks[lockID]; exists {
		if existingLock.OwnerID != m.serverID {
			return fmt.Errorf("lock %s is owned by server %s", lockID, existingLock.OwnerID)
		}
	}

	// Create lock state
	lockState := &LockState{
		OwnerID:     m.serverID,
		LockID:      lockID,
		Timestamp:   time.Now(),
		ExpiresAt:   time.Now().Add(timeout),
		IsExclusive: isExclusive,
	}

	// Prepare replication request
	req := &ReplicationRequest{
		OperationType: OperationPrepare,
		ClientID:      m.serverID,
		LockID:        lockID,
		Timestamp:     time.Now().UnixNano(),
	}

	// Execute two-phase commit
	resp, err := m.protocol.Prepare(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to prepare lock acquisition: %v", err)
	}
	if !resp.Success {
		return fmt.Errorf("failed to prepare lock acquisition: %s", resp.ErrorMessage)
	}

	// Use OperationCommit for the commit phase
	commitReq := &ReplicationRequest{
		OperationType: OperationCommit,
		ClientID:      m.serverID,
		LockID:        lockID,
		Timestamp:     time.Now().UnixNano(),
	}

	resp, err = m.protocol.Commit(ctx, commitReq)
	if err != nil {
		return fmt.Errorf("failed to commit lock acquisition: %v", err)
	}
	if !resp.Success {
		return fmt.Errorf("failed to commit lock acquisition: %s", resp.ErrorMessage)
	}

	// Store lock state
	m.locks[lockID] = lockState

	// Set cleanup timer
	timer := time.AfterFunc(timeout, func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		// Check if lock still exists and belongs to us
		if lock, exists := m.locks[lockID]; exists && lock.OwnerID == m.serverID {
			// Remove the lock
			delete(m.locks, lockID)
			delete(m.lockTimeouts, lockID)
			m.logger.Warnf("Lock %s expired and was removed", lockID)
		}
	})
	m.lockTimeouts[lockID] = timer

	m.logger.Infof("Lock %s acquired by server %s", lockID, m.serverID)
	return nil
}

// ReleaseLock releases a lock with replication
func (m *LockReplicationManager) ReleaseLock(ctx context.Context, lockID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if lock exists and is owned by this server
	lock, exists := m.locks[lockID]
	if !exists {
		return fmt.Errorf("lock %s does not exist", lockID)
	}
	if lock.OwnerID != m.serverID {
		return fmt.Errorf("lock %s is not owned by this server", lockID)
	}

	// Cancel cleanup timer
	if timer, exists := m.lockTimeouts[lockID]; exists {
		timer.Stop()
		delete(m.lockTimeouts, lockID)
	}

	// Prepare replication request
	req := &ReplicationRequest{
		OperationType: OperationPrepare,
		ClientID:      m.serverID,
		LockID:        lockID,
		Timestamp:     time.Now().UnixNano(),
	}

	// Execute two-phase commit
	resp, err := m.protocol.Prepare(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to prepare lock release: %v", err)
	}
	if !resp.Success {
		return fmt.Errorf("failed to prepare lock release: %s", resp.ErrorMessage)
	}

	// Use OperationCommit for the commit phase
	commitReq := &ReplicationRequest{
		OperationType: OperationCommit,
		ClientID:      m.serverID,
		LockID:        lockID,
		Timestamp:     time.Now().UnixNano(),
	}

	resp, err = m.protocol.Commit(ctx, commitReq)
	if err != nil {
		return fmt.Errorf("failed to commit lock release: %v", err)
	}
	if !resp.Success {
		return fmt.Errorf("failed to commit lock release: %s", resp.ErrorMessage)
	}

	// Remove lock state
	delete(m.locks, lockID)

	m.logger.Infof("Lock %s released by server %s", lockID, m.serverID)
	return nil
}

// GetLockState returns the current state of a lock
func (m *LockReplicationManager) GetLockState(lockID string) (*LockState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	lock, exists := m.locks[lockID]
	if !exists {
		return nil, fmt.Errorf("lock %s does not exist", lockID)
	}
	return lock, nil
}

// HandleLockTimeout handles the expiration of a lock
func (m *LockReplicationManager) HandleLockTimeout(lockID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if lock, exists := m.locks[lockID]; exists {
		m.logger.Warnf("Lock %s expired for server %s", lockID, lock.OwnerID)
		delete(m.locks, lockID)
		delete(m.lockTimeouts, lockID)
	}
}

// ReplicateLockState replicates the current lock state to a peer
func (m *LockReplicationManager) ReplicateLockState(ctx context.Context, peerID string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Create replication request with all locks
	req := &ReplicationRequest{
		OperationType: OperationPrepare,
		ClientID:      m.serverID,
		LockID:        peerID,
		Timestamp:     time.Now().UnixNano(),
	}

	// Execute replication
	resp, err := m.protocol.ReplicateState(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to replicate lock state to peer %s: %v", peerID, err)
	}
	if !resp.Success {
		return fmt.Errorf("failed to replicate lock state to peer %s: %s", peerID, resp.ErrorMessage)
	}

	return nil
}
