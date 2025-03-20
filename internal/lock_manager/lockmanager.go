package lock_manager

import (
	"log"
	"os"
	"sync"
)

// LockManager handles all lock-related operations
type LockManager struct {
	mu         sync.Mutex // Protects shared state
	cond       *sync.Cond // Condition variable for lock waiting
	lockHolder int32      // ID of the client holding the lock, -1 if free
	logger     *log.Logger
}

// NewLockManager initializes a new lock manager
func NewLockManager() *LockManager {
	lm := &LockManager{
		lockHolder: -1, // No client holds the lock initially
		logger:     log.New(os.Stdout, "[LockManager] ", log.LstdFlags),
	}
	lm.cond = sync.NewCond(&lm.mu)
	return lm
}

// Acquire attempts to acquire the lock for the given client
func (lm *LockManager) Acquire(clientID int32) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	lm.logger.Printf("Client %d attempting to acquire lock", clientID)
	
	// Wait until the lock is free (lockHolder == -1)
	for lm.lockHolder != -1 {
		lm.logger.Printf("Client %d waiting for lock (currently held by %d)", clientID, lm.lockHolder)
		lm.cond.Wait() // Unlocks mu, waits, then relocks mu when woken
	}
	
	// Assign the lock to this client
	lm.lockHolder = clientID
	lm.logger.Printf("Lock acquired by client %d", clientID)
	
	return true
}

// Release attempts to release the lock for the given client
func (lm *LockManager) Release(clientID int32) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	lm.logger.Printf("Client %d attempting to release lock", clientID)
	
	// Check if this client holds the lock
	if lm.lockHolder == clientID {
		lm.lockHolder = -1 // Free the lock
		lm.logger.Printf("Lock released by client %d", clientID)
		lm.cond.Broadcast() // Wake all waiting clients
		return true
	}
	
	// Client doesn't hold the lock
	lm.logger.Printf("Lock release failed: client %d doesn't hold the lock (current holder: %d)", 
		clientID, lm.lockHolder)
	return false
}

// HasLock checks if the given client holds the lock
func (lm *LockManager) HasLock(clientID int32) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.lockHolder == clientID
}

// ReleaseLockIfHeld releases the lock if the given client holds it
func (lm *LockManager) ReleaseLockIfHeld(clientID int32) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	// If this client holds the lock, release it
	if lm.lockHolder == clientID {
		lm.lockHolder = -1
		lm.logger.Printf("Lock released due to client %d closing", clientID)
		lm.cond.Broadcast()
	}
}