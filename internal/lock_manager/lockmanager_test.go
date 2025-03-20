package lock_manager

import (
	"sync"
	"testing"
	"time"
)

func TestLockManagerBasic(t *testing.T) {
	lm := NewLockManager()

	// Test initial state
	if lm.HasLock(1) {
		t.Error("New client should not have lock initially")
	}

	// Test basic acquire
	if !lm.Acquire(1) {
		t.Error("Failed to acquire lock")
	}

	// Test HasLock
	if !lm.HasLock(1) {
		t.Error("Client should have lock after acquiring it")
	}
	if lm.HasLock(2) {
		t.Error("Client 2 should not have lock")
	}

	// Test release
	if !lm.Release(1) {
		t.Error("Failed to release lock")
	}
	if lm.HasLock(1) {
		t.Error("Client should not have lock after releasing it")
	}

	// Test releasing a lock not held
	if lm.Release(1) {
		t.Error("Should not be able to release a lock not held")
	}
}

func TestLockManagerConcurrent(t *testing.T) {
	lm := NewLockManager()
	const clientCount = 5
	
	// Track which client has the lock at any time
	var lockHolder int32 = -1
	var lockHolderMu sync.Mutex
	
	// Track how many times each client got the lock
	lockAcquisitions := make([]int, clientCount)
	
	// Use a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup
	wg.Add(clientCount)
	
	// Start time for the test
	start := time.Now()
	testDuration := 500 * time.Millisecond
	
	// Launch goroutines for each client
	for i := 0; i < clientCount; i++ {
		clientID := int32(i + 1) // Client IDs start at 1
		
		go func(id int32, clientIndex int) {
			defer wg.Done()
			
			for time.Since(start) < testDuration {
				// Try to acquire the lock
				lm.Acquire(id)
				
				// Check and update lock holder
				lockHolderMu.Lock()
				if lockHolder != -1 {
					t.Errorf("Lock conflict detected! Client %d got the lock while client %d was holding it", 
						id, lockHolder)
				}
				lockHolder = id
				lockAcquisitions[clientIndex]++
				lockHolderMu.Unlock()
				
				// Simulate doing work with the lock
				time.Sleep(10 * time.Millisecond)
				
				// Release the lock
				lockHolderMu.Lock()
				lockHolder = -1
				lockHolderMu.Unlock()
				lm.Release(id)
				
				// Small delay before trying again
				time.Sleep(5 * time.Millisecond)
			}
		}(clientID, i)
	}
	
	// Wait for all goroutines to finish
	wg.Wait()
	
	// Verify all clients got a chance to acquire the lock
	for i, count := range lockAcquisitions {
		if count == 0 {
			t.Errorf("Client %d never acquired the lock", i+1)
		}
	}
}

func TestReleaseLockIfHeld(t *testing.T) {
	lm := NewLockManager()
	
	// Acquire lock for client 1
	lm.Acquire(1)
	
	// Verify client 1 has the lock
	if !lm.HasLock(1) {
		t.Error("Client 1 should have the lock")
	}
	
	// Release lock if held for client 1
	lm.ReleaseLockIfHeld(1)
	
	// Verify lock was released
	if lm.HasLock(1) {
		t.Error("Lock should have been released")
	}
	
	// Acquire lock for client 2
	lm.Acquire(2)
	
	// Try to release lock for client 1 (which doesn't hold it)
	lm.ReleaseLockIfHeld(1)
	
	// Verify client 2 still has the lock
	if !lm.HasLock(2) {
		t.Error("Client 2 should still have the lock")
	}
}

func TestLockContention(t *testing.T) {
	lm := NewLockManager()
	
	// Have client 1 acquire the lock
	lm.Acquire(1)
	
	// Set up a channel to track when client 2 gets the lock
	lockAcquired := make(chan bool)
	
	// Start a goroutine for client 2 to try to acquire the lock
	go func() {
		lm.Acquire(2)
		lockAcquired <- true
	}()
	
	// Verify client 2 is blocked waiting for the lock
	select {
	case <-lockAcquired:
		t.Error("Client 2 should be blocked waiting for the lock")
	case <-time.After(100 * time.Millisecond):
		// This is expected - client 2 is blocked
	}
	
	// Now have client 1 release the lock
	lm.Release(1)
	
	// Verify client 2 now gets the lock
	select {
	case <-lockAcquired:
		// This is expected - client 2 got the lock
	case <-time.After(100 * time.Millisecond):
		t.Error("Client 2 should have acquired the lock after client 1 released it")
	}
	
	// Clean up
	lm.Release(2)
} 