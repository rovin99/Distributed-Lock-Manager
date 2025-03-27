package internal

import (
	"context"
	"sync"
	"testing"
	"time"

	"Distributed-Lock-Manager/internal/lock_manager"
)

// TestPacketLossRetryScenario tests the scenario where a response packet is lost
// and verifies that the retry mechanism ensures liveness when integrated with the
// server implementation
func TestPacketLossRetryScenario(t *testing.T) {
	// Create a lock manager
	lm := lock_manager.NewLockManager(nil)

	// Create a channel to track events in the test
	events := make(chan string, 10)

	// Create a mutex to simulate packet loss
	var simulateLock sync.Mutex

	// Stage 1: Node 1 sends a lock acquire request, and the "response" is lost
	// We'll simulate this by directly setting the lock holder in the lock manager
	// as if the LockAcquire call succeeded server-side but client never got response
	go func() {
		events <- "Node1-Attempting"

		simulateLock.Lock()
		// Simulate server handling the request but client not getting response
		lm.ReleaseLockIfHeld(1) // Ensure it doesn't have the lock already

		// Directly set client 1 as the lock holder (simulating server grants but response lost)
		lm.ForceSetLockHolder(1)
		events <- "Server-GrantedLock-ResponseLost"
		simulateLock.Unlock()

		// Wait a bit to simulate timeout
		time.Sleep(500 * time.Millisecond)

		// Stage 2: Node 1 retries and should succeed immediately
		events <- "Node1-Retrying"
		simulateLock.Lock()
		simulateLock.Unlock()

		// This should succeed immediately since Node 1 already holds the lock
		success := lm.AcquireWithTimeout(1, context.Background())
		if !success {
			t.Error("Node 1 retry should succeed because it already holds the lock")
		}
		events <- "Node1-RetrySucceeded"

		// Hold the lock for a bit
		time.Sleep(200 * time.Millisecond)

		// Release the lock
		if !lm.Release(1) {
			t.Error("Node 1 should be able to release the lock")
		}
		events <- "Node1-ReleasedLock"
	}()

	// Wait for Node 1 to "lose" the response
	expectEvent(t, events, "Node1-Attempting")
	expectEvent(t, events, "Server-GrantedLock-ResponseLost")

	// Stage 3: Set up Node 2 to wait for the lock
	node2Acquired := make(chan bool)
	go func() {
		events <- "Node2-WaitingForLock"

		// Try to acquire the lock - should block until Node 1 releases it
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		success := lm.AcquireWithTimeout(2, ctx)

		if !success {
			t.Error("Node 2 should eventually acquire the lock")
		}

		events <- "Node2-AcquiredLock"
		node2Acquired <- true

		// Hold the lock for a bit
		time.Sleep(100 * time.Millisecond)

		// Release the lock
		lm.Release(2)
		events <- "Node2-ReleasedLock"
	}()

	// Wait for Node 2 to start waiting
	expectEvent(t, events, "Node2-WaitingForLock")

	// Wait for Node 1's retry and release
	expectEvent(t, events, "Node1-Retrying")
	expectEvent(t, events, "Node1-RetrySucceeded")
	expectEvent(t, events, "Node1-ReleasedLock")

	// Verify Node 2 acquires the lock after Node 1 releases it
	select {
	case <-node2Acquired:
		// This is expected - Node 2 got the lock after Node 1 released it
	case <-time.After(1 * time.Second):
		t.Error("Node 2 should have acquired the lock after Node 1 released it")
	}

	// Verify final events
	expectEvent(t, events, "Node2-AcquiredLock")
	expectEvent(t, events, "Node2-ReleasedLock")
}

// Helper function to verify events occur in expected order
func expectEvent(t *testing.T, events chan string, expected string) {
	select {
	case event := <-events:
		if event != expected {
			t.Errorf("Expected event %s, got %s", expected, event)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("Timed out waiting for event: %s", expected)
	}
}
