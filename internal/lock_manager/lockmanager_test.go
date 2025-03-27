package lock_manager

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	// Create logs directory if it doesn't exist
	if err := os.MkdirAll("logs", 0755); err != nil {
		log.Printf("Failed to create logs directory: %v", err)
	}

	// Redirect test logs to file
	logFile, err := os.OpenFile(filepath.Join("logs", "lockmanager_test.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Failed to open test log file: %v", err)
	} else {
		log.SetOutput(logFile)
	}
}

func TestLockManagerBasic(t *testing.T) {
	lm := NewLockManager(nil)

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

	// Test IsLocked and CurrentHolder
	if !lm.IsLocked() {
		t.Error("Lock should be marked as locked")
	}
	if lm.CurrentHolder() != 1 {
		t.Errorf("Current holder should be 1, got %d", lm.CurrentHolder())
	}

	// Test release
	if !lm.Release(1) {
		t.Error("Failed to release lock")
	}
	if lm.HasLock(1) {
		t.Error("Client should not have lock after releasing it")
	}
	if lm.IsLocked() {
		t.Error("Lock should be marked as free after release")
	}
	if lm.CurrentHolder() != -1 {
		t.Errorf("Current holder should be -1 after release, got %d", lm.CurrentHolder())
	}

	// Test releasing a lock not held
	if lm.Release(1) {
		t.Error("Should not be able to release a lock not held")
	}
}

func TestLockManagerConcurrent(t *testing.T) {
	lm := NewLockManager(nil)
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
				if !lm.Acquire(id) {
					continue
				}

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
	lm := NewLockManager(nil)

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
	lm := NewLockManager(nil)

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

func TestAcquireWithTimeout(t *testing.T) {
	lm := NewLockManager(nil)

	// Have client 1 acquire the lock
	lm.Acquire(1)

	// Try to acquire with a short timeout for client 2
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// This should time out
	start := time.Now()
	success := lm.AcquireWithTimeout(2, ctx)
	elapsed := time.Since(start)

	if success {
		t.Error("Client 2 should not have acquired the lock due to timeout")
	}

	if elapsed < 90*time.Millisecond {
		t.Errorf("Timeout occurred too quickly: %v", elapsed)
	}

	// Release the lock
	lm.Release(1)

	// Try again with a longer timeout - should succeed quickly
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start = time.Now()
	success = lm.AcquireWithTimeout(2, ctx)
	elapsed = time.Since(start)

	if !success {
		t.Error("Client 2 should have acquired the lock")
	}

	if elapsed > 100*time.Millisecond {
		t.Errorf("Lock acquisition took too long: %v", elapsed)
	}

	// Clean up
	lm.Release(2)
}

func TestFIFOFairness(t *testing.T) {
	lm := NewLockManager(nil)

	// Have client 1 acquire the lock
	lm.Acquire(1)

	// Set up channels to track when clients get the lock
	client2Acquired := make(chan bool)
	client3Acquired := make(chan bool)

	// Start client 2 waiting for the lock
	go func() {
		lm.Acquire(2)
		client2Acquired <- true
	}()

	// Give client 2 time to queue up
	time.Sleep(50 * time.Millisecond)

	// Start client 3 waiting for the lock
	go func() {
		lm.Acquire(3)
		client3Acquired <- true
	}()

	// Give client 3 time to queue up
	time.Sleep(50 * time.Millisecond)

	// Release the lock
	lm.Release(1)

	// Verify client 2 gets the lock before client 3 (FIFO order)
	select {
	case <-client2Acquired:
		// This is expected - client 2 was first in queue
	case <-client3Acquired:
		t.Error("Client 3 got the lock before client 2, violating FIFO order")
	case <-time.After(100 * time.Millisecond):
		t.Error("No client acquired the lock within timeout")
	}

	// Now release client 2's lock
	lm.Release(2)

	// Verify client 3 gets the lock
	select {
	case <-client3Acquired:
		// This is expected - client 3 was next in queue
	case <-time.After(100 * time.Millisecond):
		t.Error("Client 3 didn't acquire the lock after client 2 released it")
	}

	// Clean up
	lm.Release(3)
}

func TestStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	lm := NewLockManager(nil)
	numClients := 100
	opsPerClient := 50

	// Track total operations and conflicts
	var totalOps int64
	var conflicts int64

	// Track current lock holder
	var currentHolder int32 = -1
	var holderMu sync.Mutex

	// Use a WaitGroup to wait for all clients
	var wg sync.WaitGroup
	wg.Add(numClients)

	// Start all clients
	start := time.Now()
	for i := 0; i < numClients; i++ {
		clientID := int32(i + 1)

		go func(id int32) {
			defer wg.Done()

			for j := 0; j < opsPerClient; j++ {
				// Try to acquire the lock
				lm.Acquire(id)

				// Check for conflicts
				holderMu.Lock()
				if currentHolder != -1 {
					atomic.AddInt64(&conflicts, 1)
				}
				currentHolder = id
				holderMu.Unlock()

				// Simulate work (very brief)
				time.Sleep(time.Microsecond)

				// Release the lock
				holderMu.Lock()
				currentHolder = -1
				holderMu.Unlock()
				lm.Release(id)

				atomic.AddInt64(&totalOps, 1)
			}
		}(clientID)
	}

	// Wait for all clients to finish
	wg.Wait()
	elapsed := time.Since(start)

	// Report results
	opsPerSec := float64(totalOps) / elapsed.Seconds()
	t.Logf("Completed %d lock operations in %v (%f ops/sec)", totalOps, elapsed, opsPerSec)
	t.Logf("Detected %d conflicts", conflicts)

	// There should be no conflicts
	if conflicts > 0 {
		t.Errorf("Detected %d lock conflicts", conflicts)
	}
}

func BenchmarkLockAcquireRelease(b *testing.B) {
	lm := NewLockManager(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clientID := int32(i % 1000)
		lm.Acquire(clientID)
		lm.Release(clientID)
	}
}

func BenchmarkConcurrentLockOperations(b *testing.B) {
	lm := NewLockManager(nil)
	numGoroutines := runtime.GOMAXPROCS(0) * 2

	b.ResetTimer()

	// Run b.N operations spread across goroutines
	var wg sync.WaitGroup
	opsPerGoroutine := b.N / numGoroutines

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for i := 0; i < opsPerGoroutine; i++ {
				clientID := int32((id * 1000) + (i % 1000))
				lm.Acquire(clientID)
				lm.Release(clientID)
			}
		}(g)
	}

	wg.Wait()
}

func TestLongRunningLockHolder(t *testing.T) {
	lm := NewLockManager(nil)

	// Client 1 acquires the lock
	if !lm.Acquire(1) {
		t.Fatal("Client 1 failed to acquire lock")
	}

	// Start a goroutine for Client 2 attempting to acquire the lock
	lockAcquired := make(chan bool)
	go func() {
		lm.Acquire(2)
		lockAcquired <- true
	}()

	// Wait for 30 seconds to simulate long-running lock holder
	time.Sleep(30 * time.Second)

	// Release lock from Client 1
	lm.Release(1)

	// Verify Client 2 acquires the lock after release
	select {
	case <-lockAcquired:
		// Success
	case <-time.After(5 * time.Second):
		t.Error("Client 2 did not acquire lock after long wait")
	}
}

func TestClientDisconnection(t *testing.T) {
	lm := NewLockManager(nil)

	// Client 1 acquires the lock
	if !lm.Acquire(1) {
		t.Fatal("Client 1 failed to acquire lock")
	}

	// Simulate client 1 disconnection without releasing the lock
	lm.ReleaseLockIfHeld(1)

	// Client 2 attempts to acquire the lock
	lockAcquired := make(chan bool)
	go func() {
		lm.Acquire(2)
		lockAcquired <- true
	}()

	// Verify Client 2 acquires the lock after Client 1 disconnection
	select {
	case <-lockAcquired:
		// Success
	case <-time.After(5 * time.Second):
		t.Error("Client 2 did not acquire lock after Client 1 disconnection")
	}
}

func TestMultipleTimeoutClients(t *testing.T) {
	lm := NewLockManager(nil)

	// Client 1 acquires the lock
	if !lm.Acquire(1) {
		t.Fatal("Client 1 failed to acquire lock")
	}

	// Client 2 attempts to acquire the lock with a 5-second timeout
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	client2Acquired := make(chan bool)
	go func() {
		client2Acquired <- lm.AcquireWithTimeout(2, ctx2)
	}()

	// Client 3 attempts to acquire the lock with a 10-second timeout
	ctx3, cancel3 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel3()
	client3Acquired := make(chan bool)
	go func() {
		client3Acquired <- lm.AcquireWithTimeout(3, ctx3)
	}()

	// Wait for 6 seconds and verify Client 2 times out
	select {
	case success := <-client2Acquired:
		if success {
			t.Error("Client 2 should have timed out but acquired the lock")
		}
	case <-time.After(6 * time.Second):
		t.Error("Client 2 did not timeout as expected")
	}

	// Release lock from Client 1
	lm.Release(1)

	// Verify Client 3 acquires the lock
	select {
	case success := <-client3Acquired:
		if !success {
			t.Error("Client 3 should have acquired the lock but did not")
		}
	case <-time.After(5 * time.Second):
		t.Error("Client 3 did not acquire lock after Client 1 released it")
	}
}

func TestMixedOperation(t *testing.T) {
	lm := NewLockManager(nil)
	var wg sync.WaitGroup

	// Client 1 acquires and releases lock repeatedly
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			lm.Acquire(1)
			time.Sleep(10 * time.Millisecond)
			lm.Release(1)
		}
	}()

	// Client 2 attempts file operations without lock
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			if lm.HasLock(2) {
				t.Error("Client 2 should not have lock")
			}
			time.Sleep(15 * time.Millisecond)
		}
	}()

	// Client 3 disconnects during operation
	wg.Add(1)
	go func() {
		defer wg.Done()
		lm.Acquire(3)
		lm.ReleaseLockIfHeld(3)
	}()

	// Client 4 attempts to acquire lock with timeout
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if lm.AcquireWithTimeout(4, ctx) {
			lm.Release(4)
		}
	}()

	wg.Wait()
}

// TestPacketLossRetryMechanism tests the scenario where a response packet is lost
// and verifies that the retry mechanism ensures liveness
func TestPacketLossRetryMechanism(t *testing.T) {
	lm := NewLockManager(nil)

	// Create a channel to simulate packet loss
	responseLost := make(chan struct{})

	// Create a channel to track steps in the process
	events := make(chan string, 10)

	// Set up Node 1 to acquire the lock
	go func() {
		// First attempt - simulate that server responds, but response is lost
		// We don't call lm.Acquire directly here to avoid blocking

		events <- "Node1-Starting"

		// Check that the lock is free initially
		if lm.HasLock(1) {
			t.Error("Node 1 should not have the lock initially")
		}

		// Notify that lock is being acquired (response lost)
		events <- "Node1-FirstAttempt"

		// Background goroutine to grant the lock while simulating lost response
		go func() {
			// Get the lock for Node 1, but don't let Node 1 know yet
			lm.mu.Lock()
			lm.lockHolder = 1 // Directly assign to simulate server granting but client not knowing
			lm.mu.Unlock()

			events <- "Server-GrantedLock-ResponseLost"
			responseLost <- struct{}{} // Signal that the response was "lost"
		}()

		// Wait for the "packet loss" to be simulated
		<-responseLost

		// After timeout (simulated), Node 1 retries
		events <- "Node1-RetryingAfterTimeout"

		// This retry should succeed immediately since Node 1 already holds the lock
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		success := lm.AcquireWithTimeout(1, ctx)

		if !success {
			t.Error("Node 1 retry should succeed because it already holds the lock")
		}

		events <- "Node1-RetrySucceeded"

		// Simulate Node 1 doing work with the lock
		time.Sleep(100 * time.Millisecond)

		// Node 1 releases the lock
		if !lm.Release(1) {
			t.Error("Node 1 should be able to release the lock")
		}

		events <- "Node1-ReleasedLock"
	}()

	// Wait for Node 1 to start and the server to "lose" the response
	expectEvent(t, events, "Node1-Starting", 1*time.Second)
	expectEvent(t, events, "Node1-FirstAttempt", 1*time.Second)
	expectEvent(t, events, "Server-GrantedLock-ResponseLost", 1*time.Second)

	// Wait a bit to ensure Node 1 has time to prepare for retry
	time.Sleep(50 * time.Millisecond)

	// Set up Node 2 to wait for the lock
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

		// Release the lock
		lm.Release(2)
		events <- "Node2-ReleasedLock"
	}()

	// Now wait for Node 1's retry and release
	expectEvent(t, events, "Node1-RetryingAfterTimeout", 1*time.Second)
	expectEvent(t, events, "Node1-RetrySucceeded", 1*time.Second)
	expectEvent(t, events, "Node2-WaitingForLock", 1*time.Second)
	expectEvent(t, events, "Node1-ReleasedLock", 1*time.Second)

	// Verify Node 2 acquires the lock after Node 1 releases it
	select {
	case <-node2Acquired:
		// This is expected - Node 2 got the lock after Node 1 released it
	case <-time.After(1 * time.Second):
		t.Error("Node 2 should have acquired the lock after Node 1 released it")
	}

	// Verify final events
	expectEvent(t, events, "Node2-AcquiredLock", 1*time.Second)
	expectEvent(t, events, "Node2-ReleasedLock", 1*time.Second)
}

// Helper function to verify events occur in expected order with a specific timeout
func expectEvent(t *testing.T, events chan string, expected string, timeout time.Duration) {
	select {
	case event := <-events:
		if event != expected {
			t.Errorf("Expected event %s, got %s", expected, event)
		}
	case <-time.After(timeout):
		t.Errorf("Timed out waiting for event: %s", expected)
	}
}
