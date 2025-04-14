package server

import (
	"sort"
	"sync"
	"time"
)

// RequestCache is a thread-safe cache for storing responses to processed requests
// to ensure idempotency when clients retry requests
type RequestCache struct {
	mu             sync.Mutex
	cache          map[string]interface{} // requestId -> response (can be any type)
	expires        map[string]time.Time   // requestId -> expiration time
	inProgress     map[string]bool        // requestId -> whether request is being processed
	inProgressCond *sync.Cond             // Condition variable for waiting on in-progress requests
	ttl            time.Duration          // Time-to-live for cache entries
	maxSize        int                    // Maximum number of entries in the cache
}

// NewRequestCache creates a new request cache with the specified TTL
func NewRequestCache(ttl time.Duration) *RequestCache {
	return NewRequestCacheWithSize(ttl, 10000) // Default to 10,000 entries
}

// NewRequestCacheWithSize creates a new request cache with the specified TTL and max size
func NewRequestCacheWithSize(ttl time.Duration, maxSize int) *RequestCache {
	cache := &RequestCache{
		cache:      make(map[string]interface{}),
		expires:    make(map[string]time.Time),
		inProgress: make(map[string]bool),
		ttl:        ttl,
		maxSize:    maxSize,
	}
	cache.inProgressCond = sync.NewCond(&cache.mu)
	// Start a goroutine to periodically clean up expired entries
	go cache.cleanup()
	return cache
}

// Get retrieves a cached response for the given request ID
// Returns the response and true if found and not expired, or nil and false otherwise
// If the request is in progress, waits for it to complete or timeout
func (rc *RequestCache) Get(requestID string) (interface{}, bool) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Check if we have a cached response
	if resp, exists := rc.cache[requestID]; exists {
		if time.Now().Before(rc.expires[requestID]) {
			return resp, true
		}
		// Expired; clean up
		delete(rc.cache, requestID)
		delete(rc.expires, requestID)
	}

	// Check if the request is currently being processed by another thread
	if inProgress, exists := rc.inProgress[requestID]; exists && inProgress {
		// Wait until the request is no longer in progress or times out
		waitTimeout := time.NewTimer(5 * time.Second)
		defer waitTimeout.Stop()

		waiting := true
		for waiting {
			// Use a waitCh to wake up if the condition changes or timeout occurs
			waitCh := make(chan struct{})
			go func() {
				rc.inProgressCond.Wait()
				close(waitCh)
			}()

			// Temporarily release the lock while waiting
			rc.mu.Unlock()

			// Wait for either condition signal or timeout
			select {
			case <-waitCh:
				// Reacquire the lock
				rc.mu.Lock()
				// Check if the request is still in progress
				if _, exists := rc.inProgress[requestID]; !exists || !rc.inProgress[requestID] {
					waiting = false
				}
			case <-waitTimeout.C:
				// Timeout occurred, reacquire the lock and stop waiting
				rc.mu.Lock()
				waiting = false
			}
		}

		// Now that we're no longer waiting, check if a response was cached
		if resp, exists := rc.cache[requestID]; exists {
			if time.Now().Before(rc.expires[requestID]) {
				return resp, true
			}
			// Expired; clean up
			delete(rc.cache, requestID)
			delete(rc.expires, requestID)
		}
	}

	return nil, false
}

// MarkInProgress marks a request as being processed
// Returns true if successfully marked, false if already in progress
func (rc *RequestCache) MarkInProgress(requestID string) bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if _, exists := rc.inProgress[requestID]; exists && rc.inProgress[requestID] {
		return false
	}

	rc.inProgress[requestID] = true
	return true
}

// Set stores a response for a request ID with the configured TTL
// and removes the in-progress status
func (rc *RequestCache) Set(requestID string, resp interface{}) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// If we've reached the max size, remove some entries
	if len(rc.cache) >= rc.maxSize {
		rc.evictOldestEntries(rc.maxSize / 10) // Remove 10% of entries
	}

	rc.cache[requestID] = resp
	rc.expires[requestID] = time.Now().Add(rc.ttl)

	// Mark request as no longer in progress
	delete(rc.inProgress, requestID)

	// Signal waiters that the request is complete
	rc.inProgressCond.Broadcast()
}

// evictOldestEntries removes the specified number of oldest entries from the cache
func (rc *RequestCache) evictOldestEntries(count int) {
	// If count is greater than cache size, adjust to cache size
	if count > len(rc.cache) {
		count = len(rc.cache)
	}

	// Create a slice of request IDs sorted by expiration time
	type expiryEntry struct {
		requestID string
		expiry    time.Time
	}
	entries := make([]expiryEntry, 0, len(rc.expires))
	for reqID, expiry := range rc.expires {
		entries = append(entries, expiryEntry{reqID, expiry})
	}

	// Sort by expiry time, oldest first
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].expiry.Before(entries[j].expiry)
	})

	// Delete the oldest entries
	for i := 0; i < count; i++ {
		reqID := entries[i].requestID
		delete(rc.cache, reqID)
		delete(rc.expires, reqID)
	}
}

// cleanup periodically removes expired entries from the cache
func (rc *RequestCache) cleanup() {
	for {
		time.Sleep(1 * time.Minute) // Check every minute

		rc.mu.Lock()
		now := time.Now()
		expiredCount := 0

		// Find expired entries
		for reqID, expiry := range rc.expires {
			if now.After(expiry) {
				delete(rc.cache, reqID)
				delete(rc.expires, reqID)
				expiredCount++
			}
		}

		// Also clean up any stale in-progress requests (should never happen in normal operation)
		// but provides resilience against bugs or crashes
		for reqID := range rc.inProgress {
			// If there's no expiry for this reqID, it might be a stale in-progress marker
			if _, exists := rc.expires[reqID]; !exists {
				delete(rc.inProgress, reqID)
			}
		}

		// If we're still near max capacity after removing expired entries,
		// proactively remove some of the oldest entries
		if len(rc.cache) >= rc.maxSize*9/10 { // If we're at 90% capacity
			rc.evictOldestEntries(rc.maxSize / 5) // Remove 20% of entries
		}

		rc.mu.Unlock()
	}
}

// Clear removes all entries from the request cache
// This is useful during server role transitions to avoid stale state
func (rc *RequestCache) Clear() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Clear all maps
	rc.cache = make(map[string]interface{})
	rc.expires = make(map[string]time.Time)
	rc.inProgress = make(map[string]bool)

	// Signal any waiting goroutines
	rc.inProgressCond.Broadcast()
}
