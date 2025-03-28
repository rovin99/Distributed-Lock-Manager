package server

import (
	"sync"
	"time"

	pb "Distributed-Lock-Manager/proto"
)

// RequestCache is a thread-safe cache for storing responses to processed requests
// to ensure idempotency when clients retry requests
type RequestCache struct {
	mu      sync.Mutex
	cache   map[string]*pb.Response // requestId -> response
	expires map[string]time.Time    // requestId -> expiration time
	ttl     time.Duration           // Time-to-live for cache entries
}

// NewRequestCache creates a new request cache with the specified TTL
func NewRequestCache(ttl time.Duration) *RequestCache {
	cache := &RequestCache{
		cache:   make(map[string]*pb.Response),
		expires: make(map[string]time.Time),
		ttl:     ttl,
	}
	// Start a goroutine to periodically clean up expired entries
	go cache.cleanup()
	return cache
}

// Get retrieves a cached response for the given request ID
// Returns the response and true if found and not expired, or nil and false otherwise
func (rc *RequestCache) Get(requestID string) (*pb.Response, bool) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if resp, exists := rc.cache[requestID]; exists {
		if time.Now().Before(rc.expires[requestID]) {
			return resp, true
		}
		// Expired; clean up
		delete(rc.cache, requestID)
		delete(rc.expires, requestID)
	}
	return nil, false
}

// Set stores a response for a request ID with the configured TTL
func (rc *RequestCache) Set(requestID string, resp *pb.Response) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.cache[requestID] = resp
	rc.expires[requestID] = time.Now().Add(rc.ttl)
}

// cleanup periodically removes expired entries from the cache
func (rc *RequestCache) cleanup() {
	for {
		time.Sleep(1 * time.Minute) // Check every minute

		rc.mu.Lock()
		now := time.Now()
		for reqID, expiry := range rc.expires {
			if now.After(expiry) {
				delete(rc.cache, reqID)
				delete(rc.expires, reqID)
			}
		}
		rc.mu.Unlock()
	}
}
