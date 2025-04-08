package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// OperationType represents different types of operations tracked in metrics
type OperationType string

// Define operation types
const (
	OpLockAcquire OperationType = "lock_acquire"
	OpLockRelease OperationType = "lock_release"
	OpFileAppend  OperationType = "file_append"
	OpRenewLease  OperationType = "renew_lease"
	OpReplication OperationType = "replication"
	OpHeartbeat   OperationType = "heartbeat"
	OpInitClient  OperationType = "client_init"
)

// PerformanceMetrics tracks various performance metrics for the server
type PerformanceMetrics struct {
	mu                   sync.RWMutex
	startTime            time.Time
	operationCounts      map[OperationType]int64
	operationLatencies   map[OperationType][]time.Duration
	failureCounts        map[OperationType]int64
	leaseRenewalCount    int64
	lockAcquisitions     int64
	lockReleases         int64
	lockContention       int64 // times lock was already held when attempted to acquire
	replicationLatency   []time.Duration
	heartbeatLatency     []time.Duration
	heartbeatFailures    int64
	fencingActivations   int64
	currentLockHolder    int32
	lockHolderChanges    int64
	concurrentRequests   int64 // current number of requests being processed
	maxConcurrentReqs    int64 // maximum number of concurrent requests observed
	lastStateReplication time.Time
	maxQueueLength       int           // maximum lock queue length observed
	avgQueueWaitTime     time.Duration // average time clients wait in queue
	queueWaitSamples     int           // number of wait time samples collected
	logger               *log.Logger
}

// NewPerformanceMetrics creates a new metrics tracker
func NewPerformanceMetrics(logger *log.Logger) *PerformanceMetrics {
	if logger == nil {
		logger = log.New(os.Stdout, "[Metrics] ", log.LstdFlags)
	}

	return &PerformanceMetrics{
		startTime:          time.Now(),
		operationCounts:    make(map[OperationType]int64),
		operationLatencies: make(map[OperationType][]time.Duration),
		failureCounts:      make(map[OperationType]int64),
		replicationLatency: make([]time.Duration, 0, 100),
		heartbeatLatency:   make([]time.Duration, 0, 100),
		logger:             logger,
	}
}

// TrackOperationLatency tracks the latency of an operation
func (m *PerformanceMetrics) TrackOperationLatency(op OperationType, latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.operationCounts[op]++

	// Store at most 100 latency samples per operation type
	if len(m.operationLatencies[op]) < 100 {
		m.operationLatencies[op] = append(m.operationLatencies[op], latency)
	}
}

// TrackOperationFailure tracks a failed operation
func (m *PerformanceMetrics) TrackOperationFailure(op OperationType) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.failureCounts[op]++
}

// RecordLockAcquisition tracks a successful lock acquisition
func (m *PerformanceMetrics) RecordLockAcquisition(clientID int32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.currentLockHolder != -1 && m.currentLockHolder != clientID {
		m.lockHolderChanges++
	}
	m.currentLockHolder = clientID
	m.lockAcquisitions++
}

// RecordLockRelease tracks a lock release
func (m *PerformanceMetrics) RecordLockRelease() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.currentLockHolder = -1
	m.lockReleases++
}

// RecordLockContention tracks when a lock could not be acquired due to contention
func (m *PerformanceMetrics) RecordLockContention() {
	atomic.AddInt64(&m.lockContention, 1)
}

// RecordLeaseRenewal tracks a lease renewal
func (m *PerformanceMetrics) RecordLeaseRenewal() {
	atomic.AddInt64(&m.leaseRenewalCount, 1)
}

// RecordReplicationLatency tracks the latency of a state replication operation
func (m *PerformanceMetrics) RecordReplicationLatency(latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Store at most 100 latency samples
	if len(m.replicationLatency) < 100 {
		m.replicationLatency = append(m.replicationLatency, latency)
	}
	m.lastStateReplication = time.Now()
}

// RecordHeartbeatLatency tracks the latency of a heartbeat operation
func (m *PerformanceMetrics) RecordHeartbeatLatency(latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Store at most 100 latency samples
	if len(m.heartbeatLatency) < 100 {
		m.heartbeatLatency = append(m.heartbeatLatency, latency)
	}
}

// RecordHeartbeatFailure tracks a failed heartbeat
func (m *PerformanceMetrics) RecordHeartbeatFailure() {
	atomic.AddInt64(&m.heartbeatFailures, 1)
}

// RecordFencingActivation tracks when fencing is activated
func (m *PerformanceMetrics) RecordFencingActivation() {
	atomic.AddInt64(&m.fencingActivations, 1)
}

// IncrementConcurrentRequests increments the counter for concurrent requests
func (m *PerformanceMetrics) IncrementConcurrentRequests() {
	current := atomic.AddInt64(&m.concurrentRequests, 1)

	// Update maximum if current is greater
	for {
		max := atomic.LoadInt64(&m.maxConcurrentReqs)
		if current <= max {
			break
		}
		if atomic.CompareAndSwapInt64(&m.maxConcurrentReqs, max, current) {
			break
		}
	}
}

// DecrementConcurrentRequests decrements the counter for concurrent requests
func (m *PerformanceMetrics) DecrementConcurrentRequests() {
	atomic.AddInt64(&m.concurrentRequests, -1)
}

// RecordQueueLength updates the maximum queue length if the current length is greater
func (m *PerformanceMetrics) RecordQueueLength(length int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if length > m.maxQueueLength {
		m.maxQueueLength = length
	}
}

// RecordQueueWaitTime records the time a client waited in the queue
func (m *PerformanceMetrics) RecordQueueWaitTime(waitTime time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Update rolling average
	totalWaitTime := m.avgQueueWaitTime.Nanoseconds() * int64(m.queueWaitSamples)
	m.queueWaitSamples++
	m.avgQueueWaitTime = time.Duration((totalWaitTime + waitTime.Nanoseconds()) / int64(m.queueWaitSamples))
}

// GetStats returns a JSON representation of the metrics
func (m *PerformanceMetrics) GetStats() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Calculate average latencies
	avgLatencies := make(map[string]float64)
	for op, latencies := range m.operationLatencies {
		if len(latencies) > 0 {
			var sum time.Duration
			for _, l := range latencies {
				sum += l
			}
			avgLatencies[string(op)] = float64(sum) / float64(len(latencies)) / float64(time.Millisecond)
		}
	}

	// Calculate average replication latency
	var avgReplicationLatency float64
	if len(m.replicationLatency) > 0 {
		var sum time.Duration
		for _, l := range m.replicationLatency {
			sum += l
		}
		avgReplicationLatency = float64(sum) / float64(len(m.replicationLatency)) / float64(time.Millisecond)
	}

	// Calculate average heartbeat latency
	var avgHeartbeatLatency float64
	if len(m.heartbeatLatency) > 0 {
		var sum time.Duration
		for _, l := range m.heartbeatLatency {
			sum += l
		}
		avgHeartbeatLatency = float64(sum) / float64(len(m.heartbeatLatency)) / float64(time.Millisecond)
	}

	// Create stats map
	stats := map[string]interface{}{
		"uptime_seconds":             time.Since(m.startTime).Seconds(),
		"operation_counts":           m.operationCounts,
		"average_latencies_ms":       avgLatencies,
		"failure_counts":             m.failureCounts,
		"lease_renewals":             m.leaseRenewalCount,
		"lock_acquisitions":          m.lockAcquisitions,
		"lock_releases":              m.lockReleases,
		"lock_contention":            m.lockContention,
		"lock_holder_changes":        m.lockHolderChanges,
		"current_lock_holder":        m.currentLockHolder,
		"avg_replication_latency_ms": avgReplicationLatency,
		"avg_heartbeat_latency_ms":   avgHeartbeatLatency,
		"heartbeat_failures":         m.heartbeatFailures,
		"fencing_activations":        m.fencingActivations,
		"concurrent_requests":        m.concurrentRequests,
		"max_concurrent_requests":    m.maxConcurrentReqs,
		"max_queue_length":           m.maxQueueLength,
		"avg_queue_wait_time_ms":     float64(m.avgQueueWaitTime) / float64(time.Millisecond),
		"queue_wait_samples":         m.queueWaitSamples,
		"last_replication_time":      m.lastStateReplication.Format(time.RFC3339),
	}

	return json.Marshal(stats)
}

// StartMetricsServer starts an HTTP server to expose metrics
func (m *PerformanceMetrics) StartMetricsServer(address string) {
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		stats, err := m.GetStats()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Error generating metrics: %v", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(stats)
	})

	go func() {
		m.logger.Printf("Metrics server listening on %s", address)
		if err := http.ListenAndServe(address, nil); err != nil {
			m.logger.Printf("Metrics server error: %v", err)
		}
	}()
}

// LogMetricsSummary logs a summary of metrics
func (m *PerformanceMetrics) LogMetricsSummary() {
	stats, err := m.GetStats()
	if err != nil {
		m.logger.Printf("Error generating metrics summary: %v", err)
		return
	}

	m.logger.Printf("Metrics summary: %s", string(stats))
}
