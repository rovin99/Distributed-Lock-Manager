package wal

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LogEntry represents a single entry in the write-ahead log
type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	RequestID string    `json:"request_id"`
	Filename  string    `json:"filename"`
	Content   []byte    `json:"content"`
	Committed bool      `json:"committed"`
}

// WriteAheadLog implements a simple write-ahead logging system for file operations
type WriteAheadLog struct {
	mu      sync.Mutex
	logFile *os.File
	encoder *json.Encoder
	logPath string
	enabled bool
}

// NewWriteAheadLog creates a new write-ahead log
func NewWriteAheadLog(enabled bool) (*WriteAheadLog, error) {
	if !enabled {
		return &WriteAheadLog{enabled: false}, nil
	}

	// Create logs directory with absolute path
	logDir := "/home/naveen/Project/Distributed-Lock-Manager/logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create logs directory: %v", err)
	}

	// Use current timestamp for log filename
	timestamp := time.Now().Format("20060102-150405")
	logPath := filepath.Join(logDir, fmt.Sprintf("wal-%s.log", timestamp))

	// Open log file for writing
	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %v", err)
	}

	// Clean up old WAL files
	if err := cleanupOldWALFiles(logDir); err != nil {
		log.Printf("Warning: Failed to clean up old WAL files: %v", err)
	}

	return &WriteAheadLog{
		logFile: logFile,
		encoder: json.NewEncoder(logFile),
		logPath: logPath,
		enabled: enabled,
	}, nil
}

// cleanupOldWALFiles removes WAL files older than 24 hours
func cleanupOldWALFiles(logDir string) error {
	matches, err := filepath.Glob(filepath.Join(logDir, "wal-*.log"))
	if err != nil {
		return fmt.Errorf("failed to find log files: %v", err)
	}

	cutoff := time.Now().Add(-24 * time.Hour)
	for _, logPath := range matches {
		info, err := os.Stat(logPath)
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			if err := os.Remove(logPath); err != nil {
				log.Printf("Warning: Failed to remove old WAL file %s: %v", logPath, err)
			}
		}
	}
	return nil
}

// LogOperation records a file operation before it is actually performed
func (wal *WriteAheadLog) LogOperation(requestID, filename string, content []byte) error {
	if !wal.enabled {
		return nil
	}

	wal.mu.Lock()
	defer wal.mu.Unlock()

	entry := LogEntry{
		Timestamp: time.Now(),
		RequestID: requestID,
		Filename:  filename,
		Content:   content,
		Committed: false,
	}

	if err := wal.encoder.Encode(entry); err != nil {
		return fmt.Errorf("failed to write log entry: %v", err)
	}

	// Make sure the log is persisted to disk
	if err := wal.logFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync log file: %v", err)
	}

	return nil
}

// MarkCommitted marks an operation as successfully committed
func (wal *WriteAheadLog) MarkCommitted(requestID string) error {
	if !wal.enabled {
		return nil
	}

	wal.mu.Lock()
	defer wal.mu.Unlock()

	entry := LogEntry{
		Timestamp: time.Now(),
		RequestID: requestID,
		Committed: true,
	}

	if err := wal.encoder.Encode(entry); err != nil {
		return fmt.Errorf("failed to write commit log entry: %v", err)
	}

	// Make sure the log is persisted to disk
	if err := wal.logFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync log file: %v", err)
	}

	return nil
}

// Close closes the write-ahead log
func (wal *WriteAheadLog) Close() error {
	if !wal.enabled || wal.logFile == nil {
		return nil
	}

	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.logFile.Close()
}

// RecoverUncommittedOperations processes the log file to find uncommitted operations
// and returns a list of operations that need to be replayed
func RecoverUncommittedOperations(logDir string) ([]LogEntry, error) {
	// Use absolute path for log directory
	logDir = "/home/naveen/Project/Distributed-Lock-Manager/logs"

	// Find all log files
	matches, err := filepath.Glob(filepath.Join(logDir, "wal-*.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to find log files: %v", err)
	}

	uncommitted := make(map[string]LogEntry)

	// Process each log file
	for _, logPath := range matches {
		file, err := os.Open(logPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file %s: %v", logPath, err)
		}

		decoder := json.NewDecoder(file)
		for {
			var entry LogEntry
			if err := decoder.Decode(&entry); err != nil {
				if err == io.EOF {
					break
				}
				file.Close()
				return nil, fmt.Errorf("failed to decode log entry: %v", err)
			}

			// If this is a commit marker, remove the operation from uncommitted
			if entry.Committed {
				delete(uncommitted, entry.RequestID)
			} else {
				// Otherwise, add/update the operation in uncommitted
				uncommitted[entry.RequestID] = entry
			}
		}

		file.Close()
	}

	// Convert map values to slice
	result := make([]LogEntry, 0, len(uncommitted))
	for _, entry := range uncommitted {
		result = append(result, entry)
	}

	return result, nil
}
