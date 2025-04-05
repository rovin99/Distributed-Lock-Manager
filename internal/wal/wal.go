package wal

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// EntryType defines the type of WAL entry
type EntryType string

const (
	EntryTypeOperation EntryType = "OP"
	EntryTypeCommit    EntryType = "COMMIT"
)

// LogEntry represents a single entry in the write-ahead log
type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Type      EntryType `json:"type"`       // OP or COMMIT
	RequestID string    `json:"request_id"` // Associates OP and COMMIT

	// Fields specific to OP type
	Filename string `json:"filename,omitempty"`
	Content  []byte `json:"content,omitempty"`
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

	// Create logs directory, using environment variable if set
	logDir := os.Getenv("WAL_LOG_DIR")
	if logDir == "" {
		logDir = "logs" // Default if not specified
	}

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
		Type:      EntryTypeOperation,
		RequestID: requestID,
		Filename:  filename,
		Content:   content,
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
		Type:      EntryTypeCommit,
		RequestID: requestID,
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

// RecoverUncommittedOperations processes the log files to find operations that need to be replayed.
// It returns operations that have a COMMIT marker but might not have completed file writes.
func RecoverUncommittedOperations(logDir string) ([]LogEntry, error) {
	// If logDir is not specified, use the environment variable or default
	if logDir == "" {
		logDir = os.Getenv("WAL_LOG_DIR")
		if logDir == "" {
			logDir = "logs" // Default if not specified
		}
	}

	// Use the provided log directory path as is
	matches, err := filepath.Glob(filepath.Join(logDir, "wal-*.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to find log files: %v", err)
	}

	// Sort the log files by modification time (oldest first)
	sort.Slice(matches, func(i, j int) bool {
		iInfo, err := os.Stat(matches[i])
		if err != nil {
			return false
		}
		jInfo, err := os.Stat(matches[j])
		if err != nil {
			return true
		}
		return iInfo.ModTime().Before(jInfo.ModTime())
	})

	// Store operation details keyed by RequestID
	loggedOpsData := make(map[string]LogEntry)
	// Store committed RequestIDs
	committedIDs := make(map[string]bool)
	// Preserve commit order
	orderedCommitIDs := []string{}

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

			switch entry.Type {
			case EntryTypeOperation:
				// Store the latest operation details for this request ID
				loggedOpsData[entry.RequestID] = entry
			case EntryTypeCommit:
				// Mark as committed, potentially adding to ordered list
				if !committedIDs[entry.RequestID] {
					committedIDs[entry.RequestID] = true
					orderedCommitIDs = append(orderedCommitIDs, entry.RequestID)
				}
			}
		}

		file.Close()
	}

	// Build the list of committed operations that need to be checked
	result := make([]LogEntry, 0, len(committedIDs))
	for _, reqID := range orderedCommitIDs {
		if opData, exists := loggedOpsData[reqID]; exists {
			// We have the original operation data for this committed request
			result = append(result, opData)
		} else {
			// This case (Commit without preceding OP) should ideally not happen
			log.Printf("Warning: Found commit marker for request ID %s but no corresponding operation log entry.", reqID)
		}
	}

	return result, nil
}
