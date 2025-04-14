package file_manager

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"Distributed-Lock-Manager/internal/lock_manager"
)

func init() {
	// Use the root-level logs directory
	logsDir := filepath.Join("..", "..", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		log.Printf("Failed to create logs directory: %v", err)
	}

	// Redirect test logs to file
	logFile, err := os.OpenFile(filepath.Join(logsDir, "filemanager_test.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Failed to open test log file: %v", err)
	} else {
		log.SetOutput(logFile)
	}
}

// setupTestEnvironment creates a temporary test directory and redirects "data" to it
func setupTestEnvironment(t *testing.T) (string, func()) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "filemanager_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Use the root-level data directory
	rootDataDir := filepath.Join("..", "..", "data")

	// Create a symbolic link to redirect "data" to our temp directory
	originalDataDir := "data"
	var originalDirExists bool
	if _, err := os.Stat(originalDataDir); err == nil {
		originalDirExists = true
		// Rename the original data directory temporarily
		os.Rename(originalDataDir, originalDataDir+"_backup")
	}

	// Create the data directory in our temp location
	os.Mkdir(filepath.Join(tempDir, "data"), 0755)

	// Create a symbolic link from the root-level data directory to our temp directory
	os.MkdirAll(rootDataDir, 0755)
	os.Symlink(filepath.Join(tempDir, "data"), "data")

	// Ensure all files within the data directory are empty to avoid contamination
	for i := 0; i < 100; i++ {
		filename := fmt.Sprintf("file_%d", i)
		// Create empty files
		err := os.WriteFile(filepath.Join("data", filename), []byte{}, 0644)
		if err != nil {
			t.Logf("Warning: Failed to create empty file %s: %v", filename, err)
		}
	}

	// Return cleanup function
	cleanup := func() {
		os.Remove("data") // Remove the symlink
		os.RemoveAll(tempDir)
		if originalDirExists {
			os.Rename(originalDataDir+"_backup", originalDataDir)
		}
	}

	return tempDir, cleanup
}

func TestNewFileManager(t *testing.T) {
	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)

	// Test with sync disabled
	fm1 := NewFileManager(false, lm)
	if fm1 == nil {
		t.Fatal("NewFileManager returned nil")
	}
	if fm1.openFiles == nil {
		t.Error("openFiles map not initialized")
	}
	if fm1.fileLocks == nil {
		t.Error("fileLocks map not initialized")
	}
	if fm1.logger == nil {
		t.Error("logger not initialized")
	}
	if fm1.syncEnabled != false {
		t.Error("syncEnabled should be false")
	}

	// Test with sync enabled
	fm2 := NewFileManager(true, lm)
	if fm2.syncEnabled != true {
		t.Error("syncEnabled should be true")
	}
}

func TestBasicFileOperations(t *testing.T) {
	_, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)
	fm := NewFileManager(true, lm) // Enable sync for testing

	// Test valid filename and content
	testContent := []byte("test content")
	clientID := int32(1)

	// Acquire lock for the client and get the token
	success, token := lm.Acquire(clientID)
	if !success {
		t.Fatal("Failed to acquire lock")
	}

	// Ensure lock is released in all cases
	defer lm.Release(clientID, token)

	err := fm.AppendToFile("file_0", testContent, clientID, token)
	if err != nil {
		t.Errorf("AppendToFile failed with valid input: %v", err)
	}

	// Verify content was written
	content, err := os.ReadFile(filepath.Join("data", "file_0"))
	if err != nil {
		t.Errorf("Failed to read file: %v", err)
	}
	if string(content) != string(testContent) {
		t.Errorf("File content mismatch. Got %s, want %s", content, testContent)
	}

	// Test appending more content
	moreContent := []byte(" additional content")
	err = fm.AppendToFile("file_0", moreContent, clientID, token)
	if err != nil {
		t.Errorf("Failed to append more content: %v", err)
	}

	// Verify appended content
	content, err = os.ReadFile(filepath.Join("data", "file_0"))
	if err != nil {
		t.Errorf("Failed to read file after append: %v", err)
	}
	expectedContent := string(testContent) + string(moreContent)
	if string(content) != expectedContent {
		t.Errorf("Appended content mismatch. Got %s, want %s", content, expectedContent)
	}
}

func TestFilenameValidation(t *testing.T) {
	_, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)
	fm := NewFileManager(false, lm)
	testContent := []byte("test content")
	clientID := int32(1)

	// Acquire lock for the client and get the token
	success, token := lm.Acquire(clientID)
	if !success {
		t.Fatal("Failed to acquire lock")
	}

	// Ensure lock is released in all cases
	defer lm.Release(clientID, token)

	// Test cases for invalid filenames
	invalidFilenames := []struct {
		name  string
		input string
	}{
		{"invalid prefix", "invalid_file"},
		{"out of range (high)", "file_100"},
		{"out of range (negative)", "file_-1"},
		{"non-numeric", "file_abc"},
		{"empty", "file_"},
	}

	for _, tc := range invalidFilenames {
		t.Run(tc.name, func(t *testing.T) {
			err := fm.AppendToFile(tc.input, testContent, clientID, token)
			if err == nil {
				t.Errorf("AppendToFile should fail with %s", tc.input)
			}
		})
	}

	// Test all valid filenames
	for i := 0; i < 100; i++ {
		filename := fmt.Sprintf("file_%d", i)
		err := fm.AppendToFile(filename, testContent, clientID, token)
		if err != nil {
			t.Errorf("AppendToFile failed with valid filename %s: %v", filename, err)
		}
	}
}

func TestConcurrentSameFileAppends(t *testing.T) {
	_, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)
	fm := NewFileManager(false, lm)
	filename := "file_0"

	// Number of goroutines and writes per goroutine
	numGoroutines := 10
	writesPerGoroutine := 100

	// Use a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Each goroutine will write its ID followed by the iteration number
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			clientID := int32(id + 1)
			token := fmt.Sprintf("test-token-%d", id+1)

			// Acquire lock for this client
			success, token := lm.Acquire(clientID)
			if !success {
				t.Errorf("Goroutine %d failed to acquire lock", id)
				return
			}

			// Ensure lock is released in all cases
			defer lm.Release(clientID, token)

			for j := 0; j < writesPerGoroutine; j++ {
				content := fmt.Sprintf("G%d-%d\n", id, j)
				err := fm.AppendToFile(filename, []byte(content), clientID, token)
				if err != nil {
					t.Errorf("Goroutine %d failed to append: %v", id, err)
					return
				}
			}
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Verify the file contains the expected number of lines
	content, err := os.ReadFile(filepath.Join("data", filename))
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	lines := bytes.Count(content, []byte("\n"))
	expectedLines := numGoroutines * writesPerGoroutine
	if lines != expectedLines {
		t.Errorf("Expected %d lines, got %d", expectedLines, lines)
	}
}

func TestConcurrentMultiFileAppends(t *testing.T) {
	_, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)
	fm := NewFileManager(false, lm)

	// Number of goroutines and files
	numGoroutines := 20
	numFiles := 10
	writesPerGoroutine := 50

	// Track total writes per file
	writesPerFile := make(map[string]int)
	var writesPerFileMu sync.Mutex

	// Use a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Each goroutine will write to random files
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			clientID := int32(id + 1)
			token := fmt.Sprintf("test-token-%d", id+1)

			// Acquire lock for this client
			success, token := lm.Acquire(clientID)
			if !success {
				t.Errorf("Goroutine %d failed to acquire lock", id)
				return
			}

			// Ensure lock is released in all cases
			defer lm.Release(clientID, token)

			// Create a random number generator with a unique seed
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

			for j := 0; j < writesPerGoroutine; j++ {
				// Choose a random file
				fileNum := r.Intn(numFiles)
				filename := fmt.Sprintf("file_%d", fileNum)

				// Write to the file
				content := fmt.Sprintf("G%d-%d\n", id, j)
				err := fm.AppendToFile(filename, []byte(content), clientID, token)
				if err != nil {
					t.Errorf("Goroutine %d failed to append to %s: %v", id, filename, err)
					return
				}

				// Track the write
				writesPerFileMu.Lock()
				writesPerFile[filename]++
				writesPerFileMu.Unlock()
			}
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Verify each file has the expected number of writes
	totalWrites := 0
	for filename, count := range writesPerFile {
		content, err := os.ReadFile(filepath.Join("data", filename))
		if err != nil {
			t.Fatalf("Failed to read file %s: %v", filename, err)
		}

		lines := bytes.Count(content, []byte("\n"))
		if lines != count {
			t.Errorf("File %s: Expected %d lines, got %d", filename, count, lines)
		}

		totalWrites += count
	}

	expectedTotalWrites := numGoroutines * writesPerGoroutine
	if totalWrites != expectedTotalWrites {
		t.Errorf("Expected %d total writes, got %d", expectedTotalWrites, totalWrites)
	}
}

func TestStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	_, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)
	fm := NewFileManager(false, lm)

	// Configuration
	numGoroutines := 100
	numFiles := 20
	writesPerGoroutine := 50
	dataSize := 1024 // 1KB per write

	// Generate random data once to reuse
	randomData := make([]byte, dataSize)
	rand.Read(randomData)

	// Track metrics
	var totalWrites int64
	startTime := time.Now()

	// Use a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Start the goroutines
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			clientID := int32(id + 1)
			token := fmt.Sprintf("test-token-%d", id+1)

			// Acquire lock for this client
			success, token := lm.Acquire(clientID)
			if !success {
				t.Errorf("Goroutine %d failed to acquire lock", id)
				return
			}

			// Ensure lock is released in all cases
			defer lm.Release(clientID, token)

			// Create a random number generator with a unique seed
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

			for j := 0; j < writesPerGoroutine; j++ {
				// Choose a random file
				fileNum := r.Intn(numFiles)
				filename := fmt.Sprintf("file_%d", fileNum)

				// Write to the file
				err := fm.AppendToFile(filename, randomData, clientID, token)
				if err != nil {
					t.Errorf("Goroutine %d failed to append to %s: %v", id, filename, err)
					return
				}

				atomic.AddInt64(&totalWrites, 1)
			}
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Calculate metrics
	duration := time.Since(startTime)
	writesPerSecond := float64(totalWrites) / duration.Seconds()
	bytesWritten := int64(totalWrites) * int64(dataSize)
	mbWritten := float64(bytesWritten) / (1024 * 1024)
	mbPerSecond := mbWritten / duration.Seconds()

	t.Logf("Stress Test Results:")
	t.Logf("  Duration: %v", duration)
	t.Logf("  Total Writes: %d", totalWrites)
	t.Logf("  Writes/sec: %.2f", writesPerSecond)
	t.Logf("  Data Written: %.2f MB", mbWritten)
	t.Logf("  Throughput: %.2f MB/sec", mbPerSecond)

	// Verify all files exist and have content
	for i := 0; i < numFiles; i++ {
		filename := filepath.Join("data", fmt.Sprintf("file_%d", i))
		info, err := os.Stat(filename)
		if os.IsNotExist(err) {
			t.Errorf("File %s does not exist", filename)
			continue
		}
		if info.Size() == 0 {
			t.Errorf("File %s is empty", filename)
		}
	}
}

func TestResourceLeaks(t *testing.T) {
	_, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)
	fm := NewFileManager(false, lm)

	// Record initial number of goroutines
	initialGoroutines := runtime.NumGoroutine()

	// Perform a series of operations
	clientID := int32(1)

	// Acquire lock for the client and get the token
	success, token := lm.Acquire(clientID)
	if !success {
		t.Fatal("Failed to acquire lock")
	}

	// Ensure lock is released in all cases
	defer lm.Release(clientID, token)

	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			filename := fmt.Sprintf("file_%d", j)
			content := []byte(fmt.Sprintf("test content %d-%d", i, j))
			err := fm.AppendToFile(filename, content, clientID, token)
			if err != nil {
				t.Fatalf("Failed to append to file: %v", err)
			}
		}
	}

	// Clean up
	fm.Cleanup()

	// Check for goroutine leaks
	time.Sleep(100 * time.Millisecond) // Give goroutines time to exit
	finalGoroutines := runtime.NumGoroutine()
	if finalGoroutines > initialGoroutines+5 { // Allow for some background goroutines
		t.Errorf("Possible goroutine leak: started with %d, ended with %d",
			initialGoroutines, finalGoroutines)
	}

	// Check that all file handles were closed
	fm.mu.Lock()
	openFilesCount := len(fm.openFiles)
	fm.mu.Unlock()

	if openFilesCount > 0 {
		t.Errorf("File handle leak: %d files still open after cleanup", openFilesCount)
	}
}

func TestCreateFiles(t *testing.T) {
	_, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)
	fm := NewFileManager(false, lm)
	fm.CreateFiles()

	// Verify all 100 files were created
	for i := 0; i < 100; i++ {
		filename := filepath.Join("data", fmt.Sprintf("file_%d", i))
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			t.Errorf("File %s was not created", filename)
		}
	}
}

func TestCleanup(t *testing.T) {
	_, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)
	fm := NewFileManager(false, lm)

	// Create and open files
	clientID := int32(1)

	// Acquire lock for the client and get the token
	success, token := lm.Acquire(clientID)
	if !success {
		t.Fatal("Failed to acquire lock")
	}

	// Ensure lock is released in all cases
	defer lm.Release(clientID, token)

	for i := 0; i < 10; i++ {
		filename := fmt.Sprintf("file_%d", i)
		err := fm.AppendToFile(filename, []byte("test"), clientID, token)
		if err != nil {
			t.Fatalf("Failed to append to file: %v", err)
		}
	}

	// Verify files are in openFiles map
	fm.mu.Lock()
	initialOpenFiles := len(fm.openFiles)
	fm.mu.Unlock()

	if initialOpenFiles == 0 {
		t.Error("No files in openFiles map")
	}

	// Test cleanup
	fm.Cleanup()

	// Verify openFiles map is empty
	fm.mu.Lock()
	finalOpenFiles := len(fm.openFiles)
	fm.mu.Unlock()

	if finalOpenFiles != 0 {
		t.Errorf("openFiles map should be empty after cleanup, but has %d entries", finalOpenFiles)
	}
}

func TestErrorHandling(t *testing.T) {
	_, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)
	// We don't need the file manager for this test anymore
	// fm := NewFileManager(false, lm)

	// Test with read-only directory
	if runtime.GOOS != "windows" { // Skip on Windows as permissions work differently
		// Make data directory read-only
		err := os.Chmod("data", 0555)
		if err != nil {
			t.Fatalf("Failed to change directory permissions: %v", err)
		}

		clientID := int32(1)

		// Acquire lock for the client and get the token
		success, token := lm.Acquire(clientID)
		// We expect the lock acquisition to fail due to the read-only directory
		if success {
			t.Error("Expected lock acquisition to fail with read-only directory")
			// If lock acquisition succeeded, release it
			lm.Release(clientID, token)
		} else {
			// Since lock acquisition failed, we can't test AppendToFile
			t.Log("Lock acquisition failed as expected with read-only directory")
		}

		// Restore permissions
		os.Chmod("data", 0755)
	}
}

func BenchmarkAppendToFile(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "filemanager_bench")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a symbolic link to redirect "data" to our temp directory
	originalDataDir := "data"
	var originalDirExists bool
	if _, err := os.Stat(originalDataDir); err == nil {
		originalDirExists = true
		os.Rename(originalDataDir, originalDataDir+"_backup")
	}

	os.Mkdir(filepath.Join(tempDir, "data"), 0755)
	os.Symlink(filepath.Join(tempDir, "data"), "data")
	defer func() {
		os.Remove("data")
		if originalDirExists {
			os.Rename(originalDataDir+"_backup", originalDataDir)
		}
	}()

	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)
	fm := NewFileManager(false, lm)
	data := []byte("benchmark test data")
	clientID := int32(1)

	// Acquire lock for the client and get the token
	success, token := lm.Acquire(clientID)
	if !success {
		b.Fatal("Failed to acquire lock")
	}

	// Ensure lock is released in all cases
	defer lm.Release(clientID, token)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fileNum := i % 100
		filename := fmt.Sprintf("file_%d", fileNum)
		err := fm.AppendToFile(filename, data, clientID, token)
		if err != nil {
			b.Fatalf("Failed to append to file: %v", err)
		}
	}
	b.StopTimer()

	fm.Cleanup()
}

func BenchmarkConcurrentAppends(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "filemanager_bench")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a symbolic link to redirect "data" to our temp directory
	originalDataDir := "data"
	var originalDirExists bool
	if _, err := os.Stat(originalDataDir); err == nil {
		originalDirExists = true
		os.Rename(originalDataDir, originalDataDir+"_backup")
	}

	os.Mkdir(filepath.Join(tempDir, "data"), 0755)
	os.Symlink(filepath.Join(tempDir, "data"), "data")
	defer func() {
		os.Remove("data")
		if originalDirExists {
			os.Rename(originalDataDir+"_backup", originalDataDir)
		}
	}()

	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)
	fm := NewFileManager(false, lm)
	data := []byte("benchmark test data")

	// Number of concurrent goroutines
	numGoroutines := runtime.GOMAXPROCS(0) * 2

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for g := 0; g < numGoroutines; g++ {
			go func(id int) {
				defer wg.Done()
				clientID := int32(id + 1)

				// Acquire lock for this client and get the token
				success, token := lm.Acquire(clientID)
				if !success {
					b.Errorf("Goroutine %d failed to acquire lock", id)
					return
				}

				// Ensure lock is released in all cases
				defer lm.Release(clientID, token)

				fileNum := id % 100
				filename := fmt.Sprintf("file_%d", fileNum)
				err := fm.AppendToFile(filename, data, clientID, token)
				if err != nil {
					b.Errorf("Failed to append to file: %v", err)
				}
			}(g)
		}

		wg.Wait()
	}

	b.StopTimer()
	fm.Cleanup()
}

// TestFileManager_AppendToFile tests the AppendToFile method
func TestFileManager_AppendToFile(t *testing.T) {
	_, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)
	fm := NewFileManager(false, lm)

	// Test case 1: Append to a new file
	t.Run("Append to new file", func(t *testing.T) {
		filename := "file_0"
		content := []byte("Hello, World!")
		clientID := int32(1)

		// Acquire lock for the client and get the token
		success, token := lm.Acquire(clientID)
		if !success {
			t.Fatal("Failed to acquire lock")
		}

		// Ensure lock is released in all cases
		defer lm.Release(clientID, token)

		err := fm.AppendToFile(filename, content, clientID, token)
		if err != nil {
			t.Errorf("AppendToFile failed: %v", err)
		}

		// Verify file contents
		fileContent, err := os.ReadFile(filepath.Join("data", filename))
		if err != nil {
			t.Errorf("Failed to read file: %v", err)
		}
		if string(fileContent) != string(content) {
			t.Errorf("File content mismatch. Expected: %s, Got: %s", content, fileContent)
		}
	})

	// Test case 2: Append to existing file
	t.Run("Append to existing file", func(t *testing.T) {
		filename := "file_1"
		content1 := []byte("First line\n")
		content2 := []byte("Second line\n")
		clientID := int32(2)

		// Acquire lock for the client and get the token
		success, token := lm.Acquire(clientID)
		if !success {
			t.Fatal("Failed to acquire lock")
		}

		// Ensure lock is released in all cases
		defer lm.Release(clientID, token)

		// First append
		err := fm.AppendToFile(filename, content1, clientID, token)
		if err != nil {
			t.Errorf("First AppendToFile failed: %v", err)
		}

		// Second append
		err = fm.AppendToFile(filename, content2, clientID, token)
		if err != nil {
			t.Errorf("Second AppendToFile failed: %v", err)
		}

		// Verify file contents
		fileContent, err := os.ReadFile(filepath.Join("data", filename))
		if err != nil {
			t.Errorf("Failed to read file: %v", err)
		}
		expectedContent := string(content1) + string(content2)
		if string(fileContent) != expectedContent {
			t.Errorf("File content mismatch. Expected: %s, Got: %s", expectedContent, fileContent)
		}
	})

	// Test case 3: Append without lock
	t.Run("Append without lock", func(t *testing.T) {
		filename := "file_2"
		content := []byte("Should fail")
		clientID := int32(3)
		token := "invalid-token"

		err := fm.AppendToFile(filename, content, clientID, token)
		if err == nil {
			t.Error("AppendToFile should fail without lock")
		}
	})

	// Test case 4: Append with invalid token
	t.Run("Append with invalid token", func(t *testing.T) {
		filename := "file_3"
		content := []byte("Should fail")
		clientID := int32(4)

		// Acquire lock for the client and get the token
		success, token := lm.Acquire(clientID)
		if !success {
			t.Fatal("Failed to acquire lock")
		}

		// Ensure lock is released in all cases
		defer lm.Release(clientID, token)

		// Try to append with invalid token
		invalidToken := "invalid-token"
		err := fm.AppendToFile(filename, content, clientID, invalidToken)
		if err == nil {
			t.Error("AppendToFile should fail with invalid token")
		}
	})
}

// TestFileManager_CreateFiles tests the CreateFiles method
func TestFileManager_CreateFiles(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "file_manager_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)
	fm := NewFileManager(false, lm)

	// Create files
	fm.CreateFiles()

	// Verify that all files exist
	for i := 0; i < 100; i++ {
		filename := fmt.Sprintf("file_%d", i)
		_, err := os.Stat(filepath.Join("data", filename))
		if err != nil {
			t.Errorf("File %s should exist: %v", filename, err)
		}
	}
}

// TestFileManager_Cleanup tests the Cleanup method
func TestFileManager_Cleanup(t *testing.T) {
	_, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)
	fm := NewFileManager(false, lm)

	// Create and open files
	clientID := int32(1)

	// Acquire lock for the client and get the token
	success, token := lm.Acquire(clientID)
	if !success {
		t.Fatal("Failed to acquire lock")
	}

	// Ensure lock is released in all cases
	defer lm.Release(clientID, token)

	// Open some files by appending to them
	for i := 0; i < 10; i++ {
		filename := fmt.Sprintf("file_%d", i)
		err := fm.AppendToFile(filename, []byte("test"), clientID, token)
		if err != nil {
			t.Fatalf("Failed to append to file: %v", err)
		}
	}

	// Verify files are in openFiles map
	fm.mu.Lock()
	initialOpenFiles := len(fm.openFiles)
	fm.mu.Unlock()

	if initialOpenFiles == 0 {
		t.Error("No files in openFiles map")
	}

	// Test cleanup
	fm.Cleanup()

	// Verify openFiles map is empty
	fm.mu.Lock()
	finalOpenFiles := len(fm.openFiles)
	fm.mu.Unlock()

	if finalOpenFiles != 0 {
		t.Errorf("openFiles map should be empty after cleanup, but has %d entries", finalOpenFiles)
	}
}

func TestProcessedRequestsIdempotency(t *testing.T) {
	_, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Clean up data directory to ensure a fresh start
	os.RemoveAll("data")
	os.MkdirAll("data", 0755)

	// Create a unique directory for WAL logs
	uniqueID := uuid.New().String()
	logDir := filepath.Join(os.TempDir(), fmt.Sprintf("idempotency_test_logs_%s", uniqueID))
	os.MkdirAll(logDir, 0755)
	defer os.RemoveAll(logDir)

	// Set environment variable for WAL logs
	oldWalDir := os.Getenv("WAL_LOG_DIR")
	os.Setenv("WAL_LOG_DIR", logDir)
	defer os.Setenv("WAL_LOG_DIR", oldWalDir)

	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)

	// Create a file manager with WAL enabled
	fm := NewFileManagerWithWAL(true, true, lm)

	// Clear any processed requests from previous tests
	if err := fm.ClearProcessedRequests(); err != nil {
		t.Fatalf("Failed to clear processed requests: %v", err)
	}

	// Test valid filename and content
	testContent := []byte("test content")
	clientID := int32(1)
	requestID := "test-request-1"

	// Acquire lock for the client and get the token
	success, token := lm.Acquire(clientID)
	if !success {
		t.Fatal("Failed to acquire lock")
	}
	defer lm.Release(clientID, token)

	// First append with request ID
	err := fm.AppendToFileWithRequestID("file_0", testContent, requestID, clientID, token)
	if err != nil {
		t.Errorf("AppendToFileWithRequestID failed on first attempt: %v", err)
	}

	// Verify content was written
	content, err := os.ReadFile(filepath.Join("data", "file_0"))
	if err != nil {
		t.Errorf("Failed to read file: %v", err)
	}
	if string(content) != string(testContent) {
		t.Errorf("File content mismatch. Got %s, want %s", content, testContent)
	}

	// Try to append again with the same request ID (should be idempotent)
	err = fm.AppendToFileWithRequestID("file_0", testContent, requestID, clientID, token)
	if err != nil {
		t.Errorf("AppendToFileWithRequestID failed on second attempt: %v", err)
	}

	// Verify content was not duplicated
	content, err = os.ReadFile(filepath.Join("data", "file_0"))
	if err != nil {
		t.Errorf("Failed to read file after second append: %v", err)
	}
	if string(content) != string(testContent) {
		t.Errorf("Content was duplicated or modified. Got %s, want %s", content, testContent)
	}

	// Try with a different request ID
	requestID2 := "test-request-2"
	err = fm.AppendToFileWithRequestID("file_0", testContent, requestID2, clientID, token)
	if err != nil {
		t.Errorf("AppendToFileWithRequestID failed with new request ID: %v", err)
	}

	// Verify content was appended
	content, err = os.ReadFile(filepath.Join("data", "file_0"))
	if err != nil {
		t.Errorf("Failed to read file after third append: %v", err)
	}
	expectedContent := string(testContent) + string(testContent)
	if string(content) != expectedContent {
		t.Errorf("Content not appended correctly. Got %s, want %s", content, expectedContent)
	}

	// Verify processed requests log contains both request IDs
	processedRequestsPath := filepath.Join("data", "processed_requests.log")
	logContent, err := os.ReadFile(processedRequestsPath)
	if err != nil {
		t.Errorf("Failed to read processed requests log: %v", err)
	}

	logStr := string(logContent)
	if !strings.Contains(logStr, requestID) || !strings.Contains(logStr, requestID2) {
		t.Errorf("Processed requests log missing entries. Content: %s", logStr)
	}

	// Manually check the in-memory map
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	if !fm.processedRequests[requestID] || !fm.processedRequests[requestID2] {
		t.Errorf("In-memory processed requests map missing entries: %v", fm.processedRequests)
	}
}

func TestRecoveryWithProcessedRequests(t *testing.T) {
	_, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Clean up data directory to ensure a fresh start
	os.RemoveAll("data")
	os.MkdirAll("data", 0755)

	// Create a unique directory for WAL logs
	uniqueID := uuid.New().String()
	logDir := filepath.Join(os.TempDir(), fmt.Sprintf("recovery_test_logs_%s", uniqueID))
	os.MkdirAll(logDir, 0755)
	defer os.RemoveAll(logDir)

	// Set environment variable for WAL logs
	oldWalDir := os.Getenv("WAL_LOG_DIR")
	os.Setenv("WAL_LOG_DIR", logDir)
	defer os.Setenv("WAL_LOG_DIR", oldWalDir)

	// Create a lock manager for testing
	lm := lock_manager.NewLockManagerWithLeaseDuration(nil, 30*time.Second)

	// Setup: Create a file manager, perform operations, then simulate a crash
	{
		fm := NewFileManagerWithWAL(true, true, lm)

		// Clear any processed requests from previous tests
		if err := fm.ClearProcessedRequests(); err != nil {
			t.Fatalf("Failed to clear processed requests: %v", err)
		}

		// Acquire lock
		clientID := int32(1)
		success, token := lm.Acquire(clientID)
		if !success {
			t.Fatal("Failed to acquire lock")
		}

		// Test data
		content1 := []byte("content 1")
		requestID1 := "test-recovery-1"

		// First operation (should complete normally)
		err := fm.AppendToFileWithRequestID("file_0", content1, requestID1, clientID, token)
		if err != nil {
			t.Errorf("First append failed: %v", err)
		}

		// Release lock
		lm.Release(clientID, token)

		// Simulate a crash by not calling Cleanup() - just let it go out of scope
	}

	// Recovery: Create a new file manager that will recover from the WAL
	fm2 := NewFileManagerWithWAL(true, true, lm)

	// Verify the recovery was successful
	if !fm2.IsRecoveryComplete() {
		t.Error("Recovery not marked as complete")
	}

	if err := fm2.GetRecoveryError(); err != nil {
		t.Errorf("Recovery reported an error: %v", err)
	}

	// Check file content after recovery
	content, err := os.ReadFile(filepath.Join("data", "file_0"))
	if err != nil {
		t.Errorf("Failed to read file after recovery: %v", err)
	}
	expectedContent := "content 1"
	if string(content) != expectedContent {
		t.Errorf("File content incorrect after recovery. Got %s, want %s", string(content), expectedContent)
	}

	// Attempt duplicate append (should be idempotent due to processed requests log)
	clientID := int32(1)
	success, token := lm.Acquire(clientID)
	if !success {
		t.Fatal("Failed to acquire lock after recovery")
	}
	defer lm.Release(clientID, token)

	err = fm2.AppendToFileWithRequestID("file_0", []byte("content 1"), "test-recovery-1", clientID, token)
	if err != nil {
		t.Errorf("Idempotent append after recovery failed: %v", err)
	}

	// Verify content was not duplicated
	content, err = os.ReadFile(filepath.Join("data", "file_0"))
	if err != nil {
		t.Errorf("Failed to read file after idempotent append: %v", err)
	}
	if string(content) != expectedContent {
		t.Errorf("Content was duplicated after recovery. Got %s, want %s", string(content), expectedContent)
	}
}

func TestRollbackOnLeaseExpiry(t *testing.T) {
	// Setup
	tempDir, err := os.MkdirTemp("", "filemanager_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Change to temp directory
	originalWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}
	defer os.Chdir(originalWd)
	os.Chdir(tempDir)

	// Create data directory
	if err := os.Mkdir("data", 0755); err != nil {
		t.Fatalf("Failed to create data directory: %v", err)
	}

	// Create test file
	testFile := filepath.Join("data", "file_0")
	if err := os.WriteFile(testFile, []byte(""), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Initialize lock manager with a long lease duration to avoid automatic expiry during test
	logger := log.New(os.Stdout, "[Test] ", log.LstdFlags)
	lm := lock_manager.NewLockManagerWithLeaseDuration(logger, 30*time.Second)

	// Initialize file manager
	fm := NewFileManager(true, lm)

	// Channel to track when rollback operations are completed
	rollbackCh := make(chan string, 2)

	// Register for lease expiry with notification
	fm.RegisterForLeaseExpiryWithNotification(rollbackCh)

	// Test scenario
	// 1. Client 1 acquires lock
	clientID := int32(1)
	acquired, token := lm.Acquire(clientID)
	if !acquired {
		t.Fatalf("Failed to acquire lock")
	}

	// 2. Client 1 appends 'A'
	if err := fm.AppendToFile("file_0", []byte("A"), clientID, token); err != nil {
		t.Fatalf("Failed to append to file: %v", err)
	}

	// Verify file contains 'A'
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	if string(content) != "A" {
		t.Fatalf("Expected file to contain 'A', got %q", string(content))
	}

	// 3. Manually force lease expiry
	lm.ForceExpireLease()

	// Wait for rollback notification
	select {
	case receivedToken := <-rollbackCh:
		if receivedToken != token {
			t.Errorf("Received unexpected token in rollback: %s, expected: %s", receivedToken, token)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Timed out waiting for rollback completion")
	}

	// 4. Verify file was rolled back (content should be empty again)
	content, err = os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file after rollback: %v", err)
	}
	if string(content) != "" {
		t.Fatalf("Expected file to be empty after rollback, got %q", string(content))
	}

	// 5. Client 2 acquires lock
	clientID2 := int32(2)
	acquired, token2 := lm.Acquire(clientID2)
	if !acquired {
		t.Fatalf("Client 2 failed to acquire lock")
	}

	// 6. Client 2 appends 'B' twice
	if err := fm.AppendToFile("file_0", []byte("B"), clientID2, token2); err != nil {
		t.Fatalf("Failed to append 'B' to file: %v", err)
	}
	if err := fm.AppendToFile("file_0", []byte("B"), clientID2, token2); err != nil {
		t.Fatalf("Failed to append second 'B' to file: %v", err)
	}

	// Verify file contains 'BB'
	content, err = os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	if string(content) != "BB" {
		t.Fatalf("Expected file to contain 'BB', got %q", string(content))
	}

	// 7. Client 1 attempts to continue with old token (should fail)
	err = fm.AppendToFile("file_0", []byte("A"), clientID, token)
	if err == nil {
		t.Fatalf("Expected Client 1's append to fail with old token, but it succeeded")
	}

	// For more reliable testing, manually release Client 2's lock to avoid waiting for expiry
	if !lm.Release(clientID2, token2) {
		t.Fatalf("Failed to release Client 2's lock")
	}

	// 8. Client 1 re-acquires lock
	acquired, token3 := lm.Acquire(clientID)
	if !acquired {
		t.Fatalf("Client 1 failed to re-acquire lock")
	}

	// 9. Client 1 appends 'A' twice with new token
	if err := fm.AppendToFile("file_0", []byte("A"), clientID, token3); err != nil {
		t.Fatalf("Failed to append 'A' to file with new token: %v", err)
	}
	if err := fm.AppendToFile("file_0", []byte("A"), clientID, token3); err != nil {
		t.Fatalf("Failed to append second 'A' to file with new token: %v", err)
	}

	// 10. Verify file content is 'BBAA'
	content, err = os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file at end of test: %v", err)
	}
	if string(content) != "BBAA" {
		t.Fatalf("Expected final file content to be 'BBAA', got %q", string(content))
	}

	// 11. Force expire Client 1's lease
	lm.ForceExpireLease()

	// Wait for rollback notification
	select {
	case receivedToken := <-rollbackCh:
		if receivedToken != token3 {
			t.Errorf("Received unexpected token in rollback: %s, expected: %s", receivedToken, token3)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Timed out waiting for second rollback completion")
	}

	// 12. Verify only Client 1's operations were rolled back (file should still have "BB")
	content, err = os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file after final rollback: %v", err)
	}
	if string(content) != "BB" {
		t.Fatalf("Expected file to contain 'BB' after final rollback, got %q", string(content))
	}
}
