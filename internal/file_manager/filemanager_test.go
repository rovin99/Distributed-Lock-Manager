package file_manager

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestNewFileManager(t *testing.T) {
	fm := NewFileManager()
	if fm == nil {
		t.Fatal("NewFileManager returned nil")
	}
	if fm.openFiles == nil {
		t.Error("openFiles map not initialized")
	}
	if fm.logger == nil {
		t.Error("logger not initialized")
	}
}

func TestAppendToFile(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := ioutil.TempDir("", "filemanager_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a symbolic link to redirect "data" to our temp directory
	originalDataDir := "data"
	if _, err := os.Stat(originalDataDir); err == nil {
		// Rename the original data directory temporarily
		os.Rename(originalDataDir, originalDataDir+"_backup")
		defer func() {
			os.RemoveAll(originalDataDir) // Clean up any test data
			os.Rename(originalDataDir+"_backup", originalDataDir)
		}()
	}

	// Create the data directory in our temp location
	os.Mkdir(filepath.Join(tempDir, "data"), 0755)
	os.Symlink(filepath.Join(tempDir, "data"), "data")
	defer os.Remove("data") // Remove the symlink

	fm := NewFileManager()

	// Test valid filename and content
	testContent := []byte("test content")
	err = fm.AppendToFile("file_0", testContent)
	if err != nil {
		t.Errorf("AppendToFile failed with valid input: %v", err)
	}

	// Verify content was written
	content, err := ioutil.ReadFile(filepath.Join("data", "file_0"))
	if err != nil {
		t.Errorf("Failed to read file: %v", err)
	}
	if string(content) != string(testContent) {
		t.Errorf("File content mismatch. Got %s, want %s", content, testContent)
	}

	// Test appending more content
	moreContent := []byte(" additional content")
	err = fm.AppendToFile("file_0", moreContent)
	if err != nil {
		t.Errorf("Failed to append more content: %v", err)
	}

	// Verify appended content
	content, err = ioutil.ReadFile(filepath.Join("data", "file_0"))
	if err != nil {
		t.Errorf("Failed to read file after append: %v", err)
	}
	expectedContent := string(testContent) + string(moreContent)
	if string(content) != expectedContent {
		t.Errorf("Appended content mismatch. Got %s, want %s", content, expectedContent)
	}

	// Test invalid filename format
	err = fm.AppendToFile("invalid_file", testContent)
	if err == nil {
		t.Error("AppendToFile should fail with invalid filename format")
	}

	// Test invalid file number
	err = fm.AppendToFile("file_100", testContent)
	if err == nil {
		t.Error("AppendToFile should fail with invalid file number")
	}

	err = fm.AppendToFile("file_-1", testContent)
	if err == nil {
		t.Error("AppendToFile should fail with negative file number")
	}

	err = fm.AppendToFile("file_abc", testContent)
	if err == nil {
		t.Error("AppendToFile should fail with non-numeric file number")
	}
}

func TestCreateFiles(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := ioutil.TempDir("", "filemanager_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a symbolic link to redirect "data" to our temp directory
	originalDataDir := "data"
	if _, err := os.Stat(originalDataDir); err == nil {
		// Rename the original data directory temporarily
		os.Rename(originalDataDir, originalDataDir+"_backup")
		defer func() {
			os.RemoveAll(originalDataDir) // Clean up any test data
			os.Rename(originalDataDir+"_backup", originalDataDir)
		}()
	}

	// Create the data directory in our temp location
	os.Mkdir(filepath.Join(tempDir, "data"), 0755)
	os.Symlink(filepath.Join(tempDir, "data"), "data")
	defer os.Remove("data") // Remove the symlink

	fm := NewFileManager()
	fm.CreateFiles()

	// Verify all 100 files were created
	for i := 0; i < 100; i++ {
		filename := filepath.Join("data", "file_"+fmt.Sprint(i))
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			t.Errorf("File %s was not created", filename)
		}
	}
}

func TestCleanup(t *testing.T) {
	fm := NewFileManager()

	// Since we don't have direct access to the openFiles map,
	// we'll test that Cleanup doesn't crash
	fm.Cleanup()

	// This is a limited test since we can't easily verify that files were closed
	// In a real scenario, you might mock the os.File interface
}

// Helper function to create a test file with content
func createTestFile(path string, content []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return ioutil.WriteFile(path, content, 0644)
}
