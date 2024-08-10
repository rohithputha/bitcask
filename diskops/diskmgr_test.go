 package diskops

// import (
// 	"bytes"
// 	"os"
// 	"testing"
// )

// func TestAppendBlock(t *testing.T) {
// 	dm := &DiskMgr{
// 		fm: newFileMgr(),
// 	}
	
// 	fileName := t.TempDir() + "testAppendBlock.txt"
// 	os.Create(fileName)
// 	dm.fm.addFile(fileName)
// 	block := []byte("This is a test block")

// 	// Call the AppendBlock method
// 	dm.AppendBlock(fileName, block)

// 	// Verify that the block was appended correctly
// 	file, err := os.Open(fileName)
// 	if err != nil {
// 		t.Fatalf("Error opening file: %v", err)
// 	}
// 	defer file.Close()

// 	stat, err := file.Stat()
// 	if err != nil {
// 		t.Fatalf("Error getting file info: %v", err)
// 	}

// 	fileSize := stat.Size()
// 	expectedSize := int64(len(block))

// 	if fileSize != expectedSize {
// 		t.Errorf("Expected file size %d, got %d", expectedSize, fileSize)
// 	}

// 	readBlock := make([]byte, len(block))
// 	_, err = file.ReadAt(readBlock, 0)
// 	if err != nil {
// 		t.Fatalf("Error reading block from file: %v", err)
// 	}

// 	if !bytes.Equal(readBlock, block) {
// 		t.Errorf("Expected block %v, got %v", block, readBlock)
// 	}
// }
