package cask

import (
	"sync"
)

type DiskMgr struct {
	// DiskMgr is a struct that manages the disk operations
	fm *FileMgr
}

var diskMgrInst *DiskMgr
var diskMgrOnce sync.Once

// ######################################## New concurrency design ########################################
type bytesWithFile struct {
	bytes []byte
	file  *DbFile
}

func getBlockFromFile(blockAddrChan <-chan BlockAddr) <-chan fileOpResult {
	fileOpResultsChan := make(chan fileOpResult)
	go func() {
		defer close(fileOpResultsChan)
		block := <-blockAddrChan
		activeFiles := getActiveFile().DbFiles
		var activeFile *DbFile
		if len(activeFiles) == 0 {
			newFile := putNewFile().DbFiles[0]
			newFile.makeFileActive()
			activeFile = newFile
		} else {
			activeFile = activeFiles[0]
		}
		fileOpResultsChan <- <-activeFile.getBlock(block.Offset, block.Size)
	}()
	return fileOpResultsChan
}

func putBlockOnFile(blockBytesChan <-chan []byte) <-chan fileOpResult {
	fileOpResultsChan := make(chan fileOpResult)
	go func() {
		defer close(fileOpResultsChan)
		activeFiles := getActiveFile().DbFiles
		var activeFile *DbFile
		if len(activeFiles) == 0 {
			newFile := putNewFile().DbFiles[0]
			newFile.makeFileActive()
			activeFile = newFile
		} else {
			activeFile = activeFiles[0]
		}

		blockBytes := <-blockBytesChan
		fileOpResultsChan <- <-activeFile.appendBlock(blockBytes)
	}()
	return fileOpResultsChan
}

func putBlockOnFileWithFile(bytesWithFileChan <-chan bytesWithFile) <-chan fileOpResult {
	fileOpResultChan := make(chan fileOpResult)
	go func() {
		defer close(fileOpResultChan)
		bf := <-bytesWithFileChan
		fileOpResultChan <- <-bf.file.appendBlock(bf.bytes)
	}()
	return fileOpResultChan
}
