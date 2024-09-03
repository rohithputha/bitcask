package cask

import (
	"bitcask/data"
	"time"
)

func getPipeline(key interface{}) interface{} {
	ende := data.NewEnde()
	_, _, value := ende.DecodeData((<-getBlockFromFile(getBlockFromMem(key))).BlockBytes)
	return value
}

func putPipeline(key interface{}, value interface{}) {

	ende := data.NewEnde()
	timestamp := time.Now().Unix()
	bytes := ende.EncodeData(timestamp, key, value)

	bytesChan := make(chan []byte)
	defer close(bytesChan)
	fileOpResultChan := putBlockOnFile(bytesChan)
	KeyBlockAddrChan := func(f <-chan fileOpResult) <-chan KeyBlockAddr {
		blockAddrChan := make(chan KeyBlockAddr)
		go func() {
			defer close(blockAddrChan)
			fileOpResult := <-f
			blockAddrChan <- KeyBlockAddr{key: getByteString(key), block: &BlockAddr{Fid: fileOpResult.Fid, Offset: fileOpResult.Offset, Size: len(bytes), Timestamp: timestamp}}
		}()
		return blockAddrChan
	}(fileOpResultChan)
	putDone := putBlockOnMem(KeyBlockAddrChan)
	for {
		select {
		case bytesChan <- bytes:
			continue
		case <-putDone:
			return
		}
	}
}

func putPipelineWithFile(file *DbFile, key interface{}, value interface{}) {
	ende := data.NewEnde()
	timestamp := time.Now().Unix()
	bytes := ende.EncodeData(timestamp, key, value)

	bytesChan := make(chan bytesWithFile)
	defer close(bytesChan)
	fileOpResultChan := putBlockOnFileWithFile(bytesChan)
	KeyBlockAddrChan := func(f <-chan fileOpResult) <-chan KeyBlockAddr {
		blockAddrChan := make(chan KeyBlockAddr)
		go func() {
			defer close(blockAddrChan)
			fileOpResult := <-f
			blockAddrChan <- KeyBlockAddr{key: getByteString(key), block: &BlockAddr{Fid: fileOpResult.Fid, Offset: fileOpResult.Offset, Size: len(bytes), Timestamp: timestamp}}
		}()
		return blockAddrChan
	}(fileOpResultChan)
	putDone := putBlockOnMem(KeyBlockAddrChan)
	for {
		select {
		case bytesChan <- bytesWithFile{file: file, bytes: bytes}:
		case <-putDone:
			return
		}
	}
}
