package cask

import (
	"bitcask/data"
	"fmt"
	"os"
	"time"
)

func manageDb(sizeIncrementChan <-chan int64, done <-chan interface{}) {
	var activeFileSize int64
	if a := getActiveFile().DbFiles; len(a) > 0 {
		activeFileSize = a[0].getSize()
	}

	fileSizeSignalChan := fileSizeSignal(activeFileSize, sizeIncrementChan)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-fileSizeSignalChan:
				if a := getActiveFile().DbFiles; len(a) > 0 {
					activeFile := a[0]
					fmt.Println("making file complete ", activeFile.id)
					completeFile(activeFile.id)
				}

				completeFiles := getCompleteFiles().DbFiles
				if len(completeFiles) > 5 {
					go mergeFiles(completeFiles)
				}
			}
		}
	}()
}

func mergeFiles(completeFiles []*DbFile) {
	ende := data.NewEnde()
	temp := make(map[string]*BlockAddr)
	tempVal := make(map[string]interface{})
	tdbFile := putNewFile().DbFiles[0]
	fmt.Println("merge file id ", tdbFile.id)
	cFiles := make([]*DbFile, 0)
	for _, file := range completeFiles {
		iter := file.iterator()
		fmt.Println("file for merge " + file.id)
		for opRes := range iter {
			fmt.Println("here for a merge")
			bytes := opRes.BlockBytes
			if len(bytes) < 18 {
				fmt.Println("bytes length is less than 18")
				continue
			}
			timestamp, key, value := ende.DecodeData(bytes)
			stringKey := getByteString(key)
			fmt.Println("background merge >>> key: ", stringKey, "value: ", value)
			if timestamp == -1 {
				continue
			}
			presentKeyVal := temp[stringKey]
			if presentKeyVal == nil || timestamp >= presentKeyVal.Timestamp {
				temp[stringKey] = &BlockAddr{Fid: tdbFile.id, Offset: -1, Size: -1, Timestamp: timestamp}
				tempVal[stringKey] = value
			}
		}
		cFiles = append(cFiles, file)
	}

	for key, value := range tempVal {
		fmt.Println("background merge key: ", key, "value: ", value)
		putPipelineWithFile(tdbFile, key, value)
	}
	time.Sleep(2 * time.Second)
	//completeFile(tdbFile.id)
	for key, val := range keyDir {
		fmt.Println("keydir key: ", key, " value: ", val.Fid)
	}

	os.Exit(2)
	//for _, file := range cFiles {
	//	deleteFile(file.id) // handle errors here
	//}
}

func fileSizeSignal(initSize int64, sizeIncrement <-chan int64) <-chan interface{} {
	fsignal := make(chan interface{})
	go func() {
		defer close(fsignal)
		for i := range sizeIncrement {
			fmt.Println("size increment")
			fmt.Println(i + initSize)
			if i < 0 {
				initSize = 0
			} else {
				initSize += i
			}
			if initSize >= 500 {
				initSize = 0
				fsignal <- struct{}{}
			}
		}
	}()
	return fsignal
}
