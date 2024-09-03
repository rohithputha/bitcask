package cask

import (
	"bitcask/data"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
)

const filePath = "/Users/rohith/Desktop/Desktop - Rohith’s MacBook Air/incubation/bitcask/dbfiles"

var initOnce sync.Once
var fileMgrInst *FileMgr

type DbFileIterator interface {
	Next() ([]byte, int)
	IsNext() bool
}

type DbFileIteratorStr struct {
	file          *DbFile
	presentOffset int
}

type DbFile struct {
	id         string
	file       *os.File
	fileLock   *sync.Mutex
	isActive   bool
	isComplete bool
	location   string

	// experimental
	fileChangeOpChan       chan fileChangeIO
	fileChangeResultOpChan chan fileOpResult
}

type FileMgr struct {
	dbFiles map[string]*DbFile
}

// ----------------------------------------------- New Version ---------------------------
var fileMgrOnce sync.Once

type dbFileMap map[string]*DbFile
type operation struct {
	op     string
	id     string
	dbfile *DbFile
}
type DfmResult struct {
	DbFiles []*DbFile
	Err     error
}

var opChannel chan operation
var commonOpResultChan <-chan DfmResult
var fileSizeSignalChan chan int64

func initDbFileMap(done <-chan interface{}, files []os.DirEntry) *dbFileMap {
	dfm := make(dbFileMap)
	dfm.initOperation(done, dfm.getMaxFileId(files))
	return &dfm
}

func (dfm *dbFileMap) getMaxFileId(files []os.DirEntry) int64 {
	var maxId int64
	for _, file := range files {
		if !file.IsDir() {
			fileName := file.Name()
			id := strings.Split(fileName, ".")[0]
			id = strings.Split(id, "_")[0]
			if s, err := strconv.ParseInt(id, 10, 64); err == nil {
				if s > maxId {
					maxId = s
				}
			}
		}
	}
	return maxId

}

func (dfm *dbFileMap) initOperation(done <-chan interface{}, maxFileId int64) {
	opChannel = make(chan operation)
	result := make(chan DfmResult)
	fileSizeSignalChan = make(chan int64)
	commonOpResultChan = result

	nextFileId := make(chan string)
	go func() {
		maxFileId += 1
		for {
			select {
			case <-done:
				return
			case nextFileId <- strconv.FormatInt(maxFileId, 10):
				maxFileId += 1
			}
		}
	}()

	performOperation := func(oper operation, result chan<- DfmResult) {
		switch oper.op {
		case "put":
			(*dfm)[oper.id] = oper.dbfile
			oper.dbfile.startFileOperation()
			result <- DfmResult{Err: nil}
		case "putnew":
			select {
			case id := <-nextFileId:
				fileName := id + ".db"
				fileLocation := filePath
				file, err := os.OpenFile(fileLocation+"/"+fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
				if err != nil {
					log.Fatalf("Error opening file %s", fileLocation)
				}
				(*dfm)[id] = &DbFile{
					id:                     id,
					file:                   file,
					fileLock:               &sync.Mutex{},
					isActive:               false,
					isComplete:             false,
					location:               fileLocation,
					fileChangeResultOpChan: make(chan fileOpResult),
					fileChangeOpChan:       make(chan fileChangeIO),
				}
				(*dfm)[id].startFileOperation()
				result <- DfmResult{DbFiles: []*DbFile{(*dfm)[id]}, Err: nil}
			case <-done:
				return
			}

		case "delete":
			//close((*dfm)[oper.id].fileChangeOpChan)
			//close((*dfm)[oper.id].fileChangeResultOpChan)
			err := os.Remove((*dfm)[oper.id].location + "/" + (*dfm)[oper.id].id + "_c.db")
			if err != nil {
				result <- DfmResult{Err: errors.New("DbfileMapError::Error deleting file: " + err.Error())}
				return
			}
			delete(*dfm, oper.id)
			result <- DfmResult{Err: nil}
		case "get":
			if dbfile, ok := (*dfm)[oper.id]; ok {
				result <- DfmResult{DbFiles: []*DbFile{dbfile}, Err: nil}
			} else {
				result <- DfmResult{DbFiles: nil, Err: errors.New("DbfileMapError::Could not get file with id:" + oper.id)}
			}
		case "getall":
			filesArr := make([]*DbFile, 0)
			for _, dbfile := range *dfm {
				filesArr = append(filesArr, dbfile)
			}
			result <- DfmResult{DbFiles: filesArr, Err: nil}
		case "completefile":
			if dbfile, ok := (*dfm)[oper.id]; ok {
				dbfile.makeFileComplete()
				result <- DfmResult{DbFiles: []*DbFile{dbfile}, Err: nil}
			} else {
				result <- DfmResult{DbFiles: nil, Err: errors.New("DbfileMapError::Could not mark file complete with id:" + oper.id)}
			}
		}

	}
	go func() {
		defer close(opChannel)
		defer close(result)
		for {
			select {
			case oper := <-opChannel:
				performOperation(oper, result)
			case <-done:
				return
			}
		}
	}()
}

func putFile(id string, dbFile *DbFile) DfmResult {
	opChannel <- operation{op: "put", id: id, dbfile: dbFile}
	result := <-commonOpResultChan
	return result
}
func getFile(id string) DfmResult {
	opChannel <- operation{op: "get", id: id}
	return <-commonOpResultChan
}
func deleteFile(id string) DfmResult {
	opChannel <- operation{op: "delete", id: id}
	return <-commonOpResultChan
}
func getAllFiles() DfmResult {
	opChannel <- operation{op: "getall"}
	return <-commonOpResultChan
}
func getCompleteFiles() DfmResult {
	res := getAllFiles()
	filesArr := make([]*DbFile, 0)
	for _, dbfile := range res.DbFiles {
		if dbfile.isComplete {
			filesArr = append(filesArr, dbfile)
		}
	}
	return DfmResult{DbFiles: filesArr, Err: nil}
}
func getActiveFile() DfmResult {
	res := getAllFiles()
	for _, dbfile := range res.DbFiles {
		if dbfile.isActive {
			return DfmResult{DbFiles: []*DbFile{dbfile}, Err: nil}
		}
	}
	return DfmResult{DbFiles: nil, Err: errors.New("DbfileMapError::No active file found")}
}

func putNewFile() DfmResult {
	opChannel <- operation{op: "putnew"}
	return <-commonOpResultChan
}

func completeFile(id string) DfmResult {
	opChannel <- operation{op: "completefile", id: id}
	return <-commonOpResultChan
}

func initDbFileManager(done <-chan interface{}) {
	fileMgrOnce.Do(func() {
		files, err := os.ReadDir(filePath)
		if err != nil {
			log.Fatalf("Error reading directory: %v", err)
		}
		// all the making of the channels should happend here...?

		initDbFileMap(done, files)
		for _, file := range files {
			if !file.IsDir() {
				fileName := file.Name()
				id := strings.Split(fileName, ".")[0]
				id = strings.Split(id, "_")[0]
				isActive := strings.Contains(fileName, "_a")
				isComplete := strings.Contains(fileName, "_c")
				fileOb, err := os.OpenFile(filePath+"/"+fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
				if err != nil {
					log.Fatalf("Error opening file %s: %v", fileName, err)
				}
				dbFile := &DbFile{
					id:                     id,
					file:                   fileOb,
					fileLock:               &sync.Mutex{},
					isActive:               isActive,
					isComplete:             isComplete,
					location:               filePath,
					fileChangeResultOpChan: make(chan fileOpResult),
					fileChangeOpChan:       make(chan fileChangeIO),
				}
				putFile(id, dbFile)
			}
		}
	})
}

//################################# New file operations ###################
/*should support file operations like:
appendBlock
readBlock
deleteBlock --> not supported right now
makeFileComplete
makeFileActive
iterator on file
--> all these operations should be happening concurrently
*/

type fileChangeIO struct {
	op         string
	BlockBytes []byte
}

type fileOpResult struct {
	BlockBytes []byte
	Offset     int64
	Fid        string
	Err        error
}

func (file *DbFile) startFileOperation() {
	file.fileChangeOpChan = make(chan fileChangeIO)
	file.fileChangeResultOpChan = make(chan fileOpResult)
	file.fileChangeOps()
}

func (file *DbFile) makeFileComplete() error {
	oldFileName := getFileName(file.id, file.isActive, file.isComplete)
	newFileName := getFileName(file.id, false, true)

	err := os.Rename(file.location+"/"+oldFileName, file.location+"/"+newFileName)
	file.isComplete = true
	file.isActive = false
	if err != nil {
		log.Println("Error renaming file %s, %s", oldFileName, err)
		return err
	}
	return nil
}
func (file *DbFile) makeFileActive() {
	oldFileName := getFileName(file.id, false, false)
	newFileName := getFileName(file.id, true, false)
	file.isActive = true
	file.isComplete = false

	err := os.Rename(file.location+"/"+oldFileName, file.location+"/"+newFileName)
	if err != nil {
		log.Fatalf("Error renaming file %s, %s", oldFileName, err)
	}
}

func (file *DbFile) fileChangeOps() <-chan fileOpResult {

	performFileOps := func(oper fileChangeIO) {
		switch oper.op {
		case "appendblock":
			fmt.Println("appending block")
			stat, err := file.file.Stat()
			if err != nil {
				file.fileChangeResultOpChan <- fileOpResult{Err: err}
				return
			}
			offset := stat.Size()
			n, err := file.file.Write(oper.BlockBytes)
			if err != nil || n != len(oper.BlockBytes) {
				fmt.Println(err)
				file.fileChangeResultOpChan <- fileOpResult{Err: errors.New("FileOperationError::error while writing block to file")}
			}
			err = file.file.Sync()
			if err != nil {
				file.fileChangeResultOpChan <- fileOpResult{Err: errors.New("FileOperationError::error while writing block to file")}
			}
			file.fileChangeResultOpChan <- fileOpResult{Offset: offset, Fid: file.id, Err: nil}
		}
	}
	go func() {
		for changeOp := range file.fileChangeOpChan {
			performFileOps(changeOp)
		}
	}()
	return file.fileChangeResultOpChan
}

func (file *DbFile) getBlock(offset int64, size int) <-chan fileOpResult {
	fileOpResultChan := make(chan fileOpResult)
	go func() {
		defer close(fileOpResultChan)
		stat, err := file.file.Stat()
		if err != nil {
			fileOpResultChan <- fileOpResult{Err: err}
			return
		}
		fileSize := stat.Size()
		if offset >= fileSize {
			fileOpResultChan <- fileOpResult{Err: errors.New("FileOperationError::offset greater than file size")}
			return
		}
		b := make([]byte, size)
		file.file.ReadAt(b, offset)
		fileOpResultChan <- fileOpResult{BlockBytes: b}
	}()
	return fileOpResultChan
}

func (file *DbFile) appendBlock(b []byte) <-chan fileOpResult {
	fileOpResultChan := make(chan fileOpResult)
	go func() {
		defer close(fileOpResultChan)
		file.fileChangeOpChan <- fileChangeIO{op: "appendblock", BlockBytes: b}
		fileOpResultChan <- <-file.fileChangeResultOpChan
		fileSizeSignalChan <- int64(len(b))
	}()
	return fileOpResultChan
}

func (file *DbFile) getSize() int64 {
	stat, err := file.file.Stat()
	if err != nil {
		log.Fatalf("Error getting file info: %v", err)
	}
	return stat.Size()
}

func (file *DbFile) iterator() <-chan fileOpResult {
	fileOpResultChan := make(chan fileOpResult)
	go func() {
		defer close(fileOpResultChan)
		stat, err := file.file.Stat()
		if err != nil {
			fileOpResultChan <- fileOpResult{Err: err}
			return
		}
		offset := int64(0)
		for offset < stat.Size() {

			if err != nil {
				log.Fatalf("Error getting file info: %v", err)
			}
			fileSize := stat.Size()

			totalBytes := make([]byte, 0)
			bytesRead := int64(0)
			found := false
			for !found {
				if offset+bytesRead >= fileSize {
					found = true
					continue
				}
				buffSize := int(math.Min(float64(10), float64(fileSize-offset+bytesRead)))
				blockBytes := make([]byte, buffSize)
				_, err = file.file.ReadAt(blockBytes, offset+bytesRead)
				for i, b := range blockBytes {
					bytesRead += 1
					if b == '\r' && i > 0 {
						found = true
						break
					}
					blockBytes = blockBytes[:i]
				}
				totalBytes = append(totalBytes, blockBytes...)
			}
			d := data.NewEnde()
			_, key, val := d.DecodeData(totalBytes)
			fmt.Println("iterator >>>> ", key, " : ", val)
			fileOpResultChan <- fileOpResult{BlockBytes: totalBytes, Offset: offset}
			offset += bytesRead
		}
	}()
	return fileOpResultChan
}

func getFileName(id string, isActive bool, isComplete bool) string {
	if isActive {
		return id + "_a.db"
	}
	if isComplete {
		return id + "_c.db"
	}
	return id + ".db"
}
