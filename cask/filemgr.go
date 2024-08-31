package cask

import (
	"errors"
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

func (fmgr *FileMgr) GetFile(id string) *DbFile {
	return fmgr.dbFiles[id]
}

func (fmgr *FileMgr) getnextFileId() string {
	var maxId int64
	for _, file := range fmgr.dbFiles {
		id, err := strconv.ParseInt(file.id, 10, 64)
		if err != nil {
			log.Fatalf("Error parsing file id: %v", err)
		}
		if id > maxId {
			maxId = id
		}
	}
	return strconv.FormatInt(maxId+1, 10)
}

func (fmgr *FileMgr) getFileName(id string, isActive bool, isComplete bool) string {
	if isActive {
		return id + "_a.db"
	}
	if isComplete {
		return id + "_c.db"
	}
	return id + ".db"
}

func (fmgr *FileMgr) AddNewFile() *DbFile {
	var nid string
	withLocks(func() {
		nid = fmgr.getnextFileId()
		fileName := nid + ".db"
		fileLocation := filePath
		file, err := os.OpenFile(fileLocation+"/"+fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
		if err != nil {
			log.Fatalf("Error opening file %s", fileLocation)
		}
		fmgr.dbFiles[nid] = &DbFile{
			id:         nid,
			file:       file,
			fileLock:   &sync.Mutex{},
			isActive:   false,
			isComplete: false,
			location:   fileLocation,
		}
	}, fileMgrLock())
	return fmgr.dbFiles[nid]
}

func (fmgr *FileMgr) MakeFileActive(fileName string) error {

	withLocks(func() {
		for _, file := range fmgr.dbFiles {
			if file.isActive {
				log.Fatalf("cask:filemgr::active file exists")
			}
		}
		dbFile := fmgr.dbFiles[fileName]
		dbFile.GetFile()

		oldFileName := fmgr.getFileName(dbFile.id, dbFile.isActive, dbFile.isComplete)
		newFileName := fmgr.getFileName(fmgr.dbFiles[fileName].id, true, dbFile.isComplete)

		err := os.Rename(dbFile.location+"/"+oldFileName, dbFile.location+"/"+newFileName)
		if err != nil {
			log.Fatalf("Error renaming file %s", oldFileName)
		}
		dbFile.isActive = true
	}, fileMgrLock())

	return nil
}

func (fmgr *FileMgr) MakeFileComplete(fid string) error {

	withLocks(func() {
		dbFile := fmgr.dbFiles[fid]
		dbFile.GetFile()
		oldFileName := fmgr.getFileName(fid, dbFile.isActive, dbFile.isComplete)
		newFileName := fmgr.getFileName(fmgr.dbFiles[fid].id, false, true)

		err := os.Rename(dbFile.location+"/"+oldFileName, dbFile.location+"/"+newFileName)
		if err != nil {
			log.Fatalf("Error renaming file %s, %s", oldFileName, err)
		}
		dbFile.isActive = false
		dbFile.isComplete = true
	}, fileMgrLock())
	return nil
}

func (fmgr *FileMgr) GetActiveFile() *DbFile {

	var dbFile *DbFile
	withLocks(func() {
		for _, file := range fmgr.dbFiles {
			if file.isActive {
				dbFile = file
			}
		}
	}, fileMgrRWLock())

	return dbFile
}

func (fmgr *FileMgr) GetCompleteFiles() []*DbFile {

	var files []*DbFile
	withLocks(func() {
		for _, file := range fmgr.dbFiles {
			if file.isComplete {
				files = append(files, file)
			}
		}
	}, fileMgrRWLock())
	return files
}

func (fmgr *FileMgr) GetAllFiles() []*DbFile {
	files := fmgr.GetCompleteFiles()
	activeFile := fmgr.GetActiveFile()
	if activeFile != nil {
		files = append(files, activeFile)
	}
	return files
}

func (fmgr *FileMgr) GetFileSize(fid string) int64 {

	var file *DbFile
	withLocks(func() {
		file = fmgr.dbFiles[fid]
	}, fileMgrRWLock())

	stat, err := file.file.Stat()
	if err != nil {
		log.Fatalf("Error getting file info: %v", err)
	}
	return stat.Size()
}

func (fmgr *FileMgr) DeleteFile(fid string) {
	withLocks(func() {
		dbFile := fmgr.dbFiles[fid]
		dbFile.DeleteFile()

		delete(fmgr.dbFiles, fid)
	}, fileMgrLock())
}
func InitFileMgr() *FileMgr {

	initOnce.Do(func() {
		fmgr := &FileMgr{
			dbFiles: make(map[string]*DbFile),
		}

		files, err := os.ReadDir(filePath)
		if err != nil {
			log.Fatalf("Error reading directory: %v", err)
		}

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

				withLocks(func() {
					fmgr.dbFiles[id] = &DbFile{
						id:         id,
						file:       fileOb,
						fileLock:   &sync.Mutex{},
						isActive:   isActive,
						isComplete: isComplete,
						location:   filePath,
					}
				}, fileMgrLock())

				// Additional initialization for DbFile can be done here
			}
		}
		fileMgrInst = fmgr
	})

	return fileMgrInst
}

//------------------------------------------------

func (dbf *DbFile) GetFile() *os.File {
	return dbf.file
}

func (dbf *DbFile) GetId() string {
	return dbf.id
}

func (dbf *DbFile) DeleteFile() {
	err := os.Remove(dbf.location + "/" + dbf.id + "_c.db")
	if err != nil {
		log.Fatalf("Error deleting file: %v", err)
	}
}

// -----------------------------------------------

func (dbfi *DbFileIteratorStr) Next() ([]byte, int) {
	var totalBytes []byte
	var offset int
	withLocks(func() {
		stat, err := dbfi.file.file.Stat()
		if err != nil {
			log.Fatalf("Error getting file info: %v", err)
		}
		fileSize := stat.Size()

		totalBytes = make([]byte, 0)
		bytesRead := 0
		found := false
		offset = dbfi.presentOffset

		for !found {
			if int64(dbfi.presentOffset+bytesRead) >= fileSize {
				found = true
				continue
			}
			buffSize := int(math.Min(float64(10), float64(fileSize-int64(dbfi.presentOffset+bytesRead))))
			blockBytes := make([]byte, buffSize)
			_, err = dbfi.file.file.ReadAt(blockBytes, int64(dbfi.presentOffset+bytesRead))
			for i, b := range blockBytes {

				if b == '\r' && i > 0 {
					blockBytes = blockBytes[:i]
					found = true
					break
				}

				bytesRead += 1
			}
			totalBytes = append(totalBytes, blockBytes...)
		}
		dbfi.presentOffset += (bytesRead)
		if err != nil {
			log.Fatalf("Error reading block from file: %v", err)
		}
	}, fileRWLock(dbfi.file.GetId()))
	return totalBytes, offset
}

func (dbfi *DbFileIteratorStr) IsNext() bool {
	var isNext bool
	withLocks(func() {
		stat, err := dbfi.file.file.Stat()
		if err != nil {
			log.Fatalf("Error getting file info: %v", err)
		}
		fileSize := stat.Size()
		isNext = int64(dbfi.presentOffset) < fileSize
	}, fileRWLock(dbfi.file.GetId()))
	return isNext
}

func (dbf *DbFile) NewIterator() DbFileIterator {
	return &DbFileIteratorStr{
		file:          dbf,
		presentOffset: 0,
	}
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

func initDbFileMap(done <-chan interface{}, files []os.DirEntry) *dbFileMap {
	dfm := make(dbFileMap)
	dfm.initOperation(done, dfm.getMaxFileId(files))
	return &dfm
}

func (dfm *dbFileMap) getMaxFileId(files []os.DirEntry) int64 {
	var maxId int64
	for _, file := range files {
		if file.IsDir() {
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
			result <- DfmResult{Err: nil}
		case "putnew":
			id := <-nextFileId
			fileName := id + ".db"
			fileLocation := filePath
			file, err := os.OpenFile(fileLocation+"/"+fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
			if err != nil {
				log.Fatalf("Error opening file %s", fileLocation)
			}
			(*dfm)[id] = &DbFile{
				id:         id,
				file:       file,
				fileLock:   &sync.Mutex{},
				isActive:   false,
				isComplete: false,
				location:   fileLocation,
			}
			(*dfm)[id].startFileOperation()
			result <- DfmResult{DbFiles: []*DbFile{(*dfm)[id]}, Err: nil}
		case "delete":
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

// more methods on making the file complete need to be implemented

func initDbFileManager(done <-chan interface{}) {
	fileMgrOnce.Do(func() {
		files, err := os.ReadDir(filePath)
		if err != nil {
			log.Fatalf("Error reading directory: %v", err)
		}
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
					id:         id,
					file:       fileOb,
					fileLock:   &sync.Mutex{},
					isActive:   isActive,
					isComplete: isComplete,
					location:   filePath,
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
	op        string
	BlockByes []byte
}

type fileOpResult struct {
	BlockBytes []byte
	Err        error
}

func (file *DbFile) startFileOperation() {
	file.fileChangeOpChan = make(chan fileChangeIO)
	file.fileChangeResultOpChan = make(chan fileOpResult)
	file.fileChangeOps()
}

func (file *DbFile) makeFileComplete() {
	file.isComplete = true
	file.isActive = false
	close(file.fileChangeOpChan)
	close(file.fileChangeResultOpChan)
	oldFileName := getFileName(file.id, file.isActive, file.isComplete)
	newFileName := getFileName(file.id, false, true)

	err := os.Rename(file.location+"/"+oldFileName, file.location+"/"+newFileName)
	if err != nil {
		log.Fatalf("Error renaming file %s, %s", oldFileName, err)
	}
}
func (file *DbFile) makeFileActive() {
	file.isActive = true
	file.isComplete = false
	oldFileName := getFileName(file.id, file.isActive, file.isComplete)
	newFileName := getFileName(file.id, true, false)

	err := os.Rename(file.location+"/"+oldFileName, file.location+"/"+newFileName)
	if err != nil {
		log.Fatalf("Error renaming file %s, %s", oldFileName, err)
	}
}

func (file *DbFile) fileChangeOps() <-chan fileOpResult {

	performFileOps := func(oper fileChangeIO) {
		switch oper.op {
		case "appendblock":
			stat, err := file.file.Stat()
			if err != nil {
				file.fileChangeResultOpChan <- fileOpResult{Err: err}
				return
			}
			offset := stat.Size()
			n, err := file.file.WriteAt(oper.BlockByes, offset)
			if err != nil || n != len(oper.BlockByes) {
				file.fileChangeResultOpChan <- fileOpResult{Err: errors.New("FileOperationError::error while writing block to file")}
			}
			file.fileChangeResultOpChan <- fileOpResult{Err: nil}
		}
	}
	go func() {
		for changeOp := range file.fileChangeOpChan {
			performFileOps(changeOp)
		}
	}()
	return file.fileChangeResultOpChan
}

func (file *DbFile) getBlock(offset int64, size int64) <-chan fileOpResult {
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
	file.fileChangeOpChan <- fileChangeIO{op: "appendblock", BlockByes: b}
	return file.fileChangeResultOpChan
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
					if b == '\r' && i > 0 {
						blockBytes = blockBytes[:i]
						found = true
						break
					}
					bytesRead += 1
				}
				totalBytes = append(totalBytes, blockBytes...)
			}
			offset += bytesRead
			fileOpResultChan <- fileOpResult{BlockBytes: totalBytes}
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
