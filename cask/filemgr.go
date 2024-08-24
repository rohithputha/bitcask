package cask

import (
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

// -----------------------------------------------
