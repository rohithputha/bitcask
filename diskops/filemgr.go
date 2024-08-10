package diskops

import (
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"math"

)

const filePath = "/Users/rohith/Desktop/Desktop - Rohith’s MacBook Air/incubation/bitcask/dbfiles"

var initOnce sync.Once
var fileMgrInst *FileMgr

// type fileMgr struct {
// 	fileMaps    map[string]*os.File
// 	fileMgrLock *sync.RWMutex
// }

// func newFileMgr() *fileMgr {
// 	initOnce.Do(func() {
// 		fileMgrInst = &fileMgr{
// 			fileMaps:    make(map[string]*os.File),
// 			fileMgrLock: &sync.RWMutex{},
// 		}
// 	})
// 	return fileMgrInst
// }

// func (f *fileMgr) getFile(fileName string) *os.File {
// 	f.fileMgrLock.RLock()
// 	defer f.fileMgrLock.RUnlock()
// 	return f.fileMaps[fileName]
// }

// func (f *fileMgr) addFile(fileName string) {
// 	f.fileMgrLock.Lock()
// 	defer f.fileMgrLock.Unlock()
// 	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
// 	if err != nil {
// 		log.Fatalf("Error opening file %s", fileName)
// 	}
// 	f.fileMaps[fileName] = file
// }

// func (f *fileMgr) removeFile(fileName string) {
// 	f.fileMgrLock.Lock()
// 	defer f.fileMgrLock.Unlock()
// 	delete(f.fileMaps, fileName)
// }

// -----------------------------------------------

type DbFileIterator interface {
	Next() ([]byte, int)
	IsNext() bool
}

type DbFileIteratorStr struct {
	file *DbFile
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
	dbFiles     map[string]*DbFile
	fileMgrLock *sync.RWMutex

}

func (fmgr *FileMgr) GetFile(id string) *DbFile {
	fmgr.fileMgrLock.RLock()
	defer fmgr.fileMgrLock.RUnlock()

	return fmgr.dbFiles[id]
}

// func (fmgr *FileMgr) AddFile(fileName string) {
// 	fmgr.fileMgrLock.Lock()
// 	defer fmgr.fileMgrLock.Unlock()

// 	fileLocation := filePath+"/"+fileName
// 	id:= strings.Split(fileName, "_")[0]

// 	file, err := os.OpenFile(fileLocation, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
// 	if err != nil {
// 		log.Fatalf("Error opening file %s", fileName)
// 	}

// 	fmgr.dbFiles[fileName] = &DbFile{
// 		file: file,
// 		fileLock: &sync.Mutex{},
// 		isActive: false,
// 		location: fileLocation,
// 		id: id,
// 	}
// }

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
	fmgr.fileMgrLock.Lock()
	defer fmgr.fileMgrLock.Unlock()

	nextId := fmgr.getnextFileId()
	fileName := nextId + ".db"
	fileLocation := filePath
	file, err := os.OpenFile(fileLocation+"/"+fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		log.Fatalf("Error opening file %s", fileLocation)
	}

	fmgr.dbFiles[nextId] = &DbFile{
		id:         nextId,
		file:       file,
		fileLock:   &sync.Mutex{},
		isActive:   false,
		isComplete: false,
		location:   fileLocation,
	}

	return fmgr.dbFiles[nextId]
}

func (fmgr *FileMgr) MakeFileActive(fileName string) error {
	fmgr.fileMgrLock.Lock()
	defer fmgr.fileMgrLock.Unlock()

	for _, file := range fmgr.dbFiles {
		if file.isActive {
			return errors.New("cask:filemgr::active file exists")
		}
	}

	dbFile := fmgr.dbFiles[fileName]
	dbFile.GetFile()
	defer dbFile.ReleaseFile()

	oldFileName := fmgr.getFileName(dbFile.id, dbFile.isActive, dbFile.isComplete)
	newFileName := fmgr.getFileName(fmgr.dbFiles[fileName].id, true, dbFile.isComplete)

	err := os.Rename(dbFile.location+"/"+oldFileName, dbFile.location+"/"+newFileName)
	if err != nil {
		log.Fatalf("Error renaming file %s", oldFileName)
	}
	dbFile.isActive = true
	return nil
}

func (fmgr *FileMgr) MakeFileComplete(fid string) error {
	fmgr.fileMgrLock.Lock()
	defer fmgr.fileMgrLock.Unlock()

	dbFile := fmgr.dbFiles[fid]
	dbFile.GetFile()
	defer dbFile.ReleaseFile()

	oldFileName := fmgr.getFileName(fid, dbFile.isActive, dbFile.isComplete)
	newFileName := fmgr.getFileName(fmgr.dbFiles[fid].id, false, true)

	err := os.Rename(dbFile.location+"/"+oldFileName, dbFile.location+"/"+newFileName)
	if err != nil {
		log.Fatalf("Error renaming file %s", oldFileName)
	}
	dbFile.isActive = false
	dbFile.isComplete = true
	return nil
}

func (fmgr *FileMgr) GetActiveFile() *DbFile {
	fmgr.fileMgrLock.RLock()
	defer fmgr.fileMgrLock.RUnlock()

	for _, file := range fmgr.dbFiles {
		if file.isActive {
			return file
		}
	}
	return nil
}

func (fmgr *FileMgr) GetCompleteFiles() []*DbFile {
	fmgr.fileMgrLock.RLock()
	defer fmgr.fileMgrLock.RUnlock()

	var files []*DbFile
	for _, file := range fmgr.dbFiles {
		if file.isComplete {
			files = append(files, file)
		}
	}
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
	fmgr.fileMgrLock.RLock()
	defer fmgr.fileMgrLock.RUnlock()

	file := fmgr.dbFiles[fid]
	stat, err := file.file.Stat()
	if err != nil {
		log.Fatalf("Error getting file info: %v", err)
	}
	return stat.Size()
}


func (fmgr *FileMgr) DeleteFile(fid string) {
	fmgr.fileMgrLock.Lock()
	defer fmgr.fileMgrLock.Unlock()

	dbFile := fmgr.dbFiles[fid]
	dbFile.DeleteFile()

	delete(fmgr.dbFiles, fid)
}
func InitFileMgr() *FileMgr {

	initOnce.Do(func() {
		fmgr := &FileMgr{
			dbFiles:     make(map[string]*DbFile),
			fileMgrLock: &sync.RWMutex{},
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
				fmgr.dbFiles[id] = &DbFile{
					id:         id,
					file:       fileOb,
					fileLock:   &sync.Mutex{},
					isActive:   isActive,
					isComplete: isComplete,
					location:   filePath,
				}
				// Additional initialization for DbFile can be done here
			}
		}	
		fileMgrInst = fmgr
	})

	
	return fileMgrInst
}

//------------------------------------------------

func (dbf *DbFile) GetFile() *os.File {
	// Here, if there is the file is complete, then no lock is needed as it is read only
	// need to change it...
	// dbf.fileLock.Lock()
	
	return dbf.file
}
func (dbf *DbFile) ReleaseFile() {
	// dbf.fileLock.Unlock()
}

func (dbf *DbFile) GetId() string {
	return dbf.id
}

func (dbf *DbFile) DeleteFile() {
	dbf.fileLock.Lock()
	defer dbf.fileLock.Unlock()
	err := os.Remove(dbf.location + "/" + dbf.id + "_c.db")
	if err != nil {
		log.Fatalf("Error deleting file: %v", err)
	}
}

// -----------------------------------------------

func (dbfi *DbFileIteratorStr) Next() ([]byte, int){
	dbfi.file.fileLock.Lock()
	defer dbfi.file.fileLock.Unlock()

	stat, err := dbfi.file.file.Stat()
	if err != nil {
		log.Fatalf("Error getting file info: %v", err)
	}
	fileSize := stat.Size()
	
	totalBytes := make([]byte, 0)
	bytesRead := 0
	found := false
	offset := dbfi.presentOffset

	for !found {
		if int64(dbfi.presentOffset+bytesRead) >= fileSize {
			found = true
			continue
		}
		buffSize := int(math.Min(float64(10), float64(fileSize-int64(dbfi.presentOffset+bytesRead))))
		blockBytes := make([]byte, buffSize)
		_, err = dbfi.file.file.ReadAt(blockBytes, int64(dbfi.presentOffset+bytesRead))
		for i, b := range blockBytes {

			if b == '\r' && i > 0  {
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
	return totalBytes, offset
}

func (dbfi *DbFileIteratorStr) IsNext() bool {
	dbfi.file.fileLock.Lock()
	defer dbfi.file.fileLock.Unlock()

	stat, err := dbfi.file.file.Stat()
	if err != nil {
		log.Fatalf("Error getting file info: %v", err)
	}
	fileSize := stat.Size()
	return int64(dbfi.presentOffset) < fileSize
}

func (dbf *DbFile) NewIterator() DbFileIterator {
	return &DbFileIteratorStr{
		file: dbf,
		presentOffset: 0,
	}
}


// -----------------------------------------------