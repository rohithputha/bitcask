package cask

import (
	"log"
	"sync"
)

type DiskMgr struct {
	// DiskMgr is a struct that manages the disk operations
	fm *FileMgr
}

var diskMgrInst *DiskMgr
var diskMgrOnce sync.Once

func NewDiskMgr(fileMgr *FileMgr) *DiskMgr {
	diskMgrOnce.Do(func() {
		diskMgrInst = &DiskMgr{
			fm: fileMgr,
		}
	})

	return diskMgrInst
}

// Read a block from the file given the file name and offset
func (d *DiskMgr) ReadBlock(fileName string, offset int, totalBytes int) []byte {
	dbfile := d.fm.GetFile(fileName)
	file := dbfile.GetFile()
	var blockBytes []byte
	withLocks(func() {
		blockBytes = make([]byte, totalBytes)
		_, err := file.ReadAt(blockBytes, int64(offset))
		if err != nil {
			log.Fatalf("Error reading block from file %s", fileName)
		}

	}, fileRWLock(dbfile.id))
	return blockBytes
}

func (d *DiskMgr) AppendBlock(block []byte) (fid string, offset int64, fileWriteErr error) {
	dbfile := d.fm.GetActiveFile()
	if dbfile == nil {
		dbfile = d.fm.AddNewFile()
		d.fm.MakeFileActive(dbfile.id)
	}
	file := dbfile.GetFile()
	offset = int64(-1)
	fid = dbfile.id
	withLocks(func() {
		fileInfo, fileWriteErr := file.Stat()
		if fileWriteErr != nil {
			fid = "-1"
			offset = -1
		}
		offset = fileInfo.Size()
		_, fileWriteErr = file.Write(block)
		file.Sync()
	}, fileRWLock(dbfile.id))

	return fid, offset, fileWriteErr
}

func (d *DiskMgr) AppendBlockWithFileId(fid string, block []byte) (offset int64, err error) {
	dbfile := d.fm.GetFile(fid)
	file := dbfile.GetFile()

	fileInfo, err := file.Stat()
	if err != nil {
		return -1, err
	}

	offset = fileInfo.Size()
	_, err = file.Write(block)
	file.Sync()

	return offset, err
}

func (d *DiskMgr) GetFileSize(fid string) int64 {
	return d.fm.GetFileSize(fid)
}
