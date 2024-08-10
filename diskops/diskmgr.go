package diskops

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
	blockBytes := make([]byte, totalBytes)
	_, err := file.ReadAt(blockBytes, int64(offset))
	dbfile.ReleaseFile()
	if err != nil {
		log.Fatalf("Error reading block from file %s", fileName)
	}
	
	return blockBytes
}

func (d *DiskMgr) AppendBlock(block []byte) (fid string, offset int64, err error) {
	dbfile := d.fm.GetActiveFile()
	if dbfile == nil {
		dbfile = d.fm.AddNewFile()
		d.fm.MakeFileActive(dbfile.id)
	}
	file := dbfile.GetFile()
	defer dbfile.ReleaseFile()

	fileInfo, err := file.Stat()
	if err != nil {
		return "-1", -1, err
	}

	offset = fileInfo.Size()
	_, err = file.Write(block)
	file.Sync()
	
	return dbfile.id,offset, err
}

func (d *DiskMgr) AppendBlockWithFileId(fid string, block []byte) (offset int64, err error) {
	dbfile := d.fm.GetFile(fid)
	file := dbfile.GetFile()
	defer dbfile.ReleaseFile()

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
