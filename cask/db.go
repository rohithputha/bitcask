package cask

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"bitcask/data"
	"bitcask/diskops"
	"bitcask/memops"
)

type CaskDb struct {
	diskMgr *diskops.DiskMgr
	ed *data.Ende
	keyDir *memops.KeyDir
	dmgr *dmgr
	fileMgr *diskops.FileMgr
}

var caskDbInst *CaskDb
var caskDbOnce sync.Once

func NewCaskDb() *CaskDb {
    caskDbOnce.Do(func() {
		fileMgr := diskops.InitFileMgr()
		keyDir:= memops.NewKeyDir(fileMgr, data.NewEnde())
		diskMgr := diskops.NewDiskMgr(fileMgr)
		caskDbInst = &CaskDb{
			fileMgr: fileMgr,
			diskMgr: diskMgr,
			keyDir: keyDir,
			ed: data.NewEnde(),
			dmgr: initDmgr(fileMgr, diskMgr,keyDir, make(chan string)),
		}
	})

	return caskDbInst
}

func (c *CaskDb) ReadDbFiles() {

}

func (c *CaskDb) getByteString(key interface{}) string {
	b, err := json.Marshal(key)
	if err != nil {
		log.Fatalf("Error marshalling key: %v", err)
	}
	return string(b)
}


func (c *CaskDb) Put(key interface{}, value interface{}) {
	time:= time.Now().Unix()
	blockBytes := c.ed.EncodeData(time, key, value)
	
	fid, offset, err := c.diskMgr.AppendBlock(blockBytes)
	if err != nil {
		log.Fatalf("Error appending block to file with offset: %d",offset)
	}
	c.keyDir.AddKey(c.getByteString(key), fid, int(offset), len(blockBytes),time)

	fileSize := c.diskMgr.GetFileSize(fid)
    if err != nil {
        log.Fatalf("Error getting file size for fid %d: %v", fid, err)
    }
    if fileSize > 500 {
		c.fileMgr.MakeFileComplete(fid)
        c.dmgr.afExSig <- fid
    }
}

func (c *CaskDb) Get(key interface{}) interface{} {
	keyString := c.getByteString(key)
	blockAddr := c.keyDir.GetBlockAddr(keyString)
	if blockAddr == nil {
		return nil
	}

	blockBytes := c.diskMgr.ReadBlock(blockAddr.Fid, blockAddr.Offset, blockAddr.Size)
	_, key, value := c.ed.DecodeData(blockBytes)
	return value	
}