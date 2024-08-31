package cask

import (
	"bitcask/data"
	"sync"
)

type CaskDb struct {
	diskMgr *DiskMgr
	ed      *data.Ende
	keyDir  *KeyDir
	dmgr    *dmgr
	fileMgr *FileMgr
}

var caskDbInst *CaskDb
var caskDbOnce sync.Once

//func NewCaskDb() *CaskDb {
//	caskDbOnce.Do(func() {
//		initLocks()
//		fileMgr := InitFileMgr()
//		keyDir := NewKeyDir(fileMgr, data.NewEnde())
//		diskMgr := NewDiskMgr(fileMgr)
//		caskDbInst = &CaskDb{
//			fileMgr: fileMgr,
//			diskMgr: diskMgr,
//			keyDir:  keyDir,
//			ed:      data.NewEnde(),
//			dmgr:    initDmgr(fileMgr, diskMgr, keyDir, make(chan string, 1000)),
//		}
//	})
//	return caskDbInst
//}

//func (c *CaskDb) ReadDbFiles() {
//
//}
//
//func (c *CaskDb) getByteString(key interface{}) string {
//	b, err := json.Marshal(key)
//	if err != nil {
//		log.Fatalf("Error marshalling key: %v", err)
//	}
//	return string(b)
//}
//
//func (c *CaskDb) Put(key interface{}, value interface{}) {
//	time := time.Now().Unix()
//	blockBytes := c.ed.EncodeData(time, key, value)
//	withLocks(func() {
//		fid, offset, err := c.diskMgr.AppendBlock(blockBytes)
//		if err != nil {
//			log.Fatalf("Error appending block to file with offset: %d", offset)
//			return
//		}
//		c.keyDir.AddKey(c.getByteString(key), fid, int(offset), len(blockBytes), time)
//		fileSize := c.diskMgr.GetFileSize(fid)
//		if fileSize > 5000 {
//			c.fileMgr.MakeFileComplete(fid)
//			c.dmgr.afExSig <- fid
//		}
//	}, keyDirLock())
//}

//func (c *CaskDb) Get(key interface{}) interface{} {
//	keyString := c.getByteString(key)
//	var blockAddr *BlockAddr
//	withLocks(func() {
//		blockAddr = c.keyDir.GetBlockAddr(keyString)
//	}, keyDirRWLock())
//	if blockAddr == nil {
//		fmt.Println("block Adddress is nil")
//		return nil
//	}
//	blockBytes := c.diskMgr.ReadBlock(blockAddr.Fid, blockAddr.Offset, blockAddr.Size)
//	_, key, value := c.ed.DecodeData(blockBytes)
//	return value
//}

//func

/*
multithreading env for put()
1. 2 or more threads are being used to put for the same key
t1 done with putting the key in file
t2 now comes and puts the key in file
t2 goes and writes to the keydir
t1 then writes to the keydir
causes t1 value to be in the file and t2 value in the keydir: which is inconsistent

we need to lock keyDir for the entire transaction: would be better if the lock happens on the key in the keydir... this will not lock the entire keydir when there is a write?
but the main disadvantage is that we will have a lock foreach key and it consumes a lot of memory

one way is maybe have a lock for a range of keys...?
*/

/*
multithreading env for get() and put() mix with the same key
 t1 (put) comes and lock the keydir, for a key k
 t2 (get) comes and waits for lock on keydir to open
 t1 (put) finishes and release lock
 t2 (get) goes and fetches latest value in key dir

 t2 (get) comes first read lock on key dir
 t1 (put) comes and waits for the read lock to be released
 t2 (get) release the lock on keydire
 t1 (put) locks on keydir
 t2 (get) locks on non - active file
 t1 (put) locks on active file and finishes
 t2 (get) gets a value which is consistento time when the keydir is read...
 but what if the t2 value is in active file and t1 gets the file lock first and writes k first before t2...
 -> is consistency measured based on time it reads from file or time it reads from keydir??

*/
