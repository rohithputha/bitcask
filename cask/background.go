package cask

import (
	"bitcask/data"
	"encoding/json"
	"fmt"
	"log"
	"os"
)

//const filePath = "/Users/rohith/Desktop/Desktop - Rohith’s MacBook Air/incubation/bitcask/dbfiles"

type dmgr struct {
	filemgr *FileMgr
	diskMgr *DiskMgr
	keyDir  *KeyDir
	afExSig chan string
}

func initDmgr(filemgr *FileMgr, diskMgr *DiskMgr, keyDir *KeyDir, afExSig chan string) *dmgr {
	dmgr := &dmgr{
		filemgr: filemgr,
		diskMgr: diskMgr,
		keyDir:  keyDir,
		afExSig: afExSig,
	}
	go dmgr.manage()
	return dmgr
}

func totalFilesCheck() int {
	files, err := os.ReadDir(filePath)
	if err != nil {
		return -1
	}
	return len(files)
}

func (d *dmgr) getByteString(key interface{}) string {
	b, err := json.Marshal(key)
	if err != nil {
		log.Fatalf("Error marshalling key: %v", err)
	}
	return string(b)
}

func (d *dmgr) mergeFiles(completeFiles []*DbFile) {
	ende := data.NewEnde()
	temp := make(map[string]*BlockAddr)
	tdbFile := d.filemgr.AddNewFile()
	tFile := tdbFile.GetFile()
	cFiles := make([]*DbFile, 0)
	for _, file := range completeFiles {
		iter := file.NewIterator()
		for iter.IsNext() {
			bytes, _ := iter.Next()
			if len(bytes) < 18 {
				continue
			}
			timestamp, key, value := ende.DecodeData(bytes)
			stringKey := d.getByteString(key)

			if timestamp == -1 {
				continue
			}
			presentKeyVal := temp[stringKey]
			if presentKeyVal == nil || timestamp > presentKeyVal.Timestamp {
				tbytes := ende.EncodeData(timestamp, key, value)
				toffset, err := d.diskMgr.AppendBlockWithFileId(tdbFile.GetId(), tbytes)
				if err != nil {
					log.Fatalf("Error appending block to file with offset: %d", toffset)
				}
				temp[stringKey] = &BlockAddr{Fid: tdbFile.GetId(), Offset: int(toffset), Size: len(tbytes), Timestamp: timestamp}
			}
		}
		cFiles = append(cFiles, file)
	}
	tFile.Sync()
	d.filemgr.MakeFileComplete(tdbFile.GetId())
	presentKeyVal := new(BlockAddr)
	for key, blockAddr := range temp {
		withLocks(func() {
			presentKeyVal = d.keyDir.GetBlockAddr(key)
		}, keyDirRWLock())
		if presentKeyVal == nil {
			continue
		}
		if presentKeyVal.Timestamp <= blockAddr.Timestamp {
			withLocks(func() {
				d.keyDir.AddKey(key, blockAddr.Fid, blockAddr.Offset, blockAddr.Size, blockAddr.Timestamp)
			}, keyDirLock())
		}
	}
	//d.keyDir.SetNewBlockAddrMap(temp)

	for _, file := range cFiles {
		d.filemgr.DeleteFile(file.GetId())
	}
	fmt.Println(tdbFile.GetId())

}

func (d *dmgr) manage() {
	for {
		<-d.afExSig
		completeFiles := d.filemgr.GetCompleteFiles()
		if len(completeFiles) >= 5 {
			d.mergeFiles(completeFiles)
		}
	}
}
