package cask

import (
	"bitcask/diskops"
	"bitcask/memops"
	"bitcask/data"
	"encoding/json"

	"os"
	"log"
)

const filePath = "/Users/rohith/Desktop/Desktop - Rohith’s MacBook Air/incubation/bitcask/dbfiles"

type dmgr struct {
	filemgr *diskops.FileMgr
	diskMgr *diskops.DiskMgr
	keyDir *memops.KeyDir
	afExSig chan string
}

func initDmgr(filemgr *diskops.FileMgr,diskMgr *diskops.DiskMgr, keyDir *memops.KeyDir, afExSig chan string) *dmgr {
	dmgr := &dmgr{
		filemgr: filemgr,
		diskMgr: diskMgr,
		keyDir: keyDir,
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


func (d *dmgr) mergeFiles(completeFiles []*diskops.DbFile) {
	ende := data.NewEnde()
	temp := make(map[string]*memops.BlockAddr)
	tdbFile := d.filemgr.AddNewFile()
	tFile := tdbFile.GetFile()
	cFiles := make([]*diskops.DbFile, 0)
	for _, file := range completeFiles {
		iter := file.NewIterator()
		for iter.IsNext() {
			bytes, _ := iter.Next()
			timestamp, key, value := ende.DecodeData(bytes)
			stringKey := d.getByteString(key)

			if timestamp == -1 {
				continue
			}

			if  timestamp == d.keyDir.GetBlockAddr(stringKey).Timestamp {
				tbytes := ende.EncodeData(timestamp, key, value)
				toffset, err := d.diskMgr.AppendBlockWithFileId(tdbFile.GetId(),tbytes)
				if err != nil {
					log.Fatalf("Error appending block to file with offset: %d",toffset)
				}
				temp[stringKey] = &memops.BlockAddr{Fid: tdbFile.GetId(), Offset: int(toffset), Size: len(tbytes),Timestamp: timestamp}
			}
		}
		cFiles = append(cFiles, file)
	}
	tFile.Sync()
	tdbFile.ReleaseFile()

	d.filemgr.MakeFileComplete(tdbFile.GetId())
	d.keyDir.SetNewBlockAddrMap(temp)
	for _, file := range cFiles {
		d.filemgr.DeleteFile(file.GetId())
	}
}

func (d *dmgr) manage() {
	for {
		fid := <-d.afExSig
		d.filemgr.MakeFileComplete(fid)
		completeFiles := d.filemgr.GetCompleteFiles()
		if len(completeFiles) >= 5 {
			d.mergeFiles(completeFiles)
		}      
	}
}
