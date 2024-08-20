package cask

import (
	"encoding/json"
	"log"
	"sync"

	"bitcask/data"
)

type BlockAddr struct {
	Fid       string
	Offset    int
	Size      int
	Timestamp int64
}

type KeyDir struct {
	m       map[string]*BlockAddr
	fileMgr *FileMgr
	ende    *data.Ende
}

var keyDirInst *KeyDir
var keyDirOnce sync.Once

func NewKeyDir(fileMgr *FileMgr, ende *data.Ende) *KeyDir {

	keyDirInst = &KeyDir{
		ende:    ende,
		fileMgr: fileMgr,
	}
	keyDirInst.initMap()
	return keyDirInst
}

func (k *KeyDir) initMap() {
	k.m = make(map[string]*BlockAddr)

	for _, file := range k.fileMgr.GetAllFiles() {
		iter := file.NewIterator()
		for iter.IsNext() {
			bytes, offset := iter.Next()
			timestamp, key, _ := k.ende.DecodeData(bytes)
			if timestamp == -1 {
				continue
			}
			stringKey := k.getByteString(key)

			if v, ok := k.m[stringKey]; ok {
				if v.Timestamp < timestamp {
					k.m[stringKey] = &BlockAddr{Fid: file.GetId(), Offset: offset, Size: len(bytes), Timestamp: timestamp}
				}
			} else {
				k.m[stringKey] = &BlockAddr{Fid: file.GetId(), Offset: offset, Size: len(bytes), Timestamp: timestamp}
			}
		}
	}
}

func (k *KeyDir) getByteString(key interface{}) string {
	b, err := json.Marshal(key)
	if err != nil {
		log.Fatalf("Error marshalling key: %v", err)
	}
	return string(b)
}

func (k *KeyDir) AddKey(key string, fid string, offset int, size int, timestamp int64) {
	k.m[key] = &BlockAddr{Fid: fid, Offset: offset, Size: size, Timestamp: timestamp}
}

func (k *KeyDir) GetBlockAddr(key string) *BlockAddr {
	var g *BlockAddr
	g = k.m[key]
	return g
}

func (k *KeyDir) SetNewBlockAddrMap(newMap map[string]*BlockAddr) {
	withLocks(func() {
		k.m = newMap
	}, keyDirLock())
}
