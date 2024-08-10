package memops

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"bitcask/data"
	"bitcask/diskops"
)

type BlockAddr struct{
	Fid string
	Offset int
	Size int
	Timestamp int64
}

type KeyDir struct {
	m map[string]*BlockAddr
	keyDirLock *sync.RWMutex
	fileMgr *diskops.FileMgr
	ende *data.Ende
}

var keyDirInst *KeyDir
var keyDirOnce sync.Once

func NewKeyDir(fileMgr *diskops.FileMgr, ende *data.Ende) *KeyDir {

	keyDirInst = &KeyDir{
			keyDirLock: &sync.RWMutex{},
			ende: ende,
			fileMgr: fileMgr,
	}
	keyDirInst.initMap()
	return keyDirInst
}


func (k *KeyDir) initMap() {
	k.keyDirLock.Lock()
	defer k.keyDirLock.Unlock()

	k.m = make(map[string]*BlockAddr)

	for _, file := range k.fileMgr.GetAllFiles() {
		iter := file.NewIterator()
		for iter.IsNext(){
			bytes, offset := iter.Next()
			timestamp, key, _ := k.ende.DecodeData(bytes)
			if timestamp == -1 {
				continue
			}
			stringKey := k.getByteString(key)

			if v, ok := k.m[stringKey]; ok {
					if v.Timestamp < timestamp {
						fmt.Println(v.Timestamp)
						fmt.Println(timestamp)
						k.m[stringKey] = &BlockAddr{Fid: file.GetId(), Offset: offset, Size: len(bytes), Timestamp: timestamp}
					}
			}else{
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
	k.keyDirLock.Lock()
	defer k.keyDirLock.Unlock()

	k.m[key] = &BlockAddr{Fid: fid, Offset: offset, Size: size, Timestamp:timestamp }
}

func (k *KeyDir) GetBlockAddr(key string) *BlockAddr {
	k.keyDirLock.RLock()
	defer k.keyDirLock.RUnlock()
	return k.m[key]
}

func (k *KeyDir) SetNewBlockAddrMap(newMap map[string]*BlockAddr) {
	k.keyDirLock.Lock()
	defer k.keyDirLock.Unlock()
	k.m = newMap
}

