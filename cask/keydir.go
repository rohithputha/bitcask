package cask

import (
	"encoding/json"
	"log"
	"reflect"
	"sync"

	"bitcask/data"
)

type BlockAddr struct {
	Fid       string
	Offset    int64
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

// ################################ new concurrency design ################################

type keyDirChangeOp struct {
	op         string
	key        string
	opDoneChan chan interface{}
	block      *BlockAddr
}

type KeyBlockAddr struct {
	block *BlockAddr
	key   string
}

var keyDir map[string]*BlockAddr
var keyDirChangeOpChannel chan keyDirChangeOp
var kdOnce sync.Once

func initKeyDir(done <-chan interface{}) {
	kdOnce.Do(func() {
		keyDir = make(map[string]*BlockAddr)
		keyDirChangeOpChannel = make(chan keyDirChangeOp)
		initPresentKeyValues()
		keyDirChangeOps(done)
	})
}

func getByteString(key interface{}) string {
	if reflect.TypeOf(key).Kind() == reflect.String {
		return key.(string)
	}
	b, err := json.Marshal(key)
	if err != nil {
		log.Fatalf("Error marshalling key: %v", err)
	}
	return string(b)
}

func initPresentKeyValues() {
	res := getAllFiles()
	ende := data.NewEnde()
	for _, file := range res.DbFiles {
		iter := file.iterator()
		for opRes := range iter {
			if opRes.Err != nil {
				log.Fatalf("Error reading block from file %s", file.id)
			}
			timestamp, key, _ := ende.DecodeData(opRes.BlockBytes)

			if timestamp == -1 {
				continue
			}

			stringKey := getByteString(key)

			if v, ok := keyDir[stringKey]; ok {
				if v.Timestamp < timestamp {
					keyDir[stringKey] = &BlockAddr{Fid: file.id, Offset: opRes.Offset, Size: len(opRes.BlockBytes), Timestamp: timestamp}
				}
			} else {
				keyDir[stringKey] = &BlockAddr{Fid: file.id, Offset: opRes.Offset, Size: len(opRes.BlockBytes), Timestamp: timestamp}
			}

		}
	}
}

func keyDirChangeOps(done <-chan interface{}) {
	go func() {
		for {
			select {
			case op := <-keyDirChangeOpChannel:
				switch op.op {
				case "add":
					keyDir[op.key] = op.block
					close(op.opDoneChan)
				}
			case <-done:
				return
			}
		}
	}()
}

func getBlockFromMem(key interface{}) <-chan BlockAddr {
	blockChan := make(chan BlockAddr)
	go func() {
		strKey := getByteString(key)
		blockChan <- *keyDir[strKey]
	}()
	return blockChan
}

func putBlockOnMem(blockAddrChan <-chan KeyBlockAddr) <-chan interface{} {
	putDone := make(chan interface{})
	go func() {
		kb := <-blockAddrChan
		keyDirChangeOpChannel <- keyDirChangeOp{op: "add", key: kb.key, block: kb.block, opDoneChan: putDone}
	}()
	return putDone
}
