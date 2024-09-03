package cask

func TestFileCreation() {
	// Test file creation
	done := make(chan interface{})
	defer close(done)
	initDbFileManager(done)
	putNewFile()

}

func TestKeyDirPut() {
	done := make(chan interface{})
	defer close(done)
	initDbFileManager(done)
	//initKeyDir(done)
	bch := make(chan []byte)
	defer close(bch)
	select {
	case bch <- []byte("key1"):
	case <-done:
		return
	case <-putBlockOnFile(bch):
	}

}

func TestbytesPut() {
	done := make(chan interface{})
	defer close(done)
	initDbFileManager(done)
	initKeyDir(done)
	file := putNewFile().DbFiles[0]
	file.makeFileActive()
	//<-file.appendBlock([]byte("key1"))
}
