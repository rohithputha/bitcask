package cask

import (
	"sync"
)

var fLock map[string]*sync.RWMutex
var dirLockOnKeyMap map[string]*sync.RWMutex
var fMgrLock *sync.RWMutex
var dirLock *sync.RWMutex
var mapLock *sync.Mutex
var once sync.Once

type caskLockConfig struct {
	fileRWLockFid string
	fileLockFid   string
	keyDirRWLock  bool
	keyDirLock    bool
	fileMgrRWLock bool
	fileMgrLock   bool
}

func fileLock(fid string) func(*caskLockConfig) {
	return func(opts *caskLockConfig) {
		opts.fileLockFid = fid
	}
}
func keyDirLock() func(*caskLockConfig) {
	return func(opts *caskLockConfig) {
		opts.keyDirLock = true
	}
}
func fileRWLock(fid string) func(*caskLockConfig) {
	return func(opts *caskLockConfig) {
		opts.fileRWLockFid = fid
	}
}
func keyDirRWLock() func(*caskLockConfig) {
	return func(opts *caskLockConfig) {
		opts.keyDirRWLock = true
	}
}
func fileMgrRWLock() func(*caskLockConfig) {
	return func(opts *caskLockConfig) {
		opts.fileMgrRWLock = true
	}
}
func fileMgrLock() func(config *caskLockConfig) {
	return func(opts *caskLockConfig) {
		opts.fileMgrLock = true
	}
}

func withLocks(f func(), options ...func(*caskLockConfig)) {
	c := &caskLockConfig{}
	for _, option := range options {
		option(c)
	}
	mapLock.Lock()
	if c.fileLockFid != "" {
		if l, ok := fLock[c.fileLockFid]; !ok {
			fLock[c.fileLockFid] = &sync.RWMutex{}
			fLock[c.fileLockFid].Lock()
			defer fLock[c.fileLockFid].Unlock()
		} else {
			l.Lock()
			defer l.Unlock()
		}
	}
	if c.fileRWLockFid != "" {
		if l, ok := fLock[c.fileRWLockFid]; !ok {
			fLock[c.fileRWLockFid] = &sync.RWMutex{}
			fLock[c.fileRWLockFid].RLock()
			defer fLock[c.fileRWLockFid].RUnlock()
		} else {
			l.RLock()
			defer l.RUnlock()
		}
	}
	mapLock.Unlock()

	if c.keyDirLock {
		dirLock.Lock()
		defer dirLock.Unlock()
	}
	if c.keyDirRWLock {
		dirLock.RLock()
		defer dirLock.RUnlock()
	}
	if c.fileMgrLock {
		fMgrLock.Lock()
		defer fMgrLock.Unlock()
	}
	if c.fileMgrRWLock {
		fMgrLock.RLock()
		defer fMgrLock.RUnlock()
	}

	f()
}

func initLocks() {
	once.Do(func() {
		fLock = make(map[string]*sync.RWMutex)
		dirLock = &sync.RWMutex{}
		fMgrLock = &sync.RWMutex{}
		mapLock = &sync.Mutex{}
	})
}
