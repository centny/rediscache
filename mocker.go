package rediscache

import (
	"fmt"
	"sync"
)

var ErrMock = fmt.Errorf("mock error")

var mocking = false
var mockTrigger = map[string]int64{}
var mockRunned = map[string]int64{}
var mockRunnedLck = sync.RWMutex{}

func mockerCheck(key string) (err error) {
	if mocking {
		mockRunnedLck.Lock()
		mockRunned[key]++
		if mockTrigger[key] < 0 || mockTrigger[key] == mockRunned[key] {
			err = ErrMock
		}
		mockRunnedLck.Unlock()
	}
	return
}

func MockerStart() {
	mocking = true
}

func MockerStop() {
	MockerClear()
	mocking = false
}

func MockerClear() {
	mockRunnedLck.Lock()
	mockTrigger = map[string]int64{}
	mockRunned = map[string]int64{}
	mockRunnedLck.Unlock()
}

func MockerSet(key string, v int64) {
	mockRunnedLck.Lock()
	mockTrigger[key] = v
	mockRunnedLck.Unlock()
}
