package buffer

import "sync"

type BufferPage struct {
	Dirty        bool
	PermanentPin bool // For schema and root pages
	PinCount     uint32
	Latch        *sync.RWMutex
}
