package buffer

import (
	"sync"

	"github.com/tinydb/storage"
)

type BufferPage struct {
	dirty    bool
	pinCount uint32
	latch    *sync.RWMutex
	page     *storage.Page
}
