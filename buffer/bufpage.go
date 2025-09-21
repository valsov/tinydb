package buffer

import (
	"sync"

	"github.com/tinydb/storage"
)

type BufferPage struct {
	dirty    bool
	pinCount int
	latch    *sync.RWMutex
	page     *storage.Page
}
