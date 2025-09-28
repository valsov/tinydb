package buffer

import (
	"sync"

	"github.com/tinydb/storage"
)

type BufferPage struct {
	Page  *storage.Page
	Latch *sync.RWMutex

	dirty      bool
	pinCount   int
	moreRecent *BufferPage
}

func NewBufferPage(page *storage.Page) *BufferPage {
	return &BufferPage{
		Page:  page,
		Latch: &sync.RWMutex{},
		dirty: false,
	}
}

func (p *BufferPage) SetDirty() {
	p.dirty = true
}
