package buffer

import (
	"errors"

	"github.com/tinydb/storage"
)

var (
	ErrPageReadLocked  = errors.New("page locked for read")
	ErrPageWriteLocked = errors.New("page locked for write")
)

type Manager struct {
}

func (m *Manager) RequestPageRead(relation string, pageId uint32) (*storage.Page, error) {
	panic("todo")
}

func (m *Manager) RequestPageWrite(relation string, pageId uint32) (*storage.Page, error) {
	panic("todo")
}

func (m *Manager) SetDirty(page *storage.Page) error {
	panic("todo")
}

func (m *Manager) ReleasePage(page *storage.Page) error {
	panic("todo")
}
