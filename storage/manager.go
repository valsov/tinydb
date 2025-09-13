package storage

import "errors"

var (
	ErrPageNotFound = errors.New("page not found")
	ErrWriteFailed  = errors.New("page write failed")
)

type Manager struct {
}

func (s *Manager) GetPage(relation string, pageId uint32) (*RawPage, error) {
	panic("todo")
}

func (s *Manager) WritePage(page *RawPage, relation string) error {
	panic("todo")
}
