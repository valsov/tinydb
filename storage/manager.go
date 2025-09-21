package storage

import (
	"errors"
	"os"
	"sync"
)

var (
	ErrPageNotFound        = errors.New("page not found")
	ErrIncompletePageRead  = errors.New("unexpected page read bytes count")
	ErrWriteFailed         = errors.New("page write failed")
	ErrIncompletePageWrite = errors.New("unexpected page write bytes count")
)

type fileWrapper struct {
	*os.File
	mutex *sync.RWMutex
}

type Manager struct {
	pageDir *PageDirectory
	handles map[string]*fileWrapper
	mutex   *sync.Mutex
}

func (m *Manager) GetPage(pageId PageId) (*Page, error) {
	loc, err := m.pageDir.GetPageLoc(pageId)
	if err != nil {
		return nil, err
	}

	file, err := m.getFileHandle(loc.File)
	if err != nil {
		return nil, err
	}

	file.mutex.RLock()
	defer file.mutex.RUnlock()

	buffer := make([]byte, PageSize)
	readCount, err := file.ReadAt(buffer, int64(loc.Offset))
	if err != nil {
		return nil, err
	}
	if readCount != PageSize {
		return nil, ErrIncompletePageRead
	}
	return &Page{
		Id: pageId,
		Location: PhysLoc{
			File:   loc.File,
			Offset: loc.Offset,
		},
		Data: buffer,
	}, nil
}

func (m *Manager) WritePage(page *Page) error {
	file, err := m.getFileHandle(page.Location.File)
	if err != nil {
		return err
	}

	file.mutex.Lock()
	defer file.mutex.Unlock()

	writeCount, err := file.WriteAt(page.Data, int64(page.Location.Offset))
	if err != nil {
		return err
	}
	if writeCount != PageSize {
		return ErrIncompletePageWrite
	}

	return file.Sync()
}

func (m *Manager) getFileHandle(name string) (*fileWrapper, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	file, found := m.handles[name]
	if found {
		return file, nil
	}

	fhandle, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}

	file = &fileWrapper{
		File:  fhandle,
		mutex: &sync.RWMutex{},
	}
	m.handles[name] = file
	return file, nil
}
