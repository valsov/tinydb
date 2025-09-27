package storage

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
)

const (
	filePermissions = 0o740 // rwx r-- ---
)

var (
	ErrIncompletePageRead  = errors.New("unexpected page read bytes count")
	ErrIncompletePageWrite = errors.New("unexpected page write bytes count")
	ErrFileAlreadyExists   = errors.New("file already exists")
)

type fileWrapper struct {
	*os.File
	mutex *sync.RWMutex
}

type Manager struct {
	handles map[string]*fileWrapper
	mutex   *sync.Mutex
}

func NewStorageManager() *Manager {
	return &Manager{
		handles: map[string]*fileWrapper{},
		mutex:   &sync.Mutex{},
	}
}

func (m *Manager) GetPage(pageId PageId, location PhysLoc) (*Page, error) {
	file, err := m.getFileHandle(location.File)
	if err != nil {
		return nil, err
	}

	file.mutex.RLock()
	defer file.mutex.RUnlock()

	buffer := make([]byte, PageSize)
	readCount, err := file.ReadAt(buffer, int64(location.Offset))
	if err != nil {
		return nil, err
	}
	if readCount != PageSize {
		return nil, ErrIncompletePageRead
	}
	return &Page{
		Id:       pageId,
		Location: location,
		Data:     buffer,
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

func (m *Manager) CreateFile(fpath string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, found := m.handles[fpath]; found {
		return ErrFileAlreadyExists
	}

	dirs := path.Dir(fpath)
	if dirs != "." && dirs != "/" {
		err := os.MkdirAll(dirs, os.ModePerm)
		if err != nil {
			return fmt.Errorf("directories creation failed: %w", err)
		}
	}

	fhandle, err := os.OpenFile(fpath, os.O_RDWR|os.O_CREATE|os.O_EXCL, filePermissions)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrFileAlreadyExists
		}
		return err
	}

	file := &fileWrapper{
		File:  fhandle,
		mutex: &sync.RWMutex{},
	}
	m.handles[fpath] = file
	return nil
}

func (m *Manager) DeleteFile(fpath string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if file, found := m.handles[fpath]; found {
		file.mutex.Lock()
		defer file.mutex.Unlock()

		if err := file.Close(); err != nil {
			return fmt.Errorf("failed to close file handle: %w", err)
		}

		if err := os.Remove(fpath); err != nil {
			return fmt.Errorf("failed to delete file: %w", err)
		}

		delete(m.handles, fpath)
		return nil
	}

	if err := os.Remove(fpath); err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}
	return nil
}

func (m *Manager) getFileHandle(path string) (*fileWrapper, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	file, found := m.handles[path]
	if found {
		return file, nil
	}

	fhandle, err := os.OpenFile(path, os.O_RDWR, filePermissions)
	if err != nil {
		return nil, err
	}

	file = &fileWrapper{
		File:  fhandle,
		mutex: &sync.RWMutex{},
	}
	m.handles[path] = file
	return file, nil
}
