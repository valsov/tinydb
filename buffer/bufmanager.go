package buffer

import (
	"errors"
	"fmt"
	"sync"

	"github.com/tinydb/storage"
)

const (
	maxFrames = 1024 // todo: make config-driven
)

var (
	ErrNoFrameAvailable = errors.New("no available frame for page")
)

type Manager struct {
	store       *storage.Manager
	directory   *storage.PageDirectory
	pages       map[storage.PageId]*BufferPage
	mostRecent  *BufferPage
	leastRecent *BufferPage
	mutex       *sync.Mutex
}

func NewBufferManager(store *storage.Manager, directory *storage.PageDirectory) *Manager {
	return &Manager{
		store:     store,
		directory: directory,
		pages:     make(map[storage.PageId]*BufferPage, maxFrames),
		mutex:     &sync.Mutex{},
	}
}

func (m *Manager) GetPage(pageId storage.PageId) (*BufferPage, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if page, found := m.pages[pageId]; found {
		page.pinCount++
		m.setMostRecent(page)
		return page, nil
	}

	if len(m.pages) == maxFrames {
		ok, err := m.tryEvict()
		if err != nil {
			return nil, fmt.Errorf("page eviction failed: %w", err)
		}
		if !ok {
			return nil, ErrNoFrameAvailable
		}
	}

	page, err := m.loadPage(pageId)
	if err != nil {
		return nil, err
	}
	page.pinCount++
	return page, nil
}

func (m *Manager) ReleasePagePin(page *BufferPage) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	page.pinCount--
	return nil
}

func (m *Manager) loadPage(pageId storage.PageId) (*BufferPage, error) {
	loc, err := m.directory.GetPageLoc(pageId)
	if err != nil {
		return nil, fmt.Errorf("failed to get page location: %w", err)
	}
	page, err := m.store.GetPage(pageId, loc)
	if err != nil {
		return nil, fmt.Errorf("failed to get page from storage: %w", err)
	}

	bufPage := NewBufferPage(page)
	m.pages[pageId] = bufPage

	if m.mostRecent == nil {
		m.mostRecent = bufPage
		m.leastRecent = bufPage
		return bufPage, nil
	}

	m.mostRecent.moreRecent = bufPage
	m.mostRecent = bufPage
	return bufPage, nil
}

func (m *Manager) tryEvict() (bool, error) {
	candidate := m.leastRecent
	var prev *BufferPage
	for candidate != nil {
		if candidate.pinCount != 0 {
			prev = candidate
			candidate = candidate.moreRecent
			continue
		}

		// Evict
		candidate.Latch.Lock()
		defer candidate.Latch.Unlock()

		if candidate.dirty {
			err := m.store.WritePage(candidate.Page)
			if err != nil {
				return false, fmt.Errorf("dirty page write for eviction failed: %w", err)
			}
		}

		// It is assumed that prev is only nil if page == m.mostRecent
		delete(m.pages, candidate.Page.Id)
		if m.leastRecent == candidate {
			m.leastRecent = candidate.moreRecent
		} else if m.mostRecent == candidate {
			prev.moreRecent = nil
			m.mostRecent = prev
		} else {
			prev.moreRecent = candidate.moreRecent
		}
		return true, nil
	}

	return false, nil
}

func (m *Manager) setMostRecent(page *BufferPage) {
	prevMostRecent := m.mostRecent
	if prevMostRecent == page {
		// Already most recent
		return
	}

	if m.leastRecent == page {
		m.leastRecent = page.moreRecent
	}
	prevMostRecent.moreRecent = page
	m.mostRecent = page
}
