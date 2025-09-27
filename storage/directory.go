package storage

import (
	"errors"
	"path"
	"sync"
)

var (
	ErrPageNotFound          = errors.New("page not found")
	ErrPageAlreadyExists     = errors.New("page already exists")
	ErrRelationNotExists     = errors.New("relation doesn't exist")
	ErrRelationAlreadyExists = errors.New("relation already exists")
)

type PhysLoc struct {
	File   string
	Offset uint32
}

type PageDirectory struct {
	fileMap  map[string]string // Relation to file
	pageMap  map[PageId]uint32 // Page id to file offset
	rootPath string
	mutex    *sync.RWMutex
}

func NewPageDirectory(rootPath string) *PageDirectory {
	return &PageDirectory{
		fileMap:  map[string]string{},
		pageMap:  map[PageId]uint32{},
		rootPath: rootPath,
		mutex:    &sync.RWMutex{},
	}
}

// table1/
//
//	table1
//	table1_fsm
//	table1_index1
//	table1_index1_fsm
//
// table2/
//
//	table2
//	table2_fsm
//
// mainRel == tableX
// relation == tableX || tableX_indexY || tableX_fsm
func (p *PageDirectory) RegisterFile(mainRel string, relation string) (string, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if _, exists := p.fileMap[relation]; exists {
		return "", ErrRelationAlreadyExists
	}

	path := path.Join(p.rootPath, mainRel, relation)
	p.fileMap[relation] = path
	return path, nil
}

func (p *PageDirectory) GetPageLoc(id PageId) (PhysLoc, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	file, found := p.fileMap[id.Relation]
	if !found {
		return PhysLoc{}, ErrRelationNotExists
	}

	offset, found := p.pageMap[id]
	if !found {
		return PhysLoc{}, ErrPageNotFound
	}
	return PhysLoc{
		File:   file,
		Offset: offset,
	}, nil
}

func (p *PageDirectory) RegisterPage(id PageId, offset uint32) (PhysLoc, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	file, found := p.fileMap[id.Relation]
	if !found {
		return PhysLoc{}, ErrRelationNotExists
	}

	if _, exists := p.pageMap[id]; exists {
		return PhysLoc{}, ErrPageAlreadyExists
	}

	p.pageMap[id] = offset
	return PhysLoc{
		File:   file,
		Offset: offset,
	}, nil
}
