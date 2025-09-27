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

type relationDirectory struct {
	file    string
	pageMap map[uint32]uint32 // Page id to file offset
}

type PageDirectory struct {
	relationMap map[string]relationDirectory // Relation to file and a set of pages
	rootPath    string
	mutex       *sync.RWMutex
}

func NewPageDirectory(rootPath string) *PageDirectory {
	return &PageDirectory{
		relationMap: map[string]relationDirectory{},
		rootPath:    rootPath,
		mutex:       &sync.RWMutex{},
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

	if _, exists := p.relationMap[relation]; exists {
		return "", ErrRelationAlreadyExists
	}

	path := path.Join(p.rootPath, mainRel, relation)
	p.relationMap[relation] = relationDirectory{
		file:    path,
		pageMap: map[uint32]uint32{},
	}
	return path, nil
}

func (p *PageDirectory) UnregisterFile(relation string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	delete(p.relationMap, relation)
	return nil
}

func (p *PageDirectory) GetPageLoc(id PageId) (PhysLoc, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	relation, found := p.relationMap[id.Relation]
	if !found {
		return PhysLoc{}, ErrRelationNotExists
	}

	offset, found := relation.pageMap[id.Id]
	if !found {
		return PhysLoc{}, ErrPageNotFound
	}
	return PhysLoc{
		File:   relation.file,
		Offset: offset,
	}, nil
}

func (p *PageDirectory) RegisterPage(id PageId, offset uint32) (PhysLoc, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	relation, found := p.relationMap[id.Relation]
	if !found {
		return PhysLoc{}, ErrRelationNotExists
	}

	if _, exists := relation.pageMap[id.Id]; exists {
		return PhysLoc{}, ErrPageAlreadyExists
	}

	relation.pageMap[id.Id] = offset
	return PhysLoc{
		File:   relation.file,
		Offset: offset,
	}, nil
}

func (p *PageDirectory) UnregisterPage(id PageId) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	relation, found := p.relationMap[id.Relation]
	if !found {
		return nil
	}

	delete(relation.pageMap, id.Id)
	return nil
}
