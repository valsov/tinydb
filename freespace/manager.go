package freespace

import (
	"errors"

	"github.com/tinydb/storage"
)

var (
	ErrRelationNotExists = errors.New("relation doesn't exist")
	ErrNoSpace           = errors.New("no space available for requested size")
)

type FreeSpaceManager struct {
	relationsMap map[string]freeSpaceMap
}

func (f *FreeSpaceManager) Init(relation string) {
	f.relationsMap[relation] = freeSpaceMap{}
}

func (f *FreeSpaceManager) GetFreePageId(relation string, reqSize uint16) (storage.PageId, error) {
	fsm, found := f.relationsMap[relation]
	if !found {
		return storage.PageId{}, ErrRelationNotExists
	}

	id, found := fsm.getMatch(reqSize)
	if !found {
		return storage.PageId{}, ErrNoSpace
	}

	return storage.PageId{
		Id:       id,
		Relation: relation,
	}, nil
}
