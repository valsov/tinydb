package storage

type PhysLoc struct {
	File   string
	Offset uint32
}

// file that keeps track of location of relations ->
//   - tableX/indexY => fileABC + root page offset
//   - pageIdXXX => fileABC + offset
//
// in memory -> hashmap
// todo: store pages free space summary somehow -> free space map
type PageDirectory struct {
	fileMap map[string]PhysLoc // Relation to file and root page map
	pageMap map[PageId]PhysLoc // Page id to file offset
}

func (p *PageDirectory) GetRootPageLoc(relation string) (PhysLoc, error) {
	fOffset, found := p.fileMap[relation]
	if !found {
		return PhysLoc{}, ErrPageNotFound
	}
	return fOffset, nil
}

func (p *PageDirectory) GetPageLoc(id PageId) (PhysLoc, error) {
	fOffset, found := p.pageMap[id]
	if !found {
		return PhysLoc{}, ErrPageNotFound
	}
	return fOffset, nil
}
