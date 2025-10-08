package catalog

import "errors"

var (
	ErrLayoutNotFound = errors.New("layout not found")
)

type RelationData struct {
	layout Layout
	// index
	// constraints
}

// Catalog is the storage of relations layout and index info
type Catalog struct {
	relations map[string]RelationData
}

func NewCatalog() *Catalog {
	return &Catalog{
		relations: map[string]RelationData{},
	}
}

// todo: load all catalog from disk at init time

func (l *Catalog) GetLayout(relation string) (Layout, error) {
	relData, found := l.relations[relation]
	if found {
		return relData.layout, nil
	}

	return Layout{}, ErrLayoutNotFound
}

func (l *Catalog) SetLayout(relation string, layout Layout) error {
	relData := l.relations[relation]
	relData.layout = layout
	l.relations[relation] = relData
	return nil
}
