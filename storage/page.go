package storage

import (
	"fmt"

	"github.com/tinydb/data"
)

const (
	PageSize = 4096
)

const (
	PageTypeRoot = 1 << iota
	PageTypeInternal
	PageTypeLeaf
	PageTypeValues
)

// todo: after mvp:
// improve by allowing overflow pages
// free space management (between cells)

type PageId struct {
	Id       uint32
	Relation string
}

func (p PageId) String() string {
	return fmt.Sprintf("%s:%d", p.Relation, p.Id)
}

type Page struct {
	Id     PageId
	Header PageHeader
	Data   [PageSize]byte
}

func (p *Page) LoadPageHeader() error {
	sBytes := p.Data[:]
	offset := uint16(0)
	pageType, err := data.ReadByte(sBytes, offset)
	if err != nil {
		return err
	}
	offset++

	slotsCount, err := data.ReadInt16(sBytes, offset)
	if err != nil {
		return err
	}
	offset += 2

	freeSpace, err := data.ReadInt16(sBytes, offset)
	if err != nil {
		return err
	}
	offset += 2

	slotsEndOffset, err := data.ReadInt16(sBytes, offset)
	if err != nil {
		return err
	}
	offset += 2

	cellsEndOffset, err := data.ReadInt16(sBytes, offset)
	if err != nil {
		return err
	}

	p.Header = PageHeader{
		PageType:       pageType,
		SlotsCount:     uint16(slotsCount),
		FreeSpace:      uint16(freeSpace),
		SlotsEndOffset: uint16(slotsEndOffset),
		CellsEndOffset: uint16(cellsEndOffset),
	}
	return nil
}

func (p *Page) ReadSlot(offset uint16) (Slot, error) {
	panic("todo")
}

func (p *Page) ReadCell(offset uint16) (Cell, error) {
	panic("todo")
}

type PageHeader struct {
	PageType uint8 // Flags

	SlotsCount uint16
	FreeSpace  uint16

	SlotsEndOffset uint16
	CellsEndOffset uint16
}

type Slot struct {
	Index      uint16
	Deleted    bool
	CellOffset uint16
}

type Cell struct {
	SlotOffset uint16
	Header     CellHeader
}

type CellHeader struct {
	Size uint32
}
