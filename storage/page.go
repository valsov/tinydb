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

type PageId struct {
	Id       uint32
	Relation string
}

func (p PageId) String() string {
	return fmt.Sprintf("%s:%d", p.Relation, p.Id)
}

type Page struct {
	Id       PageId
	Location PhysLoc
	Header   PageHeader
	Data     []byte
}

func (p *Page) LoadPageHeader() error {
	offset := uint16(0)
	pageType, err := data.ReadByte(p.Data, offset)
	if err != nil {
		return err
	}
	offset++

	slotsCount, err := data.ReadUint16(p.Data, offset)
	if err != nil {
		return err
	}
	offset += 2

	freeSpace, err := data.ReadUint16(p.Data, offset)
	if err != nil {
		return err
	}
	offset += 2

	slotsEndOffset, err := data.ReadUint16(p.Data, offset)
	if err != nil {
		return err
	}
	offset += 2

	cellsEndOffset, err := data.ReadUint16(p.Data, offset)
	if err != nil {
		return err
	}

	p.Header = PageHeader{
		PageType:       pageType,
		SlotsCount:     slotsCount,
		FreeSpace:      freeSpace,
		SlotsEndOffset: slotsEndOffset,
		CellsEndOffset: cellsEndOffset,
	}
	return nil
}

// todo: write methods
func (p *Page) ReadSlot(offset uint16) (Slot, error) {
	deletedByte, err := data.ReadByte(p.Data, offset)
	if err != nil {
		return Slot{}, err
	}

	deleted := (deletedByte & 1) != 0
	if deleted {
		return Slot{
			Deleted: true,
		}, nil
	}

	cellOffset, err := data.ReadUint16(p.Data, offset)
	if err != nil {
		return Slot{}, err
	}
	return Slot{
		Deleted:    false,
		CellOffset: cellOffset,
	}, nil
}

func (p *Page) ReadCell(offset uint16) (Cell, error) {
	size, err := data.ReadUint16(p.Data, offset)
	if err != nil {
		return Cell{}, err
	}
	return Cell{
		Header: CellHeader{
			Size: size,
		},
	}, nil
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
	Size uint16
}
