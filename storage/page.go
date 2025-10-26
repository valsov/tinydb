package storage

import (
	"fmt"

	"github.com/tinydb/data"
)

const (
	PageSize         = 4096
	SlotsStartOffset = 9 // After page header
	SlotSize         = 5
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
	Id   TupleId
	Size uint16
}

type TupleId struct {
	SlotIndex uint16
	Offset    uint16
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

func (p *Page) WritePageHeader() error {
	offset := uint16(0)
	if err := data.WriteByte(p.Header.PageType, p.Data, offset); err != nil {
		return err
	}

	offset++
	if err := data.WriteUint16(p.Header.SlotsCount, p.Data, offset); err != nil {
		return err
	}

	offset += 2
	if err := data.WriteUint16(p.Header.FreeSpace, p.Data, offset); err != nil {
		return err
	}

	offset += 2
	if err := data.WriteUint16(p.Header.SlotsEndOffset, p.Data, offset); err != nil {
		return err
	}

	offset += 2
	if err := data.WriteUint16(p.Header.CellsEndOffset, p.Data, offset); err != nil {
		return err
	}

	return nil
}

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

	offset++
	cellOffset, err := data.ReadUint16(p.Data, offset)
	if err != nil {
		return Slot{}, err
	}
	return Slot{
		Deleted:    false,
		CellOffset: cellOffset,
	}, nil
}

func (p *Page) WriteSlot(slot Slot, offset uint16) (uint16, error) {
	deletedByte := byte(0)
	if slot.Deleted {
		deletedByte = 1
	}
	if err := data.WriteByte(deletedByte, p.Data, offset); err != nil {
		return 0, err
	}

	offset++
	if err := data.WriteUint16(slot.CellOffset, p.Data, offset); err != nil {
		return 0, err
	}

	// Return slot end offset
	return offset + 2, nil
}

func (p *Page) SetSlotDeleted(offset uint16) error {
	return data.WriteByte(1, p.Data, offset)
}

func (p *Page) ReadCell(offset uint16) (Cell, error) {
	slotIndex, err := data.ReadUint16(p.Data, offset)
	if err != nil {
		return Cell{}, err
	}

	offset += 2
	cellOffset, err := data.ReadUint16(p.Data, offset)
	if err != nil {
		return Cell{}, err
	}

	offset += 2
	size, err := data.ReadUint16(p.Data, offset)
	if err != nil {
		return Cell{}, err
	}
	return Cell{
		Id: TupleId{
			SlotIndex: slotIndex,
			Offset:    cellOffset,
		},
		Size: size,
	}, nil
}

func (p *Page) WriteCell(cell Cell, offset uint16) error {
	if err := data.WriteUint16(cell.Id.SlotIndex, p.Data, offset); err != nil {
		return err
	}

	offset += 2
	if err := data.WriteUint16(cell.Id.Offset, p.Data, offset); err != nil {
		return err
	}

	offset += 2
	if err := data.WriteUint16(cell.Size, p.Data, offset); err != nil {
		return err
	}

	return nil
}
