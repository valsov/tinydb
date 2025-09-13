package storage

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

type RawPage struct {
	PageId uint32
	Data   [PageSize]byte
}

type SlottedPage struct {
	PageId uint32
	Header PageHeader
	Slots  []Slot
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
	PageId    uint32
	SlotIndex uint32
	Header    CellHeader
	Data      []byte
}

type CellHeader struct {
	Size uint32
}

type ValueCellHeader struct {
	Size       uint32
	NullBitset []byte
}
