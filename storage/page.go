package storage

const (
	PageSize = 4096
)

// todo: after mvp:
// support variable length data
// improve by allowing overflow pages
// free space management (between cells)

type Page struct {
	Id      uint32
	RawData [PageSize]byte
}

type SlottedPage struct {
	Id     uint32
	Header PageHeader
	Slots  []Slot
}

// todo: may need to store more than uint8 is able to for offsets, check
type PageHeader struct {
	PageType uint8 // Flags

	SlotsCount uint8
	FreeSpace  uint8

	SlotsEndOffset uint8
	CellsEndOffset uint8
}

type Slot struct {
	Id         uint32 // May be another type
	Deleted    bool
	CellOffset uint8
}

type Cell struct {
	Id      uint32 // May be another type
	Header  CellHeader
	RawData []byte
}

type CellHeader struct {
	Size       uint32
	NullBitset []byte
}
