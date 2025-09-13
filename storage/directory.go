package storage

// file that keeps track of location of relations ->
//  - tableX/indexY => fileABC + root page offset
//  - pageIdXXX => fileABC + offset
// in memory -> hashmap
// todo: store pages free space summary somehow -> free space map

type PageDirectory struct {
	FileMap map[string]FileOffset // Relation to file and root page map
	PageMap map[uint32]FileOffset // Page id to file offset
}

type FileOffset struct {
	File   string
	Offset uint8
}
