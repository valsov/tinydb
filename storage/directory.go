package storage

// file that keeps track of location of relations ->
//  - tableX/indexY => fileABC
//  - pageIdXXX => fileABC + offset
// in memory -> hashmap
// todo: store pages free space summary somehow

type PageDirectory struct {
	FileMap map[string]string     // Relation to file map
	PageMap map[uint32]FileOffset // Page id to file offset
}

type FileOffset struct {
	File   string
	Offset uint8
}
