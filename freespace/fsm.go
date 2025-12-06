package freespace

type fsmNode struct {
	id            *uint32 // Nil for non-leaf nodes
	maxSpace      uint16
	childrenCount uint32
	parent        *fsmNode
	left          *fsmNode
	right         *fsmNode
}

type freeSpaceMap struct {
	root      *fsmNode
	leafNodes map[uint32]*fsmNode
}

func newFreeSpaceMap() *freeSpaceMap {
	return &freeSpaceMap{
		leafNodes: map[uint32]*fsmNode{},
	}
}

func (f *freeSpaceMap) getMatch(size uint16) (uint32, bool) {
	node := f.root
	if node == nil || node.maxSpace < size {
		return 0, false
	}

	for node.id == nil {
		if node.left != nil && node.left.maxSpace > size {
			node = node.left
		} else if node.right != nil {
			node = node.right
		}
	}

	return *node.id, true
}

func (f *freeSpaceMap) setFreeSpace(id uint32, free uint16) {
	if f.root == nil {
		f.root = &fsmNode{
			id:       &id,
			maxSpace: free,
		}
		f.leafNodes[id] = f.root
		return
	}

	node, found := f.leafNodes[id]
	if !found {
		node = f.createNode(id, free)
		f.leafNodes[id] = node
	}

	// Update parent chain
	for node.parent != nil {
		if node.parent.left != nil && (node.parent.right == nil || node.parent.left.maxSpace > node.parent.right.maxSpace) {
			node.maxSpace = node.parent.left.maxSpace
		} else if node.parent.right != nil && (node.parent.left == nil || node.parent.right.maxSpace > node.parent.left.maxSpace) {
			node.maxSpace = node.parent.right.maxSpace
		} else {
			// No need to up date higher
			break
		}

		node = node.parent
	}
}

func (f *freeSpaceMap) createNode(id uint32, free uint16) *fsmNode {
	node := f.root
	for {
		if node.left == nil && node.right == nil {
			// Root without children
			if node.parent == nil {
				// Create a new root and put existing node as well as new node under it
				f.root = &fsmNode{
					childrenCount: 2,
					left:          node,
				}
				node.parent = f.root
				f.root.right = &fsmNode{
					id:       &id,
					maxSpace: free,
					parent:   f.root,
				}
				return f.root.right
			}

			// Node without children
			// Create new parent node to store both children
			newParent := &fsmNode{
				childrenCount: 2,
				parent:        node.parent,
				left:          node,
			}
			if node.parent.left == node {
				node.parent.left = newParent
			} else {
				node.parent.right = newParent
			}
			node.parent = newParent
			newParent.right = &fsmNode{
				id:       &id,
				maxSpace: free,
				parent:   newParent,
			}
			return newParent.right
		}

		if node.left == nil {
			// Create new child on empty left side
			node.childrenCount++
			node.left = &fsmNode{
				id:       &id,
				maxSpace: free,
				parent:   node,
			}
			return node.left
		}

		if node.right == nil {
			// Create new child on empty right side
			node.childrenCount++
			node.right = &fsmNode{
				id:       &id,
				maxSpace: free,
				parent:   node,
			}
			return node.right
		}

		if node.left.childrenCount < node.right.childrenCount {
			node.childrenCount++
			node = node.left
		} else {
			node.childrenCount++
			node = node.right
		}
	}
}
