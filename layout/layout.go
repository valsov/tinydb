package layout

type Layout struct {
}

type LayoutManager struct {
	tables map[string]Layout
}

func (l *LayoutManager) GetLayout(table string) (Layout, error) {
	panic("todo")
}

func (l *LayoutManager) SetLayout(table string, layout Layout) (Layout, error) {
	panic("todo")
}
