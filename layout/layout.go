package layout

import (
	"errors"
)

var (
	ErrLayoutNotFound = errors.New("layout not found")
	ErrFieldNotFound  = errors.New("field not found")
)

type LayoutManager struct {
	tables map[string]Layout
}

func (l *LayoutManager) GetLayout(table string) (Layout, error) {
	layout, found := l.tables[table]
	if !found {
		return Layout{}, ErrLayoutNotFound
	}
	return layout, nil
}

func (l *LayoutManager) SetLayout(table string, layout Layout) error {
	l.tables[table] = layout
	return nil
}

type Layout struct {
	Fields []Field
}

func NewLayout(fields []Field) Layout {
	// todo: calculate private fields
	panic("todo")
}

func (l *Layout) GetField(name string) (Field, error) {
	for _, field := range l.Fields {
		if field.Name == name {
			return field, nil
		}
	}
	return Field{}, ErrFieldNotFound
}

type Field struct {
	Name       string
	Type       FieldType
	offset     uint16
	nullOffset uint16 // Where to find null info storing byte
	nullIndex  uint8  // At which position to look in the null info byte
	// not needed? => fieldLayoutIndex uint8  // Store position in layout for fast subsequent lookup
}

func (l *Field) IsNull(buffer []byte) (bool, error) {
	nullBitset, err := ReadByte(buffer, l.nullOffset)
	if err != nil {
		return false, err
	}
	return (nullBitset & (1 << l.nullIndex)) != 0, nil
}

func (l *Field) SetIsNull(isNull bool, buffer []byte) error {
	nullBitset, err := ReadByte(buffer, l.nullOffset)
	if err != nil {
		return err
	}

	if isNull {
		if (nullBitset & (1 << l.nullIndex)) != 0 {
			// Unset bit
			nullBitset ^= 1 << l.nullIndex
		}
	} else {
		// Set bit
		nullBitset |= 1 << l.nullIndex
	}

	return WriteByte(nullBitset, buffer, l.nullOffset)
}
