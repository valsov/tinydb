package layout

import (
	"errors"
)

var (
	ErrLayoutNotFound   = errors.New("layout not found")
	ErrFieldNotFound    = errors.New("field not found")
	ErrUnknownFieldType = errors.New("unknown field type")
)

type LayoutManager struct {
	tables map[string]Layout
}

func NewLayoutManager() *LayoutManager {
	return &LayoutManager{
		tables: map[string]Layout{},
	}
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

// Rules:
// - null bitsets at the beginning
// - bool fields packed into byte,
// - variable fields are represented as fixed size values first: by offset and size
//   - variable part is stored at the end, pointed to by offset in fixed size part
func NewLayout(fields []Field) (Layout, error) {
	layout := Layout{
		Fields: make([]Field, len(fields)),
	}
	if len(fields) == 0 {
		return layout, nil
	}

	// Null bitsets management
	var bitsetOffset uint16
	var bitsetIndex uint8
	for i, field := range fields {
		if field.Nullable {
			field.nullOffset = bitsetOffset
			field.nullIndex = bitsetIndex

			if bitsetIndex == 7 {
				// Reached capacity, use another bitset
				bitsetOffset++
				bitsetIndex = 0
			} else {
				bitsetIndex++
			}
		}
		layout.Fields[i] = field
	}

	// Set offsets
	var offset uint16
	if bitsetIndex == 0 {
		offset = bitsetOffset
	} else {
		offset = bitsetOffset + 1
	}

	bitsetOffset = 0
	bitsetIndex = 0
	for i, field := range layout.Fields {
		info, found := TypesInfoMap[field.Type]
		if !found {
			return Layout{}, ErrUnknownFieldType
		}

		if info.Packable {
			if bitsetOffset == 0 && i != 0 {
				// Create new bitmap
				bitsetOffset = offset
				offset++
			}
			field.offset = bitsetOffset
			field.packIndex = bitsetIndex
			if bitsetIndex == 7 {
				// Reached capacity, next packed data will use another bitset
				bitsetOffset = 0
				bitsetIndex = 0
			} else {
				bitsetIndex++
			}
		} else {
			field.offset = offset
			offset += info.Size
		}

		layout.Fields[i] = field
	}

	return layout, nil
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
	Name     string
	Type     FieldType
	Nullable bool

	offset     uint16
	packIndex  uint8  // In the case of packed value, identifies the bit index to look at
	nullOffset uint16 // Where to find null info storing bitset
	nullIndex  uint8  // At which position to look in the null info bitset
}
