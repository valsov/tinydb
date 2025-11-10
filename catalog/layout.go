package catalog

import (
	"errors"
	"time"

	"github.com/tinydb/data"
)

var (
	ErrFieldNotFound    = errors.New("field not found")
	ErrUnknownFieldType = errors.New("unknown field type")
	ErrWrongFieldType   = errors.New("wrong field type")
	ErrNotNullable      = errors.New("cannot set field null status since it isn't nullable")
)

type Layout struct {
	Fields []Field
}

// Layout rules:
//  1. All null bitsets are placed at the beginning.
//  2. Boolean fields are packed into byte, they can be packed along with nullable info bits.
//  3. Variable fields are represented with fixed size values first: metadata.
//     They are stored at the end to allow fast lookup for offset management.
//     Variable section of variable fields are stored after all fixed size items, pointed to by offset and length in metadata.
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
	for i, f := range fields {
		field := Field{
			Name:     f.Name,
			Type:     f.Type,
			Nullable: f.Nullable,
		}
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

	newBitsetRequired := false
	varLenFields := []struct {
		field Field
		size  uint16
	}{}
	for i, field := range layout.Fields {
		info, found := TypesInfoMap[field.Type]
		if !found {
			return Layout{}, ErrUnknownFieldType
		}

		if info.VariableLength {
			// Store for special processing
			varLenFields = append(varLenFields, struct {
				field Field
				size  uint16
			}{
				field: field,
				size:  info.Size,
			})
			continue
		}

		if info.Packable {
			if newBitsetRequired {
				// Create new bitset
				bitsetOffset = offset
				bitsetIndex = 0
				offset++
				newBitsetRequired = false
			}
			field.offset = bitsetOffset
			field.packIndex = bitsetIndex
			field.packed = true
			if bitsetIndex == 7 {
				// Reached capacity, next packed data will use another bitset
				newBitsetRequired = true
			} else {
				bitsetIndex++
			}
		} else {
			field.offset = offset
			offset += info.Size
		}

		layout.Fields[i] = field
	}

	// Put all variable length metadata fields at the end, their variable length value will be stored after those
	for i, varLenField := range varLenFields {
		varLenField.field.offset = offset
		layout.Fields[i] = varLenField.field
		offset += varLenField.size
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
	packed     bool
	packIndex  uint8  // In the case of packed value, identifies the bit index to look at
	nullOffset uint16 // Where to find null info storing bitset
	nullIndex  uint8  // At which position to look in the null info bitset
}

func (f Field) IsNull(buffer []byte) (bool, error) {
	if !f.Nullable {
		return false, nil
	}
	return data.IsBitSet(buffer, f.nullOffset, f.nullIndex)
}

func (f Field) SetIsNull(isNull bool, buffer []byte) error {
	if !f.Nullable {
		return ErrNotNullable
	}
	return data.WriteBit(isNull, buffer, f.nullOffset, f.nullIndex)
}

func (f Field) Read(buffer []byte) (any, error) {
	isNull, err := f.IsNull(buffer)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}

	if f.packed {
		return data.IsBitSet(buffer, f.offset, f.packIndex)
	}

	switch f.Type {
	case Int8Type:
		b, err := data.ReadByte(buffer, f.offset)
		if err != nil {
			return nil, err
		}
		return int8(b), nil
	case Int16Type:
		return data.ReadInt16(buffer, f.offset)
	case Int32Type:
		return data.ReadInt32(buffer, f.offset)
	case Int64Type:
		return data.ReadInt64(buffer, f.offset)
	case Float32Type:
		return data.ReadFloat32(buffer, f.offset)
	case Float64Type:
		return data.ReadFloat64(buffer, f.offset)
	case DatetimeType:
		unixEpoch, err := data.ReadInt64(buffer, f.offset)
		if err != nil {
			return nil, err
		}
		return time.Unix(unixEpoch, 0), nil
	case StringType:
		return readString(buffer, f.offset)
	default:
		return nil, ErrUnknownFieldType
	}
}

func (f Field) Write(value any, buffer []byte) error {
	if f.packed {
		typedBool, ok := value.(bool)
		if !ok {
			return ErrWrongFieldType
		}
		return data.WriteBit(typedBool, buffer, f.offset, f.packIndex)
	}

	switch typedVal := value.(type) {
	case int8:
		return data.WriteByte(byte(typedVal), buffer, f.offset)
	case int16:
		return data.WriteInt16(typedVal, buffer, f.offset)
	case int32:
		return data.WriteInt32(typedVal, buffer, f.offset)
	case int64:
		return data.WriteInt64(typedVal, buffer, f.offset)
	case float32:
		return data.WriteFloat32(typedVal, buffer, f.offset)
	case float64:
		return data.WriteFloat64(typedVal, buffer, f.offset)
	case time.Time:
		return data.WriteInt64(typedVal.Unix(), buffer, f.offset)
	case WriteStringData:
		return writeString(typedVal, buffer, f)
	default:
		return ErrUnknownFieldType
	}
}
