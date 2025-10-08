package catalog

import (
	"errors"

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

// Rules:
// - null bitsets at the beginning
// - bool fields packed into byte, can be packed along with nullable info bits
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
	for i, field := range layout.Fields {
		info, found := TypesInfoMap[field.Type]
		if !found {
			return Layout{}, ErrUnknownFieldType
		}
		if info.VariableLength {
			return Layout{}, errors.New("variable length fields are not yet implemented")
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
	return data.IsBitSet(f.nullOffset, f.nullIndex, buffer)
}

func (f Field) SetIsNull(isNull bool, buffer []byte) error {
	if !f.Nullable {
		return ErrNotNullable
	}
	return data.WriteBit(isNull, f.nullOffset, f.nullIndex, buffer)
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
		return data.IsBitSet(f.offset, f.packIndex, buffer)
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
	//case StringType:
	//	return f.readString(buffer)
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
		return data.WriteBit(typedBool, f.offset, f.packIndex, buffer)
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
	//case string:
	//	return f.writeString(typedVal, buffer)
	default:
		return ErrUnknownFieldType
	}
}

/*
func (f Field) readString(buffer []byte) (string, error) {
	offset, err := ReadInt16(buffer, f.offset)
	if err != nil {
		return "", err
	}
	strOffset := uint16(offset)

	length, err := ReadInt16(buffer, f.offset+2)
	if err != nil {
		return "", err
	}
	strLen := uint16(length)

	bytes, err := ReadBytes(buffer, strOffset, strLen)
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

// Variable length field write assumes there is enough space to fit value;
// it also assumes, in the case of a length increase that other variable length fields
// after it are at the correct offset to fit new field.
// Whole tuple move in case of legnth increase should be done prior to this operation.
func (f Field) writeString(value string, buffer []byte) error {

}
*/
