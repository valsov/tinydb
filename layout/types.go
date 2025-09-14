package layout

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	BoolType    FieldType = "bool"
	Int8Type    FieldType = "int8"
	Int16Type   FieldType = "int16"
	Int32Type   FieldType = "int32"
	Int64Type   FieldType = "int64"
	Float32Type FieldType = "float32"
	Float64Type FieldType = "float64"
	StringType  FieldType = "string"
)

var (
	TypesInfoMap = map[FieldType]FieldTypeInfo{
		BoolType: {
			Packable: true,
		},
		Int8Type: {
			Size: 1,
		},
		Int16Type: {
			Size: 2,
		},
		Int32Type: {
			Size: 4,
		},
		Int64Type: {
			Size: 8,
		},
		Float32Type: {
			Size: 4,
		},
		Float64Type: {
			Size: 8,
		},
		StringType: {
			Size:           10, // Offset (uint16) + length (uint64) -> 2 + 8 = 10
			VariableLength: true,
		},
	}
)

var (
	ErrOutOfBounds = errors.New("offset and type length cause out of buffer bounds")
)

type FieldType string

type FieldTypeInfo struct {
	Size           uint16
	VariableLength bool
	Packable       bool
}

func ReadByte(buffer []byte, offset uint16) (byte, error) {
	if int(offset) >= len(buffer) {
		return 0, ErrOutOfBounds
	}
	return buffer[offset], nil
}

func WriteByte(value byte, buffer []byte, offset uint16) error {
	if int(offset) >= len(buffer) {
		return ErrOutOfBounds
	}
	buffer[offset] = value
	return nil
}

func ReadInt16(buffer []byte, offset uint16) (int16, error) {
	if int(offset)+1 >= len(buffer) {
		return 0, ErrOutOfBounds
	}

	var value int16
	err := binary.Read(bytes.NewReader(buffer[offset:offset+2]), binary.BigEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("value deserialization error: %w", err)
	}
	return value, nil
}

func WriteInt16(value int16, buffer []byte, offset uint16) error {
	if int(offset)+1 >= len(buffer) {
		return ErrOutOfBounds
	}

	bBuffer := bytes.NewBuffer(buffer[offset : offset+2])
	err := binary.Write(bBuffer, binary.BigEndian, value)
	if err != nil {
		return fmt.Errorf("value serialization error: %w", err)
	}
	return nil
}

func ReadInt32(buffer []byte, offset uint16) (int32, error) {
	if int(offset)+3 >= len(buffer) {
		return 0, ErrOutOfBounds
	}

	var value int32
	err := binary.Read(bytes.NewReader(buffer[offset:offset+4]), binary.BigEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("value deserialization error: %w", err)
	}
	return value, nil
}

func WriteInt32(value int32, buffer []byte, offset uint16) error {
	if int(offset)+3 >= len(buffer) {
		return ErrOutOfBounds
	}

	bBuffer := bytes.NewBuffer(buffer[offset : offset+4])
	err := binary.Write(bBuffer, binary.BigEndian, value)
	if err != nil {
		return fmt.Errorf("value serialization error: %w", err)
	}
	return nil
}

func ReadInt64(buffer []byte, offset uint16) (int64, error) {
	if int(offset)+7 >= len(buffer) {
		return 0, ErrOutOfBounds
	}

	var value int64
	err := binary.Read(bytes.NewReader(buffer[offset:offset+8]), binary.BigEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("value deserialization error: %w", err)
	}
	return value, nil
}

func WriteInt64(value int64, buffer []byte, offset uint16) error {
	if int(offset)+7 >= len(buffer) {
		return ErrOutOfBounds
	}

	bBuffer := bytes.NewBuffer(buffer[offset : offset+8])
	err := binary.Write(bBuffer, binary.BigEndian, value)
	if err != nil {
		return fmt.Errorf("value serialization error: %w", err)
	}
	return nil
}

func ReadFloat32(buffer []byte, offset uint16) (float32, error) {
	if int(offset)+3 >= len(buffer) {
		return 0, ErrOutOfBounds
	}

	var value float32
	err := binary.Read(bytes.NewReader(buffer[offset:offset+4]), binary.BigEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("value deserialization error: %w", err)
	}
	return value, nil
}

func WriteFloat32(value float32, buffer []byte, offset uint16) error {
	if int(offset)+3 >= len(buffer) {
		return ErrOutOfBounds
	}

	bBuffer := bytes.NewBuffer(buffer[offset : offset+4])
	err := binary.Write(bBuffer, binary.BigEndian, value)
	if err != nil {
		return fmt.Errorf("value serialization error: %w", err)
	}
	return nil
}

func ReadFloat64(buffer []byte, offset uint16) (float64, error) {
	if int(offset)+7 >= len(buffer) {
		return 0, ErrOutOfBounds
	}

	var value float64
	err := binary.Read(bytes.NewReader(buffer[offset:offset+8]), binary.BigEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("value deserialization error: %w", err)
	}
	return value, nil
}

func WriteFloat64(value float64, buffer []byte, offset uint16) error {
	if int(offset)+7 >= len(buffer) {
		return ErrOutOfBounds
	}

	bBuffer := bytes.NewBuffer(buffer[offset : offset+8])
	err := binary.Write(bBuffer, binary.BigEndian, value)
	if err != nil {
		return fmt.Errorf("value serialization error: %w", err)
	}
	return nil
}

func ReadBytes(buffer []byte, offset uint16, length uint16) ([]byte, error) {
	if int(offset)+int(length)-1 >= len(buffer) {
		return nil, ErrOutOfBounds
	}

	value := make([]byte, length)
	copy(buffer[offset:offset+length], value)
	return value, nil
}

func WriteBytes(value []byte, buffer []byte, offset uint16) error {
	if int(offset)+len(value)-1 >= len(buffer) {
		return ErrOutOfBounds
	}

	intOffset := int(offset)
	for i, byte := range value {
		buffer[intOffset+i] = byte
	}
	return nil
}
