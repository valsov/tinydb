package data

import (
	"encoding/binary"
	"errors"
	"math"
)

var (
	ErrOutOfBounds = errors.New("offset and type length cause out of buffer bounds")
)

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
	return int16(binary.BigEndian.Uint16(buffer[offset : offset+2])), nil
}

func WriteInt16(value int16, buffer []byte, offset uint16) error {
	if int(offset)+1 >= len(buffer) {
		return ErrOutOfBounds
	}
	binary.BigEndian.PutUint16(buffer[offset:offset+2], uint16(value))
	return nil
}

func ReadInt32(buffer []byte, offset uint16) (int32, error) {
	if int(offset)+3 >= len(buffer) {
		return 0, ErrOutOfBounds
	}
	return int32(binary.BigEndian.Uint32(buffer[offset : offset+4])), nil
}

func WriteInt32(value int32, buffer []byte, offset uint16) error {
	if int(offset)+3 >= len(buffer) {
		return ErrOutOfBounds
	}
	binary.BigEndian.PutUint32(buffer[offset:offset+4], uint32(value))
	return nil
}

func ReadInt64(buffer []byte, offset uint16) (int64, error) {
	if int(offset)+7 >= len(buffer) {
		return 0, ErrOutOfBounds
	}
	return int64(binary.BigEndian.Uint64(buffer[offset : offset+8])), nil
}

func WriteInt64(value int64, buffer []byte, offset uint16) error {
	if int(offset)+7 >= len(buffer) {
		return ErrOutOfBounds
	}
	binary.BigEndian.PutUint64(buffer[offset:offset+8], uint64(value))
	return nil
}

func ReadFloat32(buffer []byte, offset uint16) (float32, error) {
	if int(offset)+3 >= len(buffer) {
		return 0, ErrOutOfBounds
	}
	return math.Float32frombits(binary.BigEndian.Uint32(buffer[offset : offset+4])), nil
}

func WriteFloat32(value float32, buffer []byte, offset uint16) error {
	if int(offset)+3 >= len(buffer) {
		return ErrOutOfBounds
	}
	binary.BigEndian.PutUint32(buffer[offset:offset+4], math.Float32bits(value))
	return nil
}

func ReadFloat64(buffer []byte, offset uint16) (float64, error) {
	if int(offset)+7 >= len(buffer) {
		return 0, ErrOutOfBounds
	}
	return math.Float64frombits(binary.BigEndian.Uint64(buffer[offset : offset+4])), nil
}

func WriteFloat64(value float64, buffer []byte, offset uint16) error {
	if int(offset)+7 >= len(buffer) {
		return ErrOutOfBounds
	}
	binary.BigEndian.PutUint64(buffer[offset:offset+8], math.Float64bits(value))
	return nil
}

func ReadBytes(buffer []byte, offset uint16, length uint16) ([]byte, error) {
	if int(offset)+int(length)-1 >= len(buffer) {
		return nil, ErrOutOfBounds
	}

	value := make([]byte, length)
	copy(value, buffer[offset:offset+length])
	return value, nil
}

func WriteBytes(value []byte, buffer []byte, offset uint16) error {
	if int(offset)+len(value)-1 >= len(buffer) {
		return ErrOutOfBounds
	}

	copy(buffer[offset:offset+uint16(len(value))], value)
	return nil
}
