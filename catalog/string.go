package catalog

import (
	"github.com/tinydb/data"
)

type StringData struct {
	StrBytes []byte
	Overflow OverflowData
}

type WriteStringData struct {
	StrBytes   []byte
	DataOffset uint16
	Overflow   OverflowData
}

type OverflowData struct {
	TotalLength uint32
	PageId      uint32
	SlotIndex   uint16
}

func ReadOverflowString(buffer []byte, offset uint16) (StringData, error) {
	length, err := data.ReadUint16(buffer, offset)
	if err != nil {
		return StringData{}, err
	}

	overflowPageId, err := data.ReadUint32(buffer, offset+2)
	if err != nil {
		return StringData{}, err
	}

	var strData StringData
	if overflowPageId != 0 {
		slotIndex, err := data.ReadUint16(buffer, offset+6)
		if err != nil {
			return StringData{}, err
		}

		strData.Overflow = OverflowData{
			PageId:    overflowPageId,
			SlotIndex: slotIndex,
		}
	}

	bytes, err := data.ReadBytes(buffer, offset+8, length)
	if err != nil {
		return StringData{}, err
	}

	strData.StrBytes = bytes
	return strData, nil
}

func WriteOverflowString(strData StringData, buffer []byte, offset uint16) error {
	err := data.WriteUint16(uint16(len(strData.StrBytes)), buffer, offset)
	if err != nil {
		return err
	}

	err = data.WriteUint32(strData.Overflow.PageId, buffer, offset+2)
	if err != nil {
		return err
	}

	if strData.Overflow.PageId != 0 {
		err = data.WriteUint16(strData.Overflow.SlotIndex, buffer, offset+6)
		if err != nil {
			return err
		}
	}

	err = data.WriteBytes(strData.StrBytes, buffer, offset+8)
	if err != nil {
		return err
	}

	return nil
}

func readString(buffer []byte, offset uint16) (StringData, error) {
	dataOffset, err := data.ReadUint16(buffer, offset)
	if err != nil {
		return StringData{}, err
	}

	length, err := data.ReadUint16(buffer, offset+2)
	if err != nil {
		return StringData{}, err
	}

	overflowPageId, err := data.ReadUint32(buffer, offset+4)
	if err != nil {
		return StringData{}, err
	}

	var strData StringData
	if overflowPageId != 0 {
		totalLength, err := data.ReadUint32(buffer, offset+8)
		if err != nil {
			return StringData{}, err
		}

		slotIndex, err := data.ReadUint16(buffer, offset+12)
		if err != nil {
			return StringData{}, err
		}

		strData.Overflow = OverflowData{
			PageId:      overflowPageId,
			SlotIndex:   slotIndex,
			TotalLength: totalLength,
		}
	}

	bytes, err := data.ReadBytes(buffer, dataOffset, length)
	if err != nil {
		return StringData{}, err
	}

	strData.StrBytes = bytes
	return strData, nil
}

// Variable length field write assumes there is enough space to fit value;
// it also assumes, in the case of a length increase that other variable length fields
// after it are at the correct offset to fit new field value.
// Whole tuple move in case of length increase should be done prior to this operation.
// Overflow info should be computed and passed in value.
func writeString(strData WriteStringData, buffer []byte, field Field) error {
	offset := field.offset
	err := data.WriteUint16(strData.DataOffset, buffer, offset)
	if err != nil {
		return err
	}

	err = data.WriteUint16(uint16(len(strData.StrBytes)), buffer, offset+2)
	if err != nil {
		return err
	}

	err = data.WriteUint32(strData.Overflow.PageId, buffer, offset+4)
	if err != nil {
		return err
	}

	if strData.Overflow.PageId != 0 {
		err = data.WriteUint32(strData.Overflow.TotalLength, buffer, offset+8)
		if err != nil {
			return err
		}

		err = data.WriteUint16(strData.Overflow.SlotIndex, buffer, offset+12)
		if err != nil {
			return err
		}
	}

	err = data.WriteBytes(strData.StrBytes, buffer, strData.DataOffset)
	if err != nil {
		return err
	}

	return nil
}
