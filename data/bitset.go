package data

func IsBitSet(offset uint16, index uint8, buffer []byte) (bool, error) {
	bitset, err := ReadByte(buffer, offset)
	if err != nil {
		return false, err
	}
	return (bitset & (1 << index)) != 0, nil
}

func WriteBit(isSet bool, offset uint16, index uint8, buffer []byte) error {
	bitset, err := ReadByte(buffer, offset)
	if err != nil {
		return err
	}

	if isSet {
		if (bitset & (1 << index)) != 0 {
			// Unset bit
			bitset ^= 1 << index
		}
	} else {
		// Set bit
		bitset |= 1 << index
	}

	return WriteByte(bitset, buffer, offset)
}
