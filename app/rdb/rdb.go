package rdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

func DecodeLength(r *bufio.Reader) (uint64, byte, error) {
	firstByte, err := r.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	firstTwoBits := firstByte >> 6
	if firstTwoBits == 0 {
		// 6-bit length
		return uint64(firstByte), 0, nil
	}
	if firstTwoBits == 1 {
		// 14-bit length
		secondByte, err := r.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		return (uint64(firstByte&0x3F) << 8) | uint64(secondByte), 0, nil
	}
	if firstTwoBits == 2 {
		// 32-bit length
		var lengthBytes [4]byte
		if _, err := io.ReadFull(r, lengthBytes[:]); err != nil {
			return 0, 0, err
		}
		return uint64(binary.BigEndian.Uint32(lengthBytes[:])), 0, nil
	}

	switch firstByte {
	case 0xC0:
		return 1, 0xC0, nil
	case 0xC1:
		return 2, 0xC1, nil
	case 0xC2:
		return 4, 0xC2, nil
	}

	return uint64(firstByte << 2), 0, nil
}

func DecodeValue(r *bufio.Reader) (any, error) {
	length, typ, err := DecodeLength(r)
	if err != nil {
		return nil, err
	}

	strBytes := make([]byte, length)
	if _, err := io.ReadFull(r, strBytes); err != nil {
		return nil, err
	}

	switch typ {
	case 0:
		return string(strBytes), nil
	case 0xC0:
		return strBytes[0], nil
	case 0xC1:
		return int64(binary.LittleEndian.Uint16(strBytes)), nil
	case 0xC2:
		return int64(binary.LittleEndian.Uint32(strBytes)), nil
	default:
		return nil, fmt.Errorf("unknown value type: %d", typ)
	}

}

func decodeString(r *bufio.Reader) (string, error) {
	value, err := DecodeValue(r)
	if err != nil {
		return "", err
	}
	strBytes, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("expected string value, got %T", value)
	}

	return string(strBytes), nil
}

func DecodeKV(r *bufio.Reader) (string, string, error) {
	key, err := decodeString(r)
	if err != nil {
		return "", "", err
	}
	value, err := DecodeValue(r)
	if err != nil {
		return "", "", err
	}
	return key, fmt.Sprintf("%v", value), nil
}
