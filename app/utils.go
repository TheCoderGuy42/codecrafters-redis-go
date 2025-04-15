package main

import (
	"encoding/binary"
	"fmt"
)

func isWrite(cmd string) bool {
	return cmd == "SET" || cmd == "DEL"
}

func bytesToInt64LE(b []byte) int64 {
	if len(b) == 8 {
		return int64(binary.LittleEndian.Uint64(b))
	} else if len(b) == 4 {
		return int64(binary.LittleEndian.Uint32(b))
	} else if len(b) == 2 {
		return int64(binary.LittleEndian.Uint16(b))
	} else if len(b) == 1 {
		return int64(b[0])
	}
	// Default fallback
	fmt.Printf("[DEBUG] Warning: bytesToInt64LE called with unexpected byte length: %d\n", len(b))
	var value uint64
	for i := 0; i < len(b); i++ {
		value |= uint64(b[i]) << (i * 8)
	}
	return int64(value)
}

// For big-endian
func bytesToInt64BE(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}
