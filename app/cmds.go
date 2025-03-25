package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func handlePING(conn net.Conn, args []string) error {
	_, err := conn.Write([]byte(stringToSimpleString("PONG")))
	return err
}

func handleECHO(conn net.Conn, args []string) error {
	_, err := conn.Write([]byte(stringToBulkString(args[0])))
	return err
}

func handleSET(conn net.Conn, args []string) error {
	key := args[0]
	value := args[1]
	var expire_time int
	var expiry time.Time

	if len(args) > 2 {
		expire_time, _ = strconv.Atoi(args[3])
	}

	if expire_time != 0 {
		milli := time.Duration(expire_time) * time.Millisecond
		expiry = time.Now().Add(milli)
	}

	counter.Set(key, value, expiry)

	_, err := conn.Write([]byte(stringToSimpleString("OK")))
	return err
}

func handleGET(conn net.Conn, args []string) error {
	if len(args) != 1 {
		_, err := conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
		return err
	}
	key := args[0]

	value, exists := counter.Get(key)

	if !exists {
		_, err := conn.Write([]byte("$-1\r\n"))
		return err
	} else {
		_, err := conn.Write([]byte(stringToBulkString(value)))
		return err
	}
}

func handleCONFIG(conn net.Conn, args []string) error {
	cmd := args[0]
	switch cmd {
	case "GET":
		file := args[1]
		if file == "dir" {
			fmt.Println(os.Args)
			dir := os.Args[2]
			fmt.Println(dir)
			_, err := conn.Write([]byte("*2\r\n" + stringToBulkString("dir") + stringToBulkString(dir)))
			return (err)

		} else if file == "dbfilename" {
			dbfilename := os.Args[2]
			_, err := conn.Write([]byte("*2\r\n" + stringToBulkString("dbfilename") + stringToBulkString(dbfilename)))
			return (err)
		}

	}
	return nil
}

func handleKEY(conn net.Conn, args []string) error {
	if len(os.Args) < 5 {
		return fmt.Errorf("insufficient arguments, need dir and dbfilename")
	}

	fi, err := os.Open(filepath.Join(os.Args[2], os.Args[4]))
	if err != nil {
		return err
	}
	defer fi.Close()

	reader := bufio.NewReader(fi)

	// Read the header
	header := make([]byte, 9)
	if _, err = io.ReadFull(reader, header); err != nil {
		return fmt.Errorf("failed to read header: %v", err)
	}

	if string(header) != "REDIS0011" {
		return fmt.Errorf("invalid RDB file format: %s", string(header))
	}

	// Skip metadata sections
	for {
		typeByte, err := reader.ReadByte()
		if err != nil {
			return err
		}

		if typeByte == 0xFE {
			// Database section starts
			break
		} else if typeByte == 0xFF {
			// End of file
			return nil
		} else if typeByte == 0xFA {
			// Metadata entry - skip it
			if err := skipStringEncoded(reader); err != nil {
				return err
			}
			if err := skipStringEncoded(reader); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("unexpected section type: %x", typeByte)
		}
	}

	// We're now at a database section (0xFE)

	// Read database number
	if _, err := readSizeEncoded(reader); err != nil {
		return err
	}

	// Check for hash table sizes
	nextByte, err := reader.ReadByte()
	if err != nil {
		return err
	}

	if nextByte != 0xFB {
		return fmt.Errorf("expected hash table size marker (0xFB), got: %x", nextByte)
	}

	// Read hash table sizes
	if _, err := readSizeEncoded(reader); err != nil {
		return err
	}
	if _, err := readSizeEncoded(reader); err != nil {
		return err
	}

	// Now read keys
	var keys []string

	for {
		// Peek at the next byte
		nextByte, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Put it back
		if err := reader.UnreadByte(); err != nil {
			return err
		}

		// Check for special markers
		if nextByte == 0xFF {
			reader.ReadByte() // consume it
			break             // End of file
		}

		if nextByte == 0xFE {
			break // Next database
		}

		// Handle expiry if present
		if nextByte == 0xFD || nextByte == 0xFC {
			reader.ReadByte() // consume marker

			expSize := 4
			if nextByte == 0xFC {
				expSize = 8
			}

			expBuf := make([]byte, expSize)
			if _, err := io.ReadFull(reader, expBuf); err != nil {
				return err
			}
		}

		// Read value type
		valType, err := reader.ReadByte()
		if err != nil {
			return err
		}

		// Only handling string values (0x00)
		if valType != 0x00 {
			return fmt.Errorf("unsupported value type: %x", valType)
		}

		// Read key
		key, err := readStringEncoded(reader)
		if err != nil {
			return err
		}

		keys = append(keys, key)

		// Skip value
		if err := skipStringEncoded(reader); err != nil {
			return err
		}
	}

	// Write the response
	if len(keys) == 0 {
		_, err = conn.Write([]byte("*0\r\n"))
	} else {
		resp := "*" + strconv.Itoa(len(keys)) + "\r\n"
		for _, key := range keys {
			resp += stringToBulkString(key)
		}
		_, err = conn.Write([]byte(resp))
	}

	return err
}

// Helper functions for reading RDB format

func readSizeEncoded(reader *bufio.Reader) (uint64, error) {
	firstByte, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}

	// Check the first two bits
	firstTwoBits := firstByte >> 6

	if firstTwoBits == 0 {
		// 00 - 6 bit length
		return uint64(firstByte & 0x3F), nil
	} else if firstTwoBits == 1 {
		// 01 - 14 bit length
		secondByte, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		return uint64((uint64(firstByte&0x3F) << 8) | uint64(secondByte)), nil
	} else if firstTwoBits == 2 {
		// 10 - 32 bit length
		buf := make([]byte, 4)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return 0, err
		}
		return uint64(buf[0])<<24 | uint64(buf[1])<<16 | uint64(buf[2])<<8 | uint64(buf[3]), nil
	} else {
		// 11 - special encoding
		specialType := firstByte & 0x3F

		if specialType == 0 {
			// 8-bit integer (C0 encoding)
			b, err := reader.ReadByte()
			if err != nil {
				return 0, err
			}
			return uint64(b), nil
		} else if specialType == 1 {
			// 16-bit integer (C1 encoding) - little endian
			buf := make([]byte, 2)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return 0, err
			}
			return uint64(buf[0]) | (uint64(buf[1]) << 8), nil
		} else if specialType == 2 {
			// 32-bit integer (C2 encoding) - little endian
			buf := make([]byte, 4)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return 0, err
			}
			return uint64(buf[0]) | (uint64(buf[1]) << 8) | (uint64(buf[2]) << 16) | (uint64(buf[3]) << 24), nil
		} else if specialType == 3 {
			// LZF compressed string (C3 encoding)
			return 0, fmt.Errorf("LZF compressed strings not supported")
		} else {
			// For now, just return an error for any other special encoding
			// This should be expanded if needed
			return 0, fmt.Errorf("unsupported special encoding: %x", specialType)
		}
	}
}

func readStringEncoded(reader *bufio.Reader) (string, error) {
	firstByte, err := reader.ReadByte()
	if err != nil {
		return "", err
	}

	// Put back the byte so we can use readSizeEncoded
	if err := reader.UnreadByte(); err != nil {
		return "", err
	}

	// If this is a special encoding (first two bits are 11)
	if (firstByte >> 6) == 3 {
		value, err := readSizeEncoded(reader)
		if err != nil {
			return "", err
		}
		return strconv.FormatUint(value, 10), nil
	}

	// Otherwise, it's a normal string
	length, err := readSizeEncoded(reader)
	if err != nil {
		return "", err
	}

	// Read the string
	buf := make([]byte, length)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return "", err
	}

	return string(buf), nil
}

func skipStringEncoded(reader *bufio.Reader) error {
	firstByte, err := reader.ReadByte()
	if err != nil {
		return err
	}

	// Put back the byte so we can use readSizeEncoded
	if err := reader.UnreadByte(); err != nil {
		return err
	}

	// If this is a special encoding (first two bits are 11)
	if (firstByte >> 6) == 3 {
		_, err := readSizeEncoded(reader)
		return err
	}

	// Otherwise, it's a normal string
	length, err := readSizeEncoded(reader)
	if err != nil {
		return err
	}

	// Skip the string content
	skip := make([]byte, length)
	_, err = io.ReadFull(reader, skip)
	return err
}
