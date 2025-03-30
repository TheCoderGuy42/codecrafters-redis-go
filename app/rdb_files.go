package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
)

const (
	EOF          = 0xFF //End of the RDB file
	SELECTDB     = 0xFE //Database Selector
	EXPIRETIME   = 0xFD //Expire time in seconds, see Key Expiry Timestamp
	EXPIRETIMEMS = 0xFC //	Expire time in milliseconds, see Key Expiry Timestamp
	RESIZEDB     = 0xFB //	Hash table sizes for the main keyspace and expires, see Resizedb information
	AUX          = 0xFA //	Auxiliary fields. Arbitrary key-value settings, see Auxiliary fields
)

func loadRdbFile(args []string, fileName string) ([]string, error) {
	fi, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer fi.Close()

	reader := bufio.NewReader(fi)
	// Gettign the first
	header := make([]byte, 9)
	if _, err = reader.Read(header); err != nil {
		return nil, fmt.Errorf("failed to read header: %v", err)
	}
	if string(header) != "REDIS0011" {
		return nil, fmt.Errorf("invalid RDB file format: %s", string(header))
	}
	// Skipping over the metadata section
	for {
		typeByte, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if typeByte == AUX {
			//metadata name
			_, err = readStringEncoding(reader)
			if err != nil {
				return nil, err
			}
			//metadata value
			_, err = readStringEncoding(reader)
			if err != nil {
				return nil, err
			}

		} else if typeByte == SELECTDB {
			reader.UnreadByte()
			break
		} else if typeByte == EOF {
			return nil, err
		}

	}
	//database section
	var keys_added []string
	for {
		sectionMarker, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				// End of file reached
				break
			}
			return nil, err
		}

		if sectionMarker == EOF {
			// End of file marker
			checksum := make([]byte, 8)
			_, err = io.ReadFull(reader, checksum)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				return nil, err
			}
			break
		}

		if sectionMarker != SELECTDB {
			return nil, fmt.Errorf("expected database marker 0xFE, got: %x", sectionMarker)
		}

		// first 3 fields index hashTableSize keyExpires
		// index of the database
		_, err = readSizeEncoding(reader)
		if err != nil {
			return nil, err
		}
		// FB
		nextByte, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if nextByte != RESIZEDB {
			return nil, fmt.Errorf("hash table size marker is wrong, got: %x", nextByte)
		}
		// hash table sizes
		if _, err := readSizeEncoding(reader); err != nil {
			return nil, err
		}
		if _, err := readSizeEncoding(reader); err != nil {
			return nil, err
		}
		for {
			expiry := int64(0)
			// expiry/EOF marker
			typeByte, err := reader.ReadByte()
			if err != nil {
				if err == io.EOF {
					return nil, nil // End of file reached
				}
				return nil, err
			}

			// section markers
			if typeByte == SELECTDB || typeByte == EOF {
				reader.UnreadByte() // Put back for the outer loop to handle
				break
			}

			// expiry markers
			if typeByte == EXPIRETIMEMS {
				// Millisecond expiry
				milliseconds := make([]byte, 8)
				if _, err := io.ReadFull(reader, milliseconds); err != nil {
					return nil, err
				}
				expiry = bytesToInt64LE(milliseconds)

				// after expiry, read the value type
				typeByte, err = reader.ReadByte()
				if err != nil {
					return nil, err
				}
			} else if typeByte == EXPIRETIME {
				// Second expiry
				seconds := make([]byte, 4)
				if _, err := io.ReadFull(reader, seconds); err != nil {
					return nil, err
				}
				expiry = bytesToInt64LE(seconds) * 1000
				// after expiry, read the value type
				typeByte, err = reader.ReadByte()
				if err != nil {
					return nil, err
				}
			}

			// Handle string value
			if typeByte == 0x00 {
				key, err := readStringEncoding(reader)
				if err != nil {
					return nil, err
				}

				// Read the value
				value, err := readStringEncoding(reader)
				if err != nil {
					return nil, err
				}
				ram.Set(key, value, expiry)
				keys_added = append(keys_added, key)
			} else {
				return nil, fmt.Errorf("unsupported value type: %x", typeByte)
			}
		}
	}
	return keys_added, nil
}

func readSizeEncoding(reader *bufio.Reader) (uint64, error) {
	fullByte, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}

	firstTwoBits := fullByte >> 6
	lastSixBits := uint64(fullByte & 0x3F)

	if firstTwoBits == 0 {
		return lastSixBits, nil
	} else if firstTwoBits == 1 {
		nextByte, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		return uint64(lastSixBits<<8 | uint64(nextByte)), nil
	} else if firstTwoBits == 2 {
		buf := make([]byte, 4)
		_, err := reader.Read(buf)
		if err != nil {
			return 0, err
		}
		return uint64(bytesToInt64BE(buf)), nil
	} else {
		// 11
		specialType := fullByte & 0x3F

		if specialType == 0 {
			// 8-bit int
			b, err := reader.ReadByte()
			if err != nil {
				return 0, err
			}
			return uint64(b), nil
		} else if specialType == 1 {
			// 16-bit int - little endian
			buf := make([]byte, 2)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return 0, err
			}
			return uint64(bytesToInt64LE(buf)), nil
		} else if specialType == 2 {
			// 32-bit int - little endian
			buf := make([]byte, 4)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return 0, err
			}
			return uint64(bytesToInt64LE(buf)), nil
		} else if specialType == 3 {
			// LZF compressed string (C3 encoding)
			return 0, fmt.Errorf("LZF compressed strings not supported")
		} else {
			// For now, just return an error for any other special encoding
			return 0, fmt.Errorf("unsupported special encoding: %x", specialType)
		}
	}
}

func readStringEncoding(reader *bufio.Reader) (string, error) {
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
		fmt.Println("Special encoding for string detected")
		value, err := readSizeEncoding(reader)
		if err != nil {
			return "", err
		}
		return strconv.FormatUint(value, 10), nil
	}

	// Otherwise, it's a normal string
	length, err := readSizeEncoding(reader)
	if err != nil {
		return "", err
	}

	// Read the string
	buf := make([]byte, length)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return "", fmt.Errorf("failed to read string data: %v", err)
	}

	return string(buf), nil
}

func bytesToInt64LE(b []byte) int64 {
	return int64(binary.LittleEndian.Uint64(b))
}

// For big-endian
func bytesToInt64BE(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}
