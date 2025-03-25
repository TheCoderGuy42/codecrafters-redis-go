package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Create a dedicated Redis protocol parser
type RedisParser struct {
	reader *bufio.Reader
}

func NewRedisParser(r io.Reader) *RedisParser {
	return &RedisParser{
		reader: bufio.NewReader(r),
	}
}

func (p *RedisParser) ReadCommand() ([]string, error) {
	firstByte, err := p.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	if firstByte != '*' {
		return nil, fmt.Errorf("-expected array, got %c", firstByte)
	}

	lengthStr, err := p.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	lengthStr = strings.TrimSuffix(lengthStr, "\r\n")
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %s", lengthStr)
	}

	result := make([]string, length)

	for i := 0; i < length; i++ {
		element, err := p.readBulkString()
		if err != nil {
			return nil, err
		}
		result[i] = element
	}

	return result, err
}

func (p *RedisParser) readBulkString() (string, error) {
	typeByte, err := p.reader.ReadByte()
	if err != nil {
		return "", err
	}
	if typeByte != '$' {
		return "", fmt.Errorf("-expected array, got %c", typeByte)
	}
	lengthStr, err := p.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	length, err := strconv.Atoi(strings.TrimSpace(lengthStr))
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %s", lengthStr)
	}

	data := make([]byte, length+2)
	_, err = io.ReadFull(p.reader, data)
	if err != nil {
		return "", err
	}
	return string(data[:length]), nil

}

func stringToBulkString(value string) string {
	return "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"
}

func stringToSimpleString(value string) string {
	return "+" + value + "\r\n"
}
