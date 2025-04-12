package main

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
)

type ProtocolHandler struct{}

func NewProtocolHandler() *ProtocolHandler {
	return &ProtocolHandler{}
}

func (p *ProtocolHandler) readCommands(conn net.Conn) ([]string, error) {
	//Constraint, command can't be longer than 1024
	buffer := make([]byte, 4092)
	n, err := conn.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read command: %w", err)
	}

	command, err := p.parseArray(buffer[:n])
	if err != nil {
		return nil, fmt.Errorf("failed to parse command: %w", err)
	}

	return command, nil
}

// redis protocol parser
func (p *ProtocolHandler) parseArray(data []byte) ([]string, error) {
	if len(data) == 0 || data[0] != '*' {
		return nil, fmt.Errorf("expected array, got %x", data[0])
	}
	idx := bytes.IndexByte(data, '\n')
	if idx <= 0 || data[idx-1] != '\r' {
		return nil, fmt.Errorf("malformed array length")
	}
	length, err := strconv.Atoi(string(data[1 : idx-1]))
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %s", string(data[1:idx-1]))
	}
	pos := idx + 1
	result := make([]string, length)

	for i := 0; i < length; i++ {
		element, bytesRead, err := p.parseBulkString(data[pos:])
		if err != nil {
			return nil, err
		}
		result[i] = element
		pos += bytesRead
	}

	return result, nil
}

func (p *ProtocolHandler) parseBulkString(data []byte) (string, int, error) {
	if len(data) == 0 || data[0] != '$' {
		return "", 0, fmt.Errorf("expected bulk string modifier, got %c", data[0])
	}
	idx := bytes.IndexByte(data, '\n')
	if idx <= 0 || data[idx-1] != '\r' {
		return "", 0, fmt.Errorf("malformed bulk string length")
	}

	length, err := strconv.Atoi(string(data[1 : idx-1]))
	if err != nil {
		return "", 0, fmt.Errorf("invalid bulk string length: %s", string(data[1:idx-1]))
	}

	endPos := idx + 1 + length + 2 // +1 for \n after length, +2 for \r\n after data
	if len(data) < endPos {
		return "", 0, fmt.Errorf("incomplete bulk string data")
	}

	result := string(data[idx+1 : idx+1+length])

	if data[idx+1+length] != '\r' || data[idx+1+length+1] != '\n' {
		return "", 0, fmt.Errorf("malformed bulk string terminator")
	}

	return result, endPos, nil
}

func (p *ProtocolHandler) parseSimpleString(data []byte) (string, int, error) {
	if len(data) == 0 || data[0] != '+' {
		return "", 0, fmt.Errorf("expected simple string, got %c", data[0])
	}

	idx := bytes.IndexByte(data, '\n')
	if idx <= 0 || data[idx-1] != '\r' {
		return "", 0, fmt.Errorf("malformed simple string")
	}
	result := string(data[1 : idx-1])

	return result, idx + 1, nil
}

func (p *ProtocolHandler) stringToBulkString(value string) string {
	return "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"
}

func (p *ProtocolHandler) stringToSimpleString(value string) string {
	return "+" + value + "\r\n"
}

func (p *ProtocolHandler) stringToArray(value []string) string {
	result := "*" + strconv.Itoa(len(value)) + "\r\n"

	for i := 0; i < len(value); i++ {
		result += p.stringToBulkString(value[i])
	}

	return result
}
