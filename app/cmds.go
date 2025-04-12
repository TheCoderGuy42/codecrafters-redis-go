package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type CommandFunc func(conn net.Conn, args []string) error

func (h *RedisServer) ExecuteCmd(conn net.Conn, cmd string, args []string) error {
	var fn CommandFunc
	switch cmd {
	case "PING":
		fn = h.handlePING
	case "ECHO":
		fn = h.handleECHO
	case "SET":
		fn = h.handleSET
	case "GET":
		fn = h.handleGET
	case "CONFIG":
		fn = h.handleCONFIG
	case "KEYS":
		fn = h.handleKEY
	case "INFO":
		fn = h.handleINFO
	case "REPLCONF":
		fn = h.handleREPLCONF
	case "PSYNC":
		fn = h.handlePSYNC

	default:
		{
			log.Printf("Unknown command: %s", cmd)
			_, cmdErr := conn.Write([]byte("-ERR unknown command\r\n"))
			if cmdErr != nil {
				return fmt.Errorf("error handling command %s: %v", cmd, cmdErr)
			}
		}
	}

	return fn(conn, args)
}

func (h *RedisServer) handlePING(conn net.Conn, args []string) error {
	_, err := conn.Write([]byte(h.protocol.stringToSimpleString("PONG")))
	return err
}

func (h *RedisServer) handleECHO(conn net.Conn, args []string) error {
	_, err := conn.Write([]byte(h.protocol.stringToBulkString(args[0])))
	return err
}

func (h *RedisServer) handleSET(conn net.Conn, args []string) error {
	key := args[0]
	value := args[1]
	var expiry int64
	var expire_time int

	if len(args) > 2 {
		expire_time, _ = strconv.Atoi(args[3])
	}

	if expire_time != 0 {
		milli := time.Now().UnixMilli()
		expiry = milli + int64(expire_time)
	}

	h.ram.Set(key, value, expiry)

	_, err := conn.Write([]byte(h.protocol.stringToSimpleString("OK")))
	return err
}

func (h *RedisServer) handleGET(conn net.Conn, args []string) error {
	if len(args) != 1 {
		_, err := conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
		return err
	}

	if len(os.Args) >= 5 {
		fileName := filepath.Join(os.Args[2], os.Args[4])
		_, err := h.rdb.loadRdbFile(args, fileName)
		if err != nil {
			return err
		}
	}

	key := args[0]
	value, exists := h.ram.Get(key)
	if !exists {
		_, err := conn.Write([]byte("$-1\r\n"))
		return err
	} else {
		_, err := conn.Write([]byte(h.protocol.stringToBulkString(value)))
		return err
	}
}

func (h *RedisServer) handleCONFIG(conn net.Conn, args []string) error {
	cmd := args[0]
	switch cmd {
	case "GET":
		param := args[1]
		var value string

		// move this into config.go

		h.config.mu.RLock()

		switch param {
		case "dir":
			value = h.config.Dir
		case "dbfilename":
			value = h.config.DBFilename
		default:
			h.config.mu.RUnlock()
			return fmt.Errorf("unknown config parameter: %s", param)
		}

		_, err := conn.Write([]byte(h.protocol.stringToArray([]string{param, value})))

		h.config.mu.RUnlock()

		return err
	}
	return nil
}

func (h *RedisServer) handleKEY(conn net.Conn, args []string) error {
	if len(os.Args) < 5 {
		return fmt.Errorf("insufficient arguments, need dir and dbfilename")
	}
	fileName := filepath.Join(os.Args[2], os.Args[4])
	keys_added, err := h.rdb.loadRdbFile(args, fileName)
	if err != nil {
		return err
	}
	_, err = conn.Write([]byte(h.protocol.stringToArray(keys_added)))
	if err != nil {
		return err
	}
	return nil
}
func (h *RedisServer) handleINFO(conn net.Conn, args []string) error {
	_, err := conn.Write([]byte((h.protocol.stringToBulkString(h.getConfig()))))
	return (err)
}

func (h *RedisServer) handleREPLCONF(conn net.Conn, args []string) error {

	// Get client ID from connection
	clientID := conn.RemoteAddr().String()

	// Register this connection as a replica
	h.AddReplica(clientID, conn)

	_, err := conn.Write([]byte(h.protocol.stringToSimpleString("OK")))
	return err
}
func (h *RedisServer) handlePSYNC(conn net.Conn, args []string) error {
	_, err := conn.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"))
	if err != nil {
		return nil
	}

	emptyRDBBase64 := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
	emptyRDB, err := base64.StdEncoding.DecodeString(emptyRDBBase64)
	if err != nil {
		return fmt.Errorf("failed to decode RDB data: %w", err)
	}

	// Send the RDB file header (length)
	rdbHeader := fmt.Sprintf("$%d\r\n", len(emptyRDB))
	_, err = conn.Write([]byte(rdbHeader))
	if err != nil {
		return err
	}

	// Send the RDB file content (without any additional \r\n)
	_, err = conn.Write(emptyRDB)
	if err != nil {
		return err
	}
	return err
}

func (h *RedisServer) getConfig() string {
	ret := ""

	ret += fmt.Sprintf("role:%s", h.config.Role)
	ret += fmt.Sprintf("address:%s", h.config.Addr)
	ret += fmt.Sprintf("master_address:%s", h.config.MasterAddr)
	ret += fmt.Sprintf("master_replid:%s", h.config.MasterReplid)
	ret += fmt.Sprintf("master_repl_offset:%d", h.config.MasterReplOffset)
	ret += fmt.Sprintf("connected_slaves:%d", h.config.ConnectedReplicas)

	return ret
}
