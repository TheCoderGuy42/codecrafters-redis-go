package main

import (
	"fmt"
	"log"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func (h *RedisServer) ExecuteReplicaCmd(conn net.Conn, cmd string, args []string) error {
	var fn CommandFunc
	switch cmd {
	case "SET":
		fn = h.handleReplicaSET
	case "GET":
		fn = h.handleReplicaGET
	case "REPLCONF":
		fn = h.handleReplicaREPLCONF
	case "PING":
		fn = h.handleReplicaPING

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

func (h *RedisServer) handleReplicaSET(conn net.Conn, args []string) error {
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

	return nil

}

func (h *RedisServer) handleReplicaPING(conn net.Conn, args []string) error {
	return nil
}

func (h *RedisServer) handleReplicaREPLCONF(conn net.Conn, args []string) error {
	if len(args) > 0 && strings.ToUpper(args[0]) == "GETACK" {
		h.config.mu.RLock()
		processedBytes := h.config.MasterReplOffset
		h.config.mu.RUnlock()

		// Return ACK with the processed bytes
		_, err := conn.Write([]byte(h.protocol.stringToArray([]string{"REPLCONF", "ACK", strconv.Itoa(processedBytes)})))
		return err
	}

	// Default response for other REPLCONF commands
	_, err := conn.Write([]byte(h.protocol.stringToSimpleString("OK")))
	return err
}

func (h *RedisServer) handleReplicaGET(conn net.Conn, args []string) error {
	if len(args) != 1 {
		_, err := conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
		return err
	}

	if h.config.Dir != "" && h.config.DBFilename != "" {
		fileName := filepath.Join(h.config.Dir, h.config.DBFilename)
		_, err := h.rdb.loadRdbFile(fileName)
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
