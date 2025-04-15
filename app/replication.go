package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

/*
Who should be using this function?

the replica should be using this function
*/

func (r *RedisServer) startReplication() {
	for {
		address := r.config.MasterAddr
		conn, err := net.Dial("tcp", address)
		if err != nil {
			fmt.Printf("Failed to bind to %s\n, retrying", address)
			time.Sleep(5 * time.Second)
			continue
		}

		err = r.setUpReplication(conn)
		if err != nil {
			fmt.Printf("Failed to set up replication %s", address)
			time.Sleep(5 * time.Second)
			continue
		}

		err = r.processReplicationStream(conn)
		if err != nil {
			fmt.Printf("Failed to set up replication %s", address)
			time.Sleep(5 * time.Second)
			continue
		}
		conn.Close()
		// Wait before reconnecting
		time.Sleep(time.Second * 5)
	}
}

/*
Who should be using this function?

the replica should be using this function
Replicas connect to masters to receive data changes
Masters don't connect to replicas; the connection is always initiated by the replica
*/
func (r *RedisServer) setUpReplication(conn net.Conn) error {

	r.sendPING(conn)
	r.sendREPLCONF(conn, os.Args[2])
	r.sendPSYNC(conn)

	return nil
}

func (r *RedisServer) processReplicationStream(conn net.Conn) error {
	for {
		cmds_bytes, err := r.readResponse(conn)
		// fmt.Printf("\n Cmd_bytes: %s  Length %d \n", string(cmds_bytes), len(cmds_bytes))
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Error reading command: %v", err)
				return err
			}
		}

		//parsing the cmds to bytes so they can be used properly
		cmds, err := r.protocol.parseArrays(cmds_bytes)

		fmt.Printf("%v", cmds)
		if err != nil {
			return err
		}

		for i := 0; i < len(cmds); i++ {

			r.ExecuteReplicaCmd(conn, cmds[i][0], cmds[i][1:])
		}

	}
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

func (s *RedisServer) handleReplicaGET(conn net.Conn, args []string) error {
	if len(args) != 1 {
		_, err := conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
		return err
	}

	if s.config.Dir != "" && s.config.DBFilename != "" {
		fileName := filepath.Join(s.config.Dir, s.config.DBFilename)
		_, err := s.rdb.loadRdbFile(fileName)
		if err != nil {
			return err
		}
	}

	key := args[0]
	value, exists := s.ram.Get(key)
	if !exists {
		_, err := conn.Write([]byte("$-1\r\n"))
		return err
	} else {
		_, err := conn.Write([]byte(s.protocol.stringToBulkString(value)))
		return err
	}
}

func (h *RedisServer) ExecuteReplicaCmd(conn net.Conn, cmd string, args []string) error {
	var fn CommandFunc
	switch cmd {
	case "SET":
		fn = h.handleReplicaSET
	case "GET":
		fn = h.handleReplicaGET

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

func (r *RedisServer) readResponse(conn net.Conn) ([]byte, error) {
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	return buffer[:n], nil
}

func (r *RedisServer) readResponseUntil(conn net.Conn, val int) ([]byte, error) {
	buffer := make([]byte, val)
	n, err := conn.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	return buffer[:n], nil
}

func (r *RedisServer) sendPING(conn net.Conn) error {
	cmd := []string{"PING"}
	_, err := conn.Write([]byte(r.protocol.stringToArray(cmd)))
	if err != nil {
		return err
	}
	r.readResponse(conn)
	return err
}

func (r *RedisServer) sendREPLCONF(conn net.Conn, localPort string) error {
	// since os
	cmd := []string{"REPLCONF", "listening-port", localPort}
	_, err := conn.Write([]byte(r.protocol.stringToArray(cmd)))
	if err != nil {
		return err
	}
	r.readResponse(conn)
	//HARDCODED
	cmd = []string{"REPLCONF", "capa", "psync2"}
	_, err = conn.Write([]byte(r.protocol.stringToArray(cmd)))

	r.readResponse(conn)

	return err

}
func (r *RedisServer) sendPSYNC(conn net.Conn) error {
	cmd := []string{"PSYNC", "?", "-1"}
	_, err := conn.Write([]byte(r.protocol.stringToArray(cmd)))
	if err != nil {
		return err
	}
	// HARDCODED
	// this is the empty file + set cmds actually so i have to parse the file properly
	r.readRDBFile(conn)

	return err
}

func (r *RedisServer) readRDBFile(conn net.Conn) {

	// full resync + rdb file
	resync, _ := r.readResponseUntil(conn, 56)

	fmt.Printf("resync: %s, Length %d", string(resync), len(resync))

	rdb, _ := r.readResponseUntil(conn, 93)

	fmt.Printf("rdb file: %s, Length %d", string(rdb), len(rdb))

}

// SaveRDBToFile saves RDB data to a file
func (r *RedisServer) SaveRDBToFile(data []byte, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create RDB file: %w", err)
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write data to RDB file: %w", err)
	}

	err = file.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync data to RDB file: %w", err)
	}

	return nil
}

// ... existing code ...
