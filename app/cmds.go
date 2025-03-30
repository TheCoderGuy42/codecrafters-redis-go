package main

import (
	"fmt"
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
	var expiry int64
	var expire_time int

	if len(args) > 2 {
		expire_time, _ = strconv.Atoi(args[3])
	}

	if expire_time != 0 {
		milli := time.Now().UnixMilli()
		expiry = milli + int64(expire_time)
	}

	ram.Set(key, value, expiry)

	_, err := conn.Write([]byte(stringToSimpleString("OK")))
	return err
}

func handleGET(conn net.Conn, args []string) error {
	if len(args) != 1 {
		_, err := conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
		return err
	}

	if len(os.Args) >= 5 {
		fileName := filepath.Join(os.Args[2], os.Args[4])
		_, err := loadRdbFile(args, fileName)
		if err != nil {
			return err
		}
	}

	key := args[0]
	value, exists := ram.Get(key)
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
	fileName := filepath.Join(os.Args[2], os.Args[4])
	keys_added, err := loadRdbFile(args, fileName)
	if err != nil {
		return err
	}
	_, err = conn.Write([]byte(stringToArray(keys_added)))
	if err != nil {
		return err
	}
	return nil
}
func handleINFO(conn net.Conn, args []string) error {
	print(dbConfig["role"])
	_, err := conn.Write([]byte(stringToBulkString(getDbConfig())))
	return (err)
}

func getDbConfig() string {
	ret := ""
	for k, v := range dbConfig {
		ret += k + ":" + v
	}
	return ret
}
