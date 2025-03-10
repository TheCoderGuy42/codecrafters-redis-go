package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

func handleECHO(conn net.Conn, buffer string, pos *int) {
	*pos += 2 // Skip past \r\n
	_, err := conn.Write([]byte(buffer[*pos:]))
	handleErr(err)
}

func handleSET(conn net.Conn, buffer string, pos *int) {
	key := nextString(buffer, pos)
	value := nextString(buffer, pos)
	var expire_time int
	var expiry time.Time

	if *pos < len(buffer)-2 { // Skipping /r/n
		nextString(buffer, pos) // Skip "px"
		strTime := nextString(buffer, pos)
		expire_time, _ = strconv.Atoi(strTime)
	}

	if expire_time != 0 {
		milli := time.Duration(expire_time) * time.Millisecond
		expiry = time.Now().Add(milli)
	}

	counter.Set(key, value, expiry)

	_, err := conn.Write([]byte("+OK\r\n"))
	handleErr(err)
}

func handleGET(conn net.Conn, buffer string, pos *int) {
	key := nextString(buffer, pos)

	value, exists := counter.Get(key)
	print("TESTING" + value)
	if !exists {
		_, err := conn.Write([]byte(value))
		handleErr(err)
	} else {
		_, err := conn.Write([]byte(stringToBulkString(value)))
		handleErr(err)
	}
}

func handleErr(err error) {
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
	}
}

func handleCONFIG(conn net.Conn, buffer string, pos *int) {
	cmd := nextString(buffer, pos)
	switch cmd {
	case "GET":
		file := nextString(buffer, pos)
		if file == "dir" {
			fmt.Println(os.Args)
			dir := os.Args[2]
			fmt.Println(dir)
			_, err := conn.Write([]byte("*2\r\n" + stringToBulkString("dir") + stringToBulkString(dir)))
			handleErr(err)

		} else if file == "dbfilename" {
			dbfilename := os.Args[2]
			_, err := conn.Write([]byte("*2\r\n" + stringToBulkString("dbfilename") + stringToBulkString(dbfilename)))
			handleErr(err)
		}

		// args_map := parsed(os.Args)
		// val, exists := config_map.Get("dbfilename")
		// if exists {
		// 	_, err := conn.Write([]byte(stringToBulkString(val)))
		// 	handleErr(err)
		// }
	}
}
