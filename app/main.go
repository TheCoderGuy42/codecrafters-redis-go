package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

var counter = NewSafeMap()

// var config_map = NewSafeMap()

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	ln, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)

	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleReq(conn)
	}
}

func handleReq(conn net.Conn) {
	defer conn.Close()
	for {
		buf := make([]byte, 2048)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			conn.Close()
			return
		}

		buffer := string(buf[:n])
		pos := strings.Index(buffer, "\r\n") // Skips the *1, can check it if it matters
		cmd := nextString(buffer, &pos)

		switch cmd {
		case "PING":
			_, err = conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			handleECHO(conn, buffer, &pos)
		case "SET":
			handleSET(conn, buffer, &pos)
		case "GET":
			handleGET(conn, buffer, &pos)
		case "CONFIG":
			handleCONFIG(conn, buffer, &pos)
		default:
			fmt.Println("cmd not found")
		}

		handleErr(err)
	}
}

func nextString(buffer string, pos *int) string {
	*pos += 3 // Skip past \r\n$

	lenEnd := strings.Index(buffer[*pos:], "\r\n")
	if lenEnd == -1 {
		return ""
	}

	length, err := strconv.Atoi(buffer[*pos : *pos+lenEnd])
	if err != nil {
		return ""
	}

	*pos += lenEnd + 2 // Skip past \r\n
	if *pos+length > len(buffer) {
		return ""
	}

	data := buffer[*pos : *pos+length]
	*pos += length // Move position past the extracted string

	return data
}

func stringToBulkString(value string) string {
	// ret := ""
	// for i := 0; i < len(values); i++ {
	// 	ret += "$" + strconv.Itoa(len(values[i])) + "\r\n" + values[i] + "\r\n"
	// }
	// ret += "\r\n"
	return "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"
}
