package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

var counter = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

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
	for {
		buf := make([]byte, 2048)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			conn.Close()
			return
		}

		buffer := string(buf[:n])

		pos := strings.Index(buffer, "\r\n")                  // Skips the *1, can check it if it matters
		pos += 3                                              // Skip past \r\n$
		lenEnd := strings.Index(buffer[pos:], "\r\n")         // Gets the index of length of next word
		length, err := strconv.Atoi(buffer[pos : pos+lenEnd]) // Gets the index of length of next word
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
		}

		pos += lenEnd + 2 //Skip past \r\n
		cmd := buffer[pos : pos+length]
		pos += length // Skip past the cmd

		if cmd == "PING" {
			_, err = conn.Write([]byte("+PONG\r\n"))
			if err != nil {
				fmt.Println("Error accepting connection: ", err.Error())
				os.Exit(1)
			}
		} else if cmd == "ECHO" {
			pos += 2 // Skip past the cmd
			_, err = conn.Write([]byte(buffer[pos:]))
			handleErr(err)

		} else if cmd == "SET" {

			key := nextString(buffer, &pos)

			value := nextString(buffer, &pos)

			counter.Lock()
			counter.m[key] = value
			counter.Unlock()

			_, err = conn.Write([]byte("+OK\r\n"))
			handleErr(err)
		} else if cmd == "GET" {
			key := nextString(buffer, &pos)
			counter.RLock()
			value := counter.m[key]
			counter.RUnlock()
			_, err = conn.Write([]byte("$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"))
			handleErr(err)
		} else {
			fmt.Println("cmd not found")
		}
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

func handleErr(err error) {
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
	}
}

// func parse(buf string) string {

// 	pos := strings.Index(buf, "\r\n")
// 	// Can check for the *1 and such if it matters
// 	pos += 3                                           // Skip past \r\n$
// 	lenEnd := strings.Index(buf[pos:], "\r\n")         // Gets the index of length of next word
// 	length, err := strconv.Atoi(buf[pos : pos+lenEnd]) //Gets the index of length of next word
// 	if err != nil {
// 		fmt.Println("Error accepting connection: ", err.Error())
// 	}

// 	pos += lenEnd + 2 //Skip past \r\n
// 	data := buf[pos : pos+length]
// 	if data == "PING" {
// 		_, err = conn.Write([]byte("+PONG\r\n"))
// 		if err != nil {
// 			fmt.Println("Error accepting connection: ", err.Error())
// 			os.Exit(1)
// 		}
// 	}
// 	return data
// }
