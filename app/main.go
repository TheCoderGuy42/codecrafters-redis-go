package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
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
		}
		if cmd == "ECHO" {
			pos += 2 // Skip past \r\n

			_, err = conn.Write([]byte(buffer[pos:]))
			handleErr(err)
			// lenEnd := strings.Index(buffer[pos:], "\r\n")         // Gets the index of length of next word
			// length, err := strconv.Atoi(buffer[pos : pos+lenEnd]) // Gets the index of length of next word
			// if err != nil {
			// 	fmt.Println("Error accepting connection: ", err.Error())
			// }

			// pos += lenEnd + 2 //Skip past \r\n
			// data := buffer[pos : pos+length]
			// pos += length // Skip past the cmd
		}
	}
}

// func nextString(buffer string, pos int) int {
// 	pos += 3                                              // Skip past \r\n$
// 	lenEnd := strings.Index(buffer[pos:], "\r\n")         // Gets the index of length of next word
// 	length, err := strconv.Atoi(buffer[pos : pos+lenEnd]) // Gets the index of length of next word
// 	if err != nil {
// 		fmt.Println("Error accepting connection: ", err.Error())
// 	}
// 	return length
// }

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
