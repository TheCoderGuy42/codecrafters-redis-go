package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
)

var ram = NewSafeMap()

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

	parser := NewRedisParser(conn)

	for {
		// Constraits: arguments cannot be longer than 2048 bytes
		cmdArgs, err := parser.ReadCommand()
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading command: %v", err)
			}
			return
		}

		if len(cmdArgs) == 0 {
			log.Printf("Empty command received")
			continue
		}

		cmd := strings.ToUpper(cmdArgs[0])

		var cmdErr error
		handler, exists := commandRegistry[cmd]

		if !exists {
			log.Printf("Unknown command: %s", cmd)
			_, cmdErr = conn.Write([]byte("-ERR unknown command\r\n"))
			if cmdErr != nil {
				log.Printf("Error handling command %s: %v", cmd, cmdErr)
				return
			}
		}

		err = handler(conn, cmdArgs[1:])
		if err != nil {
			log.Printf("Error handling command %s: %v", cmd, err)
			return
		}
	}
}
