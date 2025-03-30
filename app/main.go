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
var dbConfig = make(map[string]string)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	var address string

	if len(os.Args) > 2 && os.Args[1] == "--port" {
		address = "0.0.0.0:" + os.Args[2]
	} else {
		address = "0.0.0.0:6379"
		dbConfig["role"] = "master"
	}
	ln, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Printf("Failed to bind to %s\n", address)
		os.Exit(1)
	}
	defer ln.Close()

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
