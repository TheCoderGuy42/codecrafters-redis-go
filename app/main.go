package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

var ram = NewSafeMap()
var dbConfig = make(map[string]string)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	var listeningAddress string

	if len(os.Args) > 4 && os.Args[1] == "--port" && os.Args[3] == "--replicaof" {
		listeningAddress = "0.0.0.0:" + os.Args[2]

		masterInfo := strings.Split(os.Args[4], " ")

		dbConfig["role"] = "slave"

		masterHost := masterInfo[0]
		masterPort := masterInfo[1]

		go connectToMaster(masterHost, masterPort)

	} else if len(os.Args) > 2 && os.Args[1] == "--port" {
		listeningAddress = "0.0.0.0:" + os.Args[2]

		dbConfig["role"] = "master"

	} else {
		listeningAddress = "0.0.0.0:6379"

		dbConfig["role"] = "master"
		//HARDCODED
		dbConfig["master_replid"] = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		dbConfig["master_repl_offset"] = "0"
	}

	ln, err := net.Listen("tcp", listeningAddress)
	if err != nil {
		fmt.Printf("Failed to bind to %s\n", listeningAddress)
		os.Exit(1)
	}
	defer ln.Close()
	//handle client command
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
		cmdArgs, err := parser.readArray()
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

func connectToMaster(masterHost string, masterPort string) {
	for {
		address := masterHost + ":" + masterPort
		conn, err := net.Dial("tcp", address)
		if err != nil {
			fmt.Printf("Failed to bind to %s\n, retrying", address)
			time.Sleep(5 * time.Second)
			continue
		}

		fmt.Printf("Connected to master at %s ", address)

		sendPING(conn)
		var buf bytes.Buffer
		io.Copy(&buf, conn)
		fmt.Printf("%s", buf.String())
		if buf.String() != "+PONG" {
			fmt.Printf("not a valid cmd from master")
		}
		sendREPLCONF(conn, os.Args[2])

		err = setUpReplication()
		if err != nil {
			fmt.Printf("Failed to set up replication %s", address)
			time.Sleep(5 * time.Second)
			continue
		}

		err = processReplicationStream(conn)
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

func processReplicationStream(conn net.Conn) error {
	return nil
}

func setUpReplication() error {
	return nil
}
