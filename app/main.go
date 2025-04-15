package main

import (
	"fmt"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	config := parseArgs(os.Args)
	protocol := NewProtocolHandler()
	ram := NewSafeMap()
	rdb := NewRDBHandler(ram)

	s := NewRedisServer(config, protocol, ram, rdb)

	s.StartServer()
}
