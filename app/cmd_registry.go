package main

import "net"

type CommandFunc func(conn net.Conn, args []string) error

// stores all the Redis command handlers
var commandRegistry = make(map[string]CommandFunc)

func RegisterCommand(name string, handler CommandFunc) {
	commandRegistry[name] = handler
}

// Initialize commands in an init function
func init() {
	RegisterCommand("PING", handlePING)
	RegisterCommand("ECHO", handleECHO)
	RegisterCommand("SET", handleSET)
	RegisterCommand("GET", handleGET)
	RegisterCommand("CONFIG", handleCONFIG)
	RegisterCommand("KEYS", handleKEY)
	RegisterCommand("INFO", handleINFO)
	RegisterCommand("REPLCONF", handleREPLCONF)
	RegisterCommand("PSYNC", handlePSYNC)
	// Add more commands here as needed
}
