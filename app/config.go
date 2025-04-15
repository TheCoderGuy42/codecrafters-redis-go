package main

import (
	"strings"
	"sync"
)

type Config struct {
	Role              string
	Addr              string
	MasterAddr        string
	MasterReplid      string
	MasterReplOffset  int
	ConnectedReplicas int
	Connection        bool // if replica is connected to master
	Dir               string
	DBFilename        string
	mu                sync.RWMutex
}

type ServerState struct {
}

func parseArgs(args []string) *Config {
	config := Config{
		Role:              "master",
		Addr:              "0.0.0.0:6379",
		MasterAddr:        "",
		MasterReplid:      "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", // master_replid
		MasterReplOffset:  0,                                          // master_repl_offset
		ConnectedReplicas: 0,                                          // connected replicas
		Connection:        false,
		Dir:               "",
		DBFilename:        "",
	}

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--port":
			i++
			config.Addr = "0.0.0.0:" + args[i]

		case "--replicaof":
			i++
			config.MasterAddr = strings.Replace(args[i], " ", ":", 1) // localhost 5678 doesn't work needs a :
			config.Role = "slave"

		case "--dbfilename":
			i++
			config.DBFilename = args[i]

		case "--dir":
			i++
			config.Dir = args[i]
		}
	}

	return &config
}
