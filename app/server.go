package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

type RedisServer struct {
	config     *Config
	protocol   *ProtocolHandler
	ram        *SafeMap
	rdb        *RDBHandler
	listener   net.Listener
	clients    map[string]*Client
	replica    map[string]net.Conn
	replicasMu sync.RWMutex // there so you don't accidentally delete a replica while its handling a cmd
}

type Client struct {
	Conn net.Conn
	ID   string
}

func NewRedisServer(
	config *Config,
	protocol *ProtocolHandler,
	ram *SafeMap,
	rdb *RDBHandler,
) *RedisServer {
	return &RedisServer{
		config:   config,
		protocol: protocol,
		ram:      ram,
		rdb:      rdb,
		clients:  make(map[string]*Client),
		replica:  make(map[string]net.Conn),
	}
}

func (s *RedisServer) StartServer() {

	ln, err := net.Listen("tcp", s.config.Addr)
	if err != nil {
		fmt.Printf("Failed to bind to %s\n", s.config.Addr)
		os.Exit(1)
	}
	defer ln.Close()

	s.listener = ln

	// the replication manager just handles the replica <-> master communication
	if s.config.Role == "slave" {
		go s.startReplication()
	}

	s.acceptClient()
}

func (s *RedisServer) acceptClient() {
	/*
		The client must
		1. StartReplication and talk to master for state updates
			1. Initalize communication with master
			2. Continue listening to master
		2. handle regular commands sent by the user
			Should cmds sent by master be given to StartReplication?
			How are cmds given and recieved
			On start up theres the os.Args handled by the config
			after that there's clients which each talk to the same RedisServer

		2 ways argumemnts
	*/

	//handle client, you're accepting a client here
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go s.acceptConnection(conn)
	}

}

func (s *RedisServer) acceptConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// Constraits: arguments cannot be longer than 2048 bytes
		cmdArgs, err := s.protocol.readCommands(conn)

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

		if isWrite(cmd) && s.config.Role == "slave" {
			log.Printf("Cannot propagate write cmd to Read only replica")
		}

		s.ExecuteCmd(conn, cmd, cmdArgs[1:])

		if isWrite(cmd) && s.config.Role == "master" {
			s.propagateWrite(s.replica, cmdArgs)
		}

		if err != nil {
			log.Printf("Error handling command %s: %v", cmd, err)
			return
		}
	}
}

// replica id then forward the cmd
func (s *RedisServer) propagateWrite(replica map[string]net.Conn, cmds []string) {
	for _, v := range replica {
		v.Write([]byte(s.protocol.stringToArray(cmds)))
	}
}

func (s *RedisServer) AddReplica(id string, conn net.Conn) {
	s.replicasMu.Lock()
	defer s.replicasMu.Unlock()

	s.replica[id] = conn
}

// RemoveReplica removes a replica connection
func (s *RedisServer) RemoveReplica(id string) {
	s.replicasMu.Lock()
	defer s.replicasMu.Unlock()

	delete(s.replica, id)
}
