package main

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

type ReplicationManager struct {
	config   *Config
	protocol *ProtocolHandler

	replica    map[string]net.Conn
	replicasMu sync.RWMutex

	masterConn net.Conn
	masterMu   sync.RWMutex
}

func NewReplicationManager(config *Config, protocol *ProtocolHandler) *ReplicationManager {
	return &ReplicationManager{
		config:   config,
		protocol: protocol,
		replica:  make(map[string]net.Conn),
	}
}

/*
Who should be using this function?

the replica should be using this function
*/

func (r *ReplicationManager) startReplication() {
	for {
		fmt.Print("connectToMaster")
		address := r.config.MasterAddr
		conn, err := net.Dial("tcp", address)
		if err != nil {
			fmt.Printf("Failed to bind to %s\n, retrying", address)
			time.Sleep(5 * time.Second)
			continue
		}

		r.masterConn = conn

		err = r.setUpReplication()
		if err != nil {
			fmt.Printf("Failed to set up replication %s", address)
			time.Sleep(5 * time.Second)
			continue
		}

		err = r.processReplicationStream(conn)
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

/*
Who should be using this function?

the replica should be using this function
Replicas connect to masters to receive data changes
Masters don't connect to replicas; the connection is always initiated by the replica
*/
func (r *ReplicationManager) setUpReplication() error {
	conn := r.masterConn

	r.sendPING(conn)
	r.sendREPLCONF(conn, os.Args[2])
	r.sendPSYNC(conn)

	return nil
}

func (r *ReplicationManager) processReplicationStream(conn net.Conn) error {
	return nil
}

func (r *ReplicationManager) readResponse(conn net.Conn) ([]byte, error) {
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	return buffer[:n], nil
}

func (r *ReplicationManager) isWrite(cmd string) bool {
	return cmd == "SET" || cmd == "DEL"
}

func (r *ReplicationManager) sendPING(conn net.Conn) error {
	cmd := []string{"PING"}
	_, err := conn.Write([]byte(r.protocol.stringToArray(cmd)))
	if err != nil {
		return err
	}
	r.readResponse(conn)
	return err
}

func (r *ReplicationManager) sendREPLCONF(conn net.Conn, localPort string) error {
	// since os
	cmd := []string{"REPLCONF", "listening-port", localPort}
	_, err := conn.Write([]byte(r.protocol.stringToArray(cmd)))
	if err != nil {
		return err
	}
	r.readResponse(conn)
	//HARDCODED
	cmd = []string{"REPLCONF", "capa", "psync2"}
	_, err = conn.Write([]byte(r.protocol.stringToArray(cmd)))

	r.readResponse(conn)

	return err

}
func (r *ReplicationManager) sendPSYNC(conn net.Conn) error {
	cmd := []string{"PSYNC", "?", "-1"}
	_, err := conn.Write([]byte(r.protocol.stringToArray(cmd)))
	if err != nil {
		return err
	}
	// HARDCODED
	// this is the empty file
	r.readResponse(conn)
	return err
}
