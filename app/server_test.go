package main

import (
	"net"
	"strings"
	"testing"
	"time"
)

// Create a TCP connection helper function
func createTestConn(t *testing.T) (net.Conn, net.Conn) {
	server, client := net.Pipe() // Simulate a network connection
	go handleReq(server)         // Run handler in a goroutine
	return client, server
}

// ✅ Test PING
func TestHandleReq_PING(t *testing.T) {
	client, server := createTestConn(t)
	defer client.Close()
	defer server.Close()

	client.Write([]byte("*1\r\n$4\r\nPING\r\n"))

	buf := make([]byte, 1024)
	n, _ := client.Read(buf)
	response := string(buf[:n])

	expected := "+PONG\r\n"
	if response != expected {
		t.Errorf("Expected '%s', got '%s'", expected, response)
	}
}

// ✅ Test ECHO
func TestHandleReq_ECHO(t *testing.T) {
	client, server := createTestConn(t)
	defer client.Close()
	defer server.Close()

	client.Write([]byte("*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"))

	buf := make([]byte, 1024)
	n, err := client.Read(buf)
	if err != nil {
		t.Fatalf("Error reading response: %v", err)
	}

	response := string(buf[:n])

	// Debugging output
	t.Logf("Received response: %q", response)

	// Extract the actual echoed message from RESP format
	parts := strings.Split(response, "\r\n")
	if len(parts) < 3 || parts[0][0] != '$' {
		t.Errorf("Unexpected response format: %s", response)
		return
	}

	actualMessage := parts[2] // Extract the echoed value
	if actualMessage != "hello" {
		t.Errorf("Expected 'hello', got '%s'", actualMessage)
	}
}

// ✅ Test SET and GET
func TestHandleReq_SET_GET(t *testing.T) {
	client, server := createTestConn(t)
	defer client.Close()
	defer server.Close()

	// Send SET command
	client.Write([]byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"))

	buf := make([]byte, 1024)
	n, _ := client.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		t.Errorf("Expected +OK response for SET")
	}

	// Allow server time before sending GET
	time.Sleep(10 * time.Millisecond)

	// Send GET command
	client.Write([]byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"))
	n, _ = client.Read(buf)
	expected := "$5\r\nvalue\r\n"

	if string(buf[:n]) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(buf[:n]))
	}
}

// ✅ Test Expiry Functionality
func TestHandleReq_Expiry(t *testing.T) {
	client, server := createTestConn(t)
	defer client.Close()
	defer server.Close()

	// Send SET command with expiry (100ms)
	client.Write([]byte("*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\npx\r\n$3\r\n100\r\n"))
	buf := make([]byte, 1024)
	n, _ := client.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		t.Errorf("Expected +OK response for SET with expiry")
	}

	// Wait for key to expire
	time.Sleep(150 * time.Millisecond)

	// Try GET command
	client.Write([]byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"))
	n, _ = client.Read(buf)
	expected := "$-1\r\n" // Key should not exist
	if string(buf[:n]) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(buf[:n]))
	}
}
