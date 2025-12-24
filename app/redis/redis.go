package redis

import (
	"bytes"
	"fmt"
	"net"
)

type Server interface {
	Start() error
}

type server struct {
}

func NewServer() Server {
	return &server{}
}

type Request struct {
	Command []string
	res     chan string
}

func (r *server) Start() error {
	requestChan := make(chan Request, 100)

	storage := make(map[string]string)

	go func() {
		for req := range requestChan {
			switch req.Command[0] {
			case "PING":
				req.res <- "+PONG\r\n"
			case "ECHO":
				if len(req.Command) < 2 {
					req.res <- "-ERR wrong number of arguments for 'ECHO' command\r\n"
					continue
				}
				result := fmt.Sprintf("%s", encodeBulkString(req.Command[1]))
				req.res <- result
			case "SET":
				if len(req.Command) < 3 {
					req.res <- "-ERR wrong number of arguments for 'SET' command\r\n"
					continue
				}
				storage[req.Command[1]] = req.Command[2]
				req.res <- "+OK\r\n"
			case "GET":
				if len(req.Command) < 2 {
					req.res <- "-ERR wrong number of arguments for 'GET' command\r\n"
					continue
				}
				value, exists := storage[req.Command[1]]
				if !exists {
					req.res <- "$-1\r\n"
					continue
				}
				result := fmt.Sprintf("%s", encodeBulkString(value))
				req.res <- result
			default:
				req.res <- "-ERR unknown command\r\n"
			}
		}
	}()

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		return fmt.Errorf("failed to listen on port 6379: %s", err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			return fmt.Errorf("failed to accept connection: %s", err)
		}

		fmt.Printf("Got new connection from %s\n", conn.LocalAddr())

		go r.handleConnection(conn, requestChan)
	}
}

func (r *server) handleConnection(conn net.Conn, requestChan chan Request) {
	defer conn.Close()
	buffer := make([]byte, 1024) // if the command exceeds 1024 bytes, it will be truncated for now

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err.Error() != "EOF" {
				fmt.Printf("Error reading from connection: %s\n", err)
			} else {
				fmt.Printf("Connection from %s closed by client\n", conn.LocalAddr())
			}
			return
		}

		command, err := parseCommand(bytes.NewReader(buffer[:n]))

		if err != nil {
			fmt.Printf("Error parsing command: %s\n", err)
			return
		}

		resChan := make(chan string)
		requestChan <- Request{
			Command: command,
			res:     resChan,
		}

		response := <-resChan
		conn.Write([]byte(response))

	}
}
