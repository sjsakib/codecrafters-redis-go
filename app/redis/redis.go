package redis

import (
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

	go func() {
		for req := range requestChan {
			result := "+PONG\r\n"
			req.res <- result
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

		command := []string{string(buffer[:n])}

		resChan := make(chan string)
		requestChan <- Request{
			Command: command,
			res:     resChan,
		}

		response := <-resChan
		conn.Write([]byte(response))

	}
}
