package redis

import (
	"fmt"
	"net"
)

type RawRequest struct {
	input []byte
	res   chan []byte
}

type Server interface {
	Start(address string) error
}

type goroutineMux struct {
	engine Engine
}

func NewServer(engine Engine) Server {
	return &goroutineMux{
		engine: engine,
	}
}

func (m *goroutineMux) Start(address string) error {
	requestChan := make(chan RawRequest, 100)

	go func() {
		for req := range requestChan {
			resp := m.engine.Handle(req.input)

			req.res <- resp

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

		go m.handleConnection(conn, requestChan)
	}
}

func (m *goroutineMux) handleConnection(conn net.Conn, requestChan chan RawRequest) {
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

		resChan := make(chan []byte)
		requestChan <- RawRequest{
			input: buffer[:n],
			res:   resChan,
		}

		response := <-resChan
		conn.Write([]byte(response))

	}
}
