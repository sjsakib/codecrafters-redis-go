package server

import (
	"fmt"
	"net"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/engine"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type Server interface {
	Start(address string) error
}

type goroutineServer struct {
	engine engine.Engine
}

func NewServer(engine engine.Engine) Server {
	return &goroutineServer{
		engine: engine,
	}
}

func (s *goroutineServer) Start(address string) error {

	err := s.engine.StartLoop()
	if err != nil {
		return fmt.Errorf("failed to start engine loop: %s", err)
	}

	l, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on port 6379: %s", err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			return fmt.Errorf("failed to accept connection: %s", err)
		}

		go s.handleConnection(conn)
	}
}

func (s *goroutineServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 1024) // if the command exceeds 1024 bytes, it will be truncated for now

	connId := utils.RandomID()

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

		timeStamp := time.Now()

		resChan := make(chan *engine.Response)
		req := engine.Request{
			Input:     buffer[:n],
			ResCh:     resChan,
			Timestamp: timeStamp,
			ConnId:    connId,
		}

		s.engine.ReqCh() <- &req

		go func() {
			for {
				response, ok := <-resChan
				if !ok {
					break
				}
				conn.Write(response.Data)
			}
		}()

	}
}
