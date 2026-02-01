package redis

import (
	"fmt"
	"net"
	"time"
)

type RawReq struct {
	input              []byte
	command            []string
	resCh              chan *RawResp
	timeStamp          time.Time
	connId             string
	isTimeoutScheduled bool
}

type RawResp struct {
	Data      []byte
	RetryWait *time.Duration
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

	m.engine.StartLoop()

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

		go m.handleConnection(conn, m.engine.ReqCh())
	}
}

func (m *goroutineMux) handleConnection(conn net.Conn, requestChan chan *RawReq) {
	defer conn.Close()
	buffer := make([]byte, 1024) // if the command exceeds 1024 bytes, it will be truncated for now

	connId := randomID()

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

		resChan := make(chan *RawResp)
		req := RawReq{
			input:     buffer[:n],
			resCh:     resChan,
			timeStamp: timeStamp,
			connId:    connId,
		}

		requestChan <- &req

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
