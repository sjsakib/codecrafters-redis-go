package redis

import (
	"bytes"
	"fmt"
	"net"
)

type Client interface {
	Send(command []string) (any, error)
}

type tcpClient struct {
	address string
	conn    net.Conn
}

func NewClient(address string) Client {
	return &tcpClient{
		address: address,
		conn:    nil,
	}
}

func (c *tcpClient) Send(command []string) (any, error) {
	if c.conn == nil {
		conn, err := net.Dial("tcp", c.address)
		if err != nil {
			return nil, fmt.Errorf("failed to dial tcp: %w", err)
		}
		c.conn = conn
	}

	_, err := c.conn.Write(encodeResp(command))
	if err != nil {
		return nil, fmt.Errorf("failed to write to connection: %w", err)
	}

	var respBuffer [1024]byte
	n, err := c.conn.Read(respBuffer[:])
	if err != nil {
		return nil, fmt.Errorf("failed to read from connection: %w", err)
	}

	data, err := parse(bytes.NewReader(respBuffer[:n]))
	if err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return data, nil
}
