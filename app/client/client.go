package client

import (
	"bytes"
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type Client interface {
	Send(command []string) error
	Do(command []string) (any, error)
	Read() ([]byte, error)
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

func (c *tcpClient) Do(command []string) (any, error) {
	err := c.Send(command)

	if err != nil {
		return nil, fmt.Errorf("failed to send command: %w", err)
	}

	response, err := c.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	data, err := resp.Parse(bytes.NewReader(response))
	if err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return data, nil
}

func (c *tcpClient) Send(command []string) error {
	if c.conn == nil {
		conn, err := net.Dial("tcp", c.address)
		if err != nil {
			return fmt.Errorf("failed to dial tcp: %w", err)
		}
		c.conn = conn
	}

	_, err := c.conn.Write(resp.EncodeResp(command))
	if err != nil {
		return fmt.Errorf("failed to write to connection: %w", err)
	}

	return nil
}

func (c *tcpClient) Read() ([]byte, error) {
	var buffer [1024]byte
	n, err := c.conn.Read(buffer[:])

	if err != nil {
		if err.Error() == "EOF" {
			return nil, err
		}
		return nil, fmt.Errorf("failed to read from connection: %w", err)
	}

	return buffer[:n], nil

}
