package redis

import (
	"bytes"
	"fmt"
	"time"
)

type Engine interface {
	Handle(input []byte) []byte
}

type engine struct {
	storage Storage
}

func NewEngine(storage Storage) Engine {
	return &engine{
		storage: storage,
	}
}

func (e *engine) Handle(input []byte) []byte {
	command, err := parseCommand(bytes.NewReader(input))

	if err != nil {
		return []byte(fmt.Sprintf("-ERR Failed to parse command: %s\r\n", err))
	}

	switch command[0] {
	case "PING":
		return []byte("+PONG\r\n")
	case "ECHO":
		if len(command) < 2 {
			return []byte("-ERR wrong number of arguments for 'ECHO' command\r\n")
		}
		result := encodeBulkString(command[1])
		return []byte(result)
	case "SET":
		return e.handleSetCommand(command)
	case "GET":
		if len(command) < 2 {
			return []byte("-ERR wrong number of arguments for 'GET' command\r\n")
		}
		value, exists := e.storage.Get(command[1])
		if !exists {
			return []byte("$-1\r\n")
		}
		result := fmt.Sprintf("%s", encodeBulkString(value))
		return []byte(result)
	default:
		return []byte("-ERR unknown command\r\n")
	}
}

func (e *engine) handleSetCommand(command []string) []byte {
	if len(command) < 3 {
		return []byte("-ERR wrong number of arguments for 'SET' command\r\n")
	}

	e.storage.Set(command[1], command[2])
	if len(command) > 3 && (command[3] == "EX" || command[3] == "PX") {
		if len(command) < 5 {
			return []byte("-ERR wrong number of arguments for 'SET' command with expiry\r\n")
		}
		var duration int
		fmt.Sscanf(command[4], "%d", &duration)
		if command[3] == "EX" {
			e.storage.Expire(command[1], time.Duration(duration)*time.Second)
		} else {
			e.storage.Expire(command[1], time.Duration(duration)*time.Millisecond)
		}
	}
	return []byte("+OK\r\n")

}
