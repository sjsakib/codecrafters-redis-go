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
		return fmt.Appendf(nil, "-ERR Failed to parse command: %s\r\n", err)
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
			return encodeNull()
		}
		result := encodeResp(value)
		return []byte(result)
	case "RPUSH":
		return e.handleRPushCommand(command)
	case "LRANGE":
		return e.handleLRangeCommand(command)
	case "LPUSH":
		return e.handleLPushCommand(command)
	case "LLEN":
		return e.handleLLen(command)
	case "LPOP":
		return e.handleLPopCommand(command)
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

func (e *engine) handleRPushCommand(command []string) []byte {
	if len(command) < 3 {
		return []byte("-ERR wrong number of arguments for 'RPUSH' command\r\n")
	}

	list, err := e.storage.GetOrMakeList(command[1])

	if err != nil {
		return encodeError(err)
	}
	for i := 2; i < len(command); i++ {
		list = append(list, command[i])
	}
	e.storage.Set(command[1], list)
	return []byte(encodeResp(len(list)))
}

func (e *engine) handleLPushCommand(command []string) []byte {
	if len(command) < 3 {
		return []byte("-ERR wrong number of arguments for 'LPUSH' command\r\n")
	}
	list, err := e.storage.GetOrMakeList(command[1])
	if err != nil {
		return encodeError(err)
	}
	for i := 2; i < len(command); i++ {
		list = append([]any{command[i]}, list...)
	}
	e.storage.Set(command[1], list)
	return []byte(encodeResp(len(list)))
}

func (e *engine) handleLRangeCommand(command []string) []byte {
	if len(command) < 4 {
		return []byte("-ERR wrong number of arguments for 'LRANGE' command\r\n")
	}
	existingValue, ok := e.storage.Get(command[1])
	if !ok {
		return []byte(encodeResp([]any{}))
	}
	list, ok := existingValue.([]any)
	if !ok {
		return []byte("-ERR value is not a list\r\n")
	}
	var start, end int
	fmt.Sscanf(command[2], "%d", &start)
	fmt.Sscanf(command[3], "%d", &end)

	if start < 0 {
		start = len(list) + start
	}
	if end < 0 {
		end = len(list) + end
	}
	if start < 0 {
		start = 0
	}
	if end >= len(list) {
		end = len(list) - 1
	}
	if start > end || start >= len(list) {
		return []byte(encodeResp([]any{}))
	}

	return []byte(encodeResp(list[start : end+1]))

}

func (e *engine) handleLLen(command []string) []byte {
	if len(command) < 2 {
		return []byte("-ERR wrong number of arguments for 'LLEN' command\r\n")
	}
	list, err := e.storage.GetOrMakeList(command[1])

	if err != nil {
		return encodeError(err)
	}
	return []byte(encodeResp(len(list)))

}

func (e *engine) handleLPopCommand(command []string) []byte {
	if len(command) < 2 {
		return []byte("-ERR wrong number of arguments for 'LPOP' command\r\n")
	}
	existingValue, ok := e.storage.Get(command[1])
	if !ok {
		return encodeNull()
	}
	list, ok := existingValue.([]any)
	if !ok {
		return encodeError(ErrWrongType)
	}
	if len(list) == 0 {
		return encodeNull()
	}

	popCount := 1
	if len(command) >= 3 {
		_, err := fmt.Sscanf(command[2], "%d", &popCount)
		if err != nil {
			return []byte("-ERR count must be an integer\r\n")
		}
		if popCount < 1 {
			return []byte("-ERR count must be positive\r\n")
		}
		if popCount > len(list) {
			popCount = len(list)
		}
	}

	poppedValues := list[:popCount]
	list = list[popCount:]
	e.storage.Set(command[1], list)

	if popCount == 1 {
		return []byte(encodeResp(poppedValues[0]))
	}
	return []byte(encodeResp(poppedValues))
}
