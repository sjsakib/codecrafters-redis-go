package redis

import (
	"fmt"
	"strings"
	"time"
)

type Engine interface {
	Handle(req *RawReq) *RawResp
}

type engine struct {
	storage Storage
}

func NewEngine(storage Storage) Engine {
	return &engine{
		storage: storage,
	}
}

func (e *engine) Handle(req *RawReq) *RawResp {
	command, err := parseCommand(req.input)

	resp := RawResp{}

	if err != nil {
		resp.Data = encodeErrorMessage(fmt.Sprintf("Failed to parse command: %s", err))
		return &resp
	}

	switch command[0] {
	case "PING":
		resp.Data = encodeSimpleString("PONG")
	case "ECHO":
		if len(command) < 2 {
			resp.Data = encodeErrorMessage("wrong number of arguments for 'ECHO' command")
		}
		resp.Data = encodeBulkString(command[1])
	case "SET":
		resp.Data = e.handleSetCommand(command)
	case "GET":
		if len(command) < 2 {
			resp.Data = encodeInvalidArgCount("GET")
			break
		}
		value, exists := e.storage.Get(command[1])
		if !exists {
			resp.Data = encodeNull()
			break
		}
		resp.Data = encodeResp(value)
	case "RPUSH":
		resp.Data = e.handleRPushCommand(command)
	case "LRANGE":
		resp.Data = e.handleLRangeCommand(command)
	case "LPUSH":
		resp.Data = e.handleLPushCommand(command)
	case "LLEN":
		resp.Data = e.handleLLen(command)
	case "LPOP":
		resp.Data = e.handleLPopCommand(command)
	case "BLPOP":
		return e.handleBLPop(req)
	case "TYPE":
		resp.Data = e.handleType(command)
	case "XADD":
		resp.Data = e.handleXAdd(command)
	case "XRANGE":
		resp.Data = e.handleXRange(command)
	case "XREAD":
		resp.Data = e.handleXRead(command)
	default:
		resp.Data = encodeErrorMessage("unknown command: " + command[0])
	}

	return &resp
}

func (e *engine) handleSetCommand(command []string) []byte {
	if len(command) < 3 {
		return encodeInvalidArgCount("SET")
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
		return encodeInvalidArgCount(command[0])
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
		return encodeInvalidArgCount(command[0])
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
		return encodeInvalidArgCount("LRANGE")
	}
	existingValue, ok := e.storage.Get(command[1])
	if !ok {
		return []byte(encodeResp([]any{}))
	}
	list, ok := existingValue.([]any)
	if !ok {
		return encodeErrorMessage("value is not a list")
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
		return encodeInvalidArgCount(command[0])
	}
	list, err := e.storage.GetOrMakeList(command[1])

	if err != nil {
		return encodeError(err)
	}
	return []byte(encodeResp(len(list)))

}

func (e *engine) handleLPopCommand(command []string) []byte {
	if len(command) < 2 {
		return encodeInvalidArgCount(command[0])
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
			return encodeErrorMessage("count must be an integer")
		}
		if popCount < 1 {
			return encodeErrorMessage("count must be positive")
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

func (e *engine) handleBLPop(req *RawReq) *RawResp {
	command, err := parseCommand(req.input)
	resp := RawResp{}
	if len(command) < 3 {
		resp.Data = encodeInvalidArgCount(command[0])
		return &resp
	}

	var timeoutSec float32
	_, err = fmt.Sscanf(command[2], "%f", &timeoutSec)
	if err != nil {
		resp.Data = encodeErrorMessage("timeout must be an integer")
		return &resp
	}
	if timeoutSec < 0 {
		resp.Data = encodeErrorMessage("timeout must be non-negative")
		return &resp
	}

	timeoutTime := req.timeStamp.Add(time.Duration(timeoutSec*1000) * time.Millisecond)

	if timeoutSec != 0 && timeoutTime.Before(time.Now()) {
		resp.Data = encodeNullArray()
		return &resp
	}

	list, err := e.storage.GetOrMakeList(command[1])

	if err != nil {
		resp.Data = encodeError(err)
		return &resp
	}

	if len(list) > 0 {
		poppedValue := list[0]
		list = list[1:]
		e.storage.Set(command[1], list)
		resp.Data = encodeResp([]any{command[1], poppedValue})
		return &resp
	}

	wait := (time.Duration(100) * time.Millisecond)
	resp.RetryWait = &wait

	return &resp
}

func (e *engine) handleType(command []string) []byte {
	if len(command) < 2 {
		return encodeInvalidArgCount(command[0])
	}

	value, exists := e.storage.Get(command[1])
	if !exists {
		return encodeSimpleString("none")
	}

	switch value.(type) {
	case string:
		return encodeSimpleString("string")
	case []any:
		return encodeSimpleString("list")
	case *Stream:
		return encodeSimpleString("stream")
	default:
		return encodeSimpleString("unknown")
	}
}

func (e *engine) handleXAdd(command []string) []byte {
	if len(command) < 2 {
		return encodeInvalidArgCount(command[0])
	}

	stream, err := e.storage.GetOrMakeStream(command[1])
	if err != nil {
		return encodeError(err)
	}

	entry := StreamEntry{
		ID:     EntryID{},
		Fields: make(map[string]string),
	}

	validatedID, err := stream.GenerateOrValidateEntryID(command[2])
	if err != nil {
		return encodeErrorMessage(err.Error())
	}
	entry.ID = *validatedID

	for i := 3; i < len(command); i += 2 {
		if i+1 >= len(command) {
			return encodeErrorMessage("XADD requires field-value pairs")
		}
		entry.Fields[command[i]] = command[i+1]
	}

	stream.Entries = append(stream.Entries, entry)
	e.storage.Set(command[1], stream)

	return encodeResp(entry.ID)
}

func (e *engine) handleXRange(command []string) []byte {
	if len(command) < 4 {
		return encodeInvalidArgCount(command[0])
	}

	stream, err := e.storage.GetOrMakeStream(command[1])
	if err != nil {
		return encodeError(err)
	}

	startID, err := parseRangeID(command[2], true)
	if err != nil {
		return encodeError(err)
	}
	endID, err := parseRangeID(command[3], false)
	if err != nil {
		return encodeError(err)
	}

	entries := stream.GetRange(startID, &endID)

	return encodeResp(entries)
}

func (e *engine) handleXRead(command []string) []byte {
	if len(command) < 4 || strings.ToUpper(command[1]) != "STREAMS" {
		return encodeInvalidArgCount(command[0])
	}

	streamKey := command[2]

	stream, err := e.storage.GetStream(streamKey)
	if err != nil {
		return encodeError(err)
	}

	idStr := command[3]
	entryID := EntryID{}
	_, err = fmt.Sscanf(idStr, "%d-%d", &entryID.T, &entryID.S)
	if err != nil {
		return encodeError(err)
	}
	entries := stream.GetRange(entryID, nil)
	return encodeResp([]any{[]any{streamKey,entries}})

}
