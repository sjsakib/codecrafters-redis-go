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
	storage          Storage
	commandQueues    map[string][]*RawReq
	isExecutingMulti bool
}

func NewEngine(storage Storage) Engine {
	return &engine{
		storage:       storage,
		commandQueues: make(map[string][]*RawReq),
	}
}

func (e *engine) Handle(req *RawReq) *RawResp {
	resp := RawResp{}

	if req.command == nil {
		command, err := parseCommand(req.input)
		if err != nil {
			resp.Data = encodeErrorMessage(fmt.Sprintf("Failed to parse command: %s", err))
			return &resp
		}

		req.command = command
	}

	if e.queueIfMulti(req) {
		resp.Data = encodeSimpleString("QUEUED")
		return &resp
	}

	command := req.command

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
		return e.handleXRead(req)
	case "INCR":
		resp.Data = e.handleIncr(command)
	case "MULTI":
		return e.handleMulti(req)
	case "EXEC":
		return e.handleExec(req.connId)
	case "DISCARD":
		return e.handleDiscard(req.connId)
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

func (e *engine) handleXRead(req *RawReq) *RawResp {
	resp := RawResp{}
	if req.command == nil {
		command, err := parseCommand(req.input)
		if err != nil {
			resp.Data = encodeErrorMessage(fmt.Sprintf("Failed to parse command: %s", err))
			return &resp
		}
		req.command = command
	}

	command := req.command

	keys := make([]string, 0)
	ids := make([]EntryID, 0)

	i := 1

	isBlocking := false
	if strings.ToUpper(command[i]) == "BLOCK" {
		if len(command) < 6 {
			resp.Data = encodeInvalidArgCount(command[0])
			return &resp
		}
		isBlocking = true
		var timeoutMs int
		_, err := fmt.Sscanf(command[i+1], "%d", &timeoutMs)
		if err != nil {
			resp.Data = encodeErrorMessage("BLOCK time must be an integer")
			return &resp
		}
		if timeoutMs < 0 {
			resp.Data = encodeErrorMessage("BLOCK time must be non-negative")
			return &resp
		}
		timeoutTime := req.timeStamp.Add(time.Duration(timeoutMs) * time.Millisecond)
		if timeoutMs != 0 && timeoutTime.Before(time.Now()) {
			resp.Data = encodeNullArray()
			return &resp
		}

		i += 2
	}

	if strings.ToUpper(command[i]) != "STREAMS" {
		resp.Data = encodeInvalidArgCount(command[0])

		return &resp
	}

	i++

	keyCount := (len(command) - i) / 2

	for ; i < len(command)-keyCount; i++ {
		keys = append(keys, command[i])
	}
	for ; i < len(command); i++ {
		idStr := command[i]
		entryID := EntryID{}
		if idStr == "$" {
			id, err := e.storage.GetStreamTopID(command[i-keyCount])
			if err != nil {
				resp.Data = encodeError(err)
				return &resp
			}
			command[i] = fmt.Sprintf("%d-%d", id.T, id.S)
			ids = append(ids, *id)
			continue
		}
		_, err := fmt.Sscanf(idStr, "%d-%d", &entryID.T, &entryID.S)
		if err != nil {
			resp.Data = encodeError(err)
			return &resp
		}
		ids = append(ids, entryID)
	}

	result := make([]any, 0)

	hasData := false
	for idx, streamKey := range keys {
		stream, err := e.storage.GetStream(streamKey)
		if err != nil {
			if err == ErrKeyNotFound {
				continue
			}
			resp.Data = encodeError(err)
			return &resp
		}
		entries := stream.GetAfterID(ids[idx])
		if len(entries) > 0 {
			hasData = true
		}
		result = append(result, []any{streamKey, entries})
	}

	if hasData {
		resp.Data = encodeResp(result)
	} else if isBlocking {
		wait := (time.Duration(100) * time.Millisecond)
		resp.RetryWait = &wait
	}

	return &resp
}

func (e *engine) handleIncr(command []string) []byte {
	if len(command) < 2 {
		return encodeInvalidArgCount(command[0])
	}

	value, exists := e.storage.Get(command[1])
	if !exists {
		e.storage.Set(command[1], "1")
		return encodeResp(1)
	}

	strValue, ok := value.(string)
	if !ok {
		return encodeErrorMessage("value is not an integer or out of range")
	}

	var intValue int
	_, err := fmt.Sscanf(strValue, "%d", &intValue)
	if err != nil {
		return encodeErrorMessage("value is not an integer or out of range")
	}

	intValue += 1
	e.storage.Set(command[1], fmt.Sprintf("%d", intValue))
	return encodeResp(intValue)
}

func (e *engine) handleMulti(req *RawReq) *RawResp {
	_, exists := e.commandQueues[req.connId]
	if !exists {
		e.commandQueues[req.connId] = make([]*RawReq, 0)
	} else {
		return &RawResp{Data: encodeErrorMessage("MULTI calls cannot be nested")}
	}
	return &RawResp{Data: encodeSimpleString("OK")}
}

func (e *engine) queueIfMulti(req *RawReq) bool {
	if req.command[0] == "EXEC" || req.command[0] == "MULTI" || e.isExecutingMulti {
		return false
	}
	queue, exists := e.commandQueues[req.connId]
	if exists {
		e.commandQueues[req.connId] = append(queue, req)
	}
	return exists
}

func (e *engine) handleExec(connId string) *RawResp {
	_, exists := e.commandQueues[connId]
	if !exists {
		return &RawResp{Data: encodeErrorMessage("EXEC without MULTI")}
	}

	responses := make([][]byte, 0)

	e.isExecutingMulti = true
	defer func() { e.isExecutingMulti = false }()

	for _, queuedReq := range e.commandQueues[connId] {
		resp := e.Handle(queuedReq)
		responses = append(responses, resp.Data)
	}

	delete(e.commandQueues, connId)

	return &RawResp{Data: encodeArray(responses)}
}

func (e *engine) handleDiscard(connId string) *RawResp {
	_, exists := e.commandQueues[connId]
	if !exists {
		return &RawResp{Data: encodeErrorMessage("DISCARD without MULTI")}
	}
	
	delete(e.commandQueues, connId)
	return &RawResp{Data: encodeSimpleString("OK")}
}
