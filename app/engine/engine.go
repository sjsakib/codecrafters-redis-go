package engine

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/client"
	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/storage"
)

type Engine interface {
	Handle(req *Request) *Response
	StartLoop() error
	ReqCh() chan *Request
	SetConfig(config Config)
}

type engine struct {
	storage storage.Storage

	config Config

	reqCh     chan *Request
	timeoutCh chan *TimeOut

	// replication
	replicationInfo ReplicationInfo
	slaveReqs       []*Request
	ackMap          map[string]*WaitReq

	commandQueues    map[string][]*Request
	isExecutingMulti bool

	blockedReqs []*Request

	// pub/sub
	channels map[string][]*Request
	subCount map[string]int
}

func NewEngine(storage storage.Storage, masterAddress string) Engine {
	return &engine{
		storage:       storage,
		reqCh:         make(chan *Request, 100),
		commandQueues: make(map[string][]*Request),
		config:        Config{},
		replicationInfo: ReplicationInfo{
			MasterAddress: masterAddress,
			ReplicationId: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			Offset:        0,
		},
		blockedReqs: make([]*Request, 0),
		timeoutCh:   make(chan *TimeOut, 100),
		slaveReqs:   make([]*Request, 0),
		ackMap:      make(map[string]*WaitReq),
		channels:    make(map[string][]*Request),
		subCount:    make(map[string]int),
	}
}

func (e *engine) SetConfig(config Config) {
	e.config = config
}

func (e *engine) StartLoop() error {

	err := e.LoadRDBFileIfPresent()
	if err != nil {
		fmt.Println("Failed to load RDB file:", err)
	}

	err = e.startReplicationIfSlave()
	if err != nil {
		return fmt.Errorf("Failed to start replication: %w\n", err)
	}
	go func() {
		for {
			select {
			case to := <-e.timeoutCh:
				e.handleTimeout(to)
			case req, ok := <-e.reqCh:
				if !ok {
					return
				}
				e.Handle(req)
			}
		}
	}()

	return nil
}

func (e *engine) ReqCh() chan *Request {
	return e.reqCh
}

func (e *engine) Handle(req *Request) *Response {
	response := Response{}

	if req.Command == nil {
		command, err := resp.ParseCommand(req.Input)
		if err != nil {
			response.Data = resp.EncodeErrorMessage(fmt.Sprintf("Failed to parse command: %s", err))
			return &response
		}

		req.Command = command
	}

	if e.queueIfMulti(req) {
		response.Data = resp.EncodeSimpleString("QUEUED")
		req.ResCh <- &response
		close(req.ResCh)
		return &response
	}

	if !e.verifySubscribed(req) {
		errMsg := fmt.Sprintf(
			"Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
			strings.ToLower(req.Command[0]))
		response.Data = resp.EncodeErrorMessage(errMsg)
		req.ResCh <- &response
		return &response
	}

	command := req.Command

	shouldClose := true

	switch Command(command[0]) {
	case CmdPing:
		if e.isSubscribed(req) {
			response.Data = resp.EncodeArray([]string{"pong", ""})
		} else {
			response.Data = resp.EncodeSimpleString("PONG")
		}

	case CmdEcho:
		if len(command) < 2 {
			response.Data = resp.EncodeErrorMessage("wrong number of arguments for 'ECHO' command")
		}
		response.Data = resp.EncodeBulkString(command[1])
	case CmdSet:
		response.Data = e.handleSetCommand(command)
	case CmdGet:
		if len(command) < 2 {
			response.Data = resp.EncodeInvalidArgCount("GET")
			break
		}
		value, exists := e.storage.Get(command[1])
		if !exists {
			response.Data = resp.EncodeNull()
			break
		}
		response.Data = resp.EncodeResp(value)
	case CmdRPush:
		response.Data = e.handleRPushCommand(command)
		defer e.handleBlockedCommands(CmdBLPop)
	case CmdLPush:
		response.Data = e.handleLPushCommand(command)
		defer e.handleBlockedCommands(CmdBLPop)
	case CmdLRange:
		response.Data = e.handleLRangeCommand(command)
	case CmdLLen:
		response.Data = e.handleLLen(command)
	case CmdLPop:
		response.Data = e.handleLPopCommand(command)
	case CmdBLPop:
		response.Data = e.handleBLPop(req)
	case CmdType:
		response.Data = e.handleType(command)
	case CmdXAdd:
		response.Data = e.handleXAdd(command)
		defer e.handleBlockedCommands(CmdXRead)
	case CmdXRange:
		response.Data = e.handleXRange(command)
	case CmdXRead:
		response.Data = e.handleXRead(req)
	case CmdIncr:
		response.Data = e.handleIncr(command)
	case CmdMulti:
		response.Data = e.handleMulti(req)
	case CmdExec:
		response.Data = e.handleExec(req)
	case CmdDiscard:
		response.Data = e.handleDiscard(req.ConnId)
	case CmdInfo:
		response.Data = e.handleInfo(command)
	case CmdReplConf:
		// For simplicity, we just acknowledge these commands without actual replication logic
		return e.handleReplConf(req)
	case CmdPsync:
		// For simplicity, we just acknowledge these commands without actual replication logic
		response.Data = resp.EncodeSimpleString("FULLRESYNC " + e.replicationInfo.ReplicationId + " 0")

		response.Data = append(response.Data, []byte("$88\r\n")...)
		response.Data = append(response.Data, []byte{0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2}...)
		e.slaveReqs = append(e.slaveReqs, req)
		shouldClose = false
	case CmdWait:
		response.Data = e.handleWait(req)
	case CmdConfig:
		response.Data = e.handleConfig(command)
	case CmdKeys:
		response.Data = e.handleKeys(command)
	case CmdSubscribe:
		response.Data = e.handleSubscribe(req)
	case CmdPublish:
		response.Data = e.handlePub(command)
	case CmdUnsubscribe:
		response.Data = e.handleUnsubscribe(req)
	case CmdZAdd:
		response.Data = e.handleZAdd(command)
	default:
		response.Data = resp.EncodeErrorMessage("unknown command: " + command[0])
	}

	if !e.isExecutingMulti && response.Data != nil {
		req.ResCh <- &response
		if shouldClose {
			close(req.ResCh)
		}
	}

	if isWriteCommand(command[0]) && e.IsMaster() {
		cmdBytes := resp.EncodeResp(command)
		e.replicationInfo.Offset += int64(len(cmdBytes))
		for _, slaveReq := range e.slaveReqs {
			slaveReq.ResCh <- &Response{Data: cmdBytes}
		}
	}

	return &response
}

func (e *engine) timeoutReq(req *Request, duration time.Duration) {
	if req.IsTimeoutScheduled {
		return
	}
	if duration == 0 {
		return
	}
	req.IsTimeoutScheduled = true
	to := &TimeOut{
		Req: req,
	}
	time.AfterFunc(duration, func() {
		e.timeoutCh <- to
	})
}

func (e *engine) handleTimeout(to *TimeOut) {
	for _, req := range e.blockedReqs {
		if req == to.Req {
			response := Response{}
			response.Data = resp.EncodeNullArray()
			req.ResCh <- &response
			close(req.ResCh)

			newBlocked := make([]*Request, 0)
			for _, r := range e.blockedReqs {
				if r != req {
					newBlocked = append(newBlocked, r)
				}
			}
			e.blockedReqs = newBlocked
			break
		}
	}

	for _, req := range e.ackMap {
		if req.Req == to.Req {
			response := Response{}
			response.Data = resp.EncodeResp(req.AckedCount)
			req.Req.ResCh <- &response
			close(req.Req.ResCh)
			delete(e.ackMap, to.Req.ConnId)
			break
		}
	}
}

func (e *engine) handleBlockedCommands(command Command) {
	newBlockedReqs := make([]*Request, 0)
	gotSuccess := false
	for _, req := range e.blockedReqs {
		if req.Command[0] != string(command) {
			newBlockedReqs = append(newBlockedReqs, req)
			continue
		}
		var res *Response
		if !gotSuccess {
			res = e.Handle(req)
			if res != nil {
				gotSuccess = true
			}
		}
		if res == nil {
			newBlockedReqs = append(newBlockedReqs, req)
		}

	}
	e.blockedReqs = newBlockedReqs
}

func (e *engine) IsMaster() bool {
	return e.replicationInfo.MasterAddress == ""
}

func (e *engine) LoadRDBFileIfPresent() error {
	if e.config.DBFilename == "" || e.config.Dir == "" {
		return nil
	}

	dirAbsPath, err := filepath.Abs(e.config.Dir)
	if err != nil {
		return fmt.Errorf("RDB: failed to get absolute path of DB directory: %w", err)
	}

	fullPath := filepath.Join(dirAbsPath, e.config.DBFilename)
	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("RDB: file does not exist")
			return nil
		}
		return fmt.Errorf("failed to open RDB file: %w", err)
	}
	defer file.Close()
	return e.loadRDB(file)

}

func (e *engine) loadRDB(reader io.Reader) error {
	r := bufio.NewReader(reader)

	fmt.Println("RDB: Reading DB")

	var magic [9]byte
	_, err := r.Read(magic[:])
	if err != nil {
		return fmt.Errorf("failed to read RDB magic: %w", err)
	}

	fmt.Println("RDB: found version: ", string(magic[:]))

	for {
		marker, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				fmt.Println("RDB: Finished reading RDB file")
			}
			return fmt.Errorf("RDB: failed to read RDB marker: %w", err)
		}

		if marker != 0xFA {
			r.UnreadByte()
			break
		}
		k, v, err := rdb.DecodeKV(r)

		if err != nil {
			return fmt.Errorf("RDB: failed to decode RDB key-value pair: %w", err)
		}

		fmt.Printf("%s: %s\n", k, v)
	}

	for {
		marker, err := r.ReadByte()
		if err != nil {
			return fmt.Errorf("RDB: failed to read RDB EOF marker: %w", err)
		}
		if marker != 0xFE {
			r.UnreadByte()
			break
		}
		index, _, err := rdb.DecodeLength(r)
		if err != nil {
			return fmt.Errorf("RDB: failed to decode RDB database index: %w", err)
		}
		fmt.Printf("\nRDB: reading DB with index %d\n", index)
		for {
			marker, err := r.ReadByte()
			if err != nil {
				if err == io.EOF {
					fmt.Println("RDB: finished reading RDB file")
					return nil
				}
				return fmt.Errorf("RDB: failed to read RDB database EOF marker: %w", err)
			}
			if marker == 0xFB {
				fmt.Println("Reading hashtable")
				dataLen, _, err := rdb.DecodeLength(r)
				if err != nil {
					return fmt.Errorf("failed to decode RDB hashtable length: %w", err)
				}
				expLen, _, err := rdb.DecodeLength(r)
				if err != nil {
					return fmt.Errorf("failed to decode RDB hashtable expiry length: %w", err)
				}
				fmt.Println("data: ", dataLen, "exp: ", expLen)
				var expTime *time.Time
				for range dataLen + expLen {
					typeByte, err := r.ReadByte()
					if err != nil {
						return fmt.Errorf("failed to read RDB hashtable entry type: %w", err)
					}
					switch typeByte {
					case 0x00:
						key, val, err := rdb.DecodeKV(r)
						if err != nil {
							return fmt.Errorf("failed to decode RDB hashtable entry key-value pair: %w", err)
						}
						fmt.Println("RDB: found KV", key, val)

						e.storage.Set(key, val)
						if expTime != nil {
							e.storage.ExpireTime(key, *expTime)
							expTime = nil
						}
					case 0xFC:
						var expBytes [8]byte
						_, err := r.Read(expBytes[:])
						if err != nil {
							return fmt.Errorf("RDB: failed to read RDB hashtable entry expiry: %w", err)
						}
						t := time.UnixMilli(int64(binary.LittleEndian.Uint64(expBytes[:])))
						expTime = &t
					case 0xFD:
						var expBytes [4]byte
						_, err := r.Read(expBytes[:])
						if err != nil {
							return fmt.Errorf("RDB: failed to read RDB hashtable entry expiry: %w", err)
						}
						t := time.Unix(int64(binary.LittleEndian.Uint32(expBytes[:])), 0)
						expTime = &t
					default:
						return fmt.Errorf("RDB: unsupported RDB hashtable entry type: %d", typeByte)
					}
					// e.storage.ExpireTime(key, expTime)
				}

			}
		}

	}

	return nil
}

func (e *engine) startReplicationIfSlave() error {
	if e.replicationInfo.MasterAddress == "" {
		return nil
	}

	c := client.NewClient(e.replicationInfo.MasterAddress)

	_, err := c.Do([]string{"PING"})
	if err != nil {
		return fmt.Errorf("failed to ping master: %w", err)
	}

	_, err = c.Do([]string{"REPLCONF", "listening-port", "6380"})

	if err != nil {
		return fmt.Errorf("failed to send REPLCONF command to master: %w", err)
	}

	_, err = c.Do([]string{"REPLCONF", "capa", "psync2"})
	if err != nil {
		return fmt.Errorf("failed to send REPLCONF command to master: %w", err)
	}

	err = c.Send([]string{"PSYNC", "?", "-1"})
	if err != nil {
		return fmt.Errorf("failed to send PSYNC command to master: %w", err)
	}
	go func() {
		for {
			input, err := c.Read()
			if err != nil {
				if strings.Contains(err.Error(), "EOF") {
					fmt.Println("REPL: connection closed by master, retrying in 5 seconds...")
					time.AfterFunc(5*time.Second, func() {
						e.startReplicationIfSlave()
					})
				}
				return
			}

			commands, err := resp.ParseCommands(input)
			if err != nil {
				fmt.Println("REPL: failed to parse command from master:", err)
				break
			}
			for _, command := range commands {
				if command.Command[0] == "REDIS0011�" || strings.HasPrefix(command.Command[0], "FULLRESYN") {
					continue
				}
				req := &Request{
					Command: command.Command,
					ResCh:   make(chan *Response),
				}
				if len(req.Command) >= 2 && req.Command[0] == string(CmdReplConf) && req.Command[1] == "GETACK" {
					err := c.Send([]string{string(CmdReplConf), "ACK", fmt.Sprintf("%d", e.replicationInfo.Offset)})

					if err != nil {
						fmt.Println("REPL: failed to send REPLCONF ACK to master:", err)
					}
				} else {
					e.reqCh <- req
					go func() {
						for range req.ResCh {
							// drain response channel
						}
					}()
				}
				e.replicationInfo.Offset += int64(command.Length)
			}

		}
	}()
	return nil
}

func (e *engine) handleReplConf(req *Request) *Response {
	response := Response{}
	defer func() {
		if response.Data != nil {
			req.ResCh <- &response
			close(req.ResCh)
		}
	}()
	if len(req.Command) < 3 {
		response.Data = resp.EncodeInvalidArgCount("REPLCONF")
		return &response
	}

	if strings.ToUpper(req.Command[1]) == "ACK" {
		ackOffset := int64(0)
		_, err := fmt.Sscanf(req.Command[2], "%d", &ackOffset)
		if err != nil {
			response.Data = resp.EncodeErrorMessage("ACK offset must be an integer")
			return &response
		}

		for connId, waitReq := range e.ackMap {
			if ackOffset >= waitReq.OffsetTarget {
				waitReq.AckedCount++
				if waitReq.AckedCount >= waitReq.NumReplicas {
					waitReq.Req.ResCh <- &Response{
						Data: resp.EncodeResp(waitReq.AckedCount),
					}
					close(waitReq.Req.ResCh)
					delete(e.ackMap, connId)
				}
			}
		}

		response.Data = nil
		close(req.ResCh)
		return &response

	}
	response.Data = resp.EncodeSimpleString("OK")
	return &response
}

func (e *engine) handleWait(req *Request) []byte {
	if len(req.Command) < 3 {
		return resp.EncodeInvalidArgCount(string(CmdWait))
	}

	numReplicas := 0
	_, err := fmt.Sscanf(req.Command[1], "%d", &numReplicas)
	if err != nil {
		return resp.EncodeErrorMessage("numreplicas must be an integer")
	}

	if numReplicas == 0 {
		return resp.EncodeResp(0)
	}

	timeoutMs := 0
	_, err = fmt.Sscanf(req.Command[2], "%d", &timeoutMs)
	if err != nil {
		return resp.EncodeErrorMessage("timeout must be an integer")
	}

	if e.replicationInfo.Offset == 0 {
		return resp.EncodeResp(len(e.slaveReqs))
	}

	for _, slaveReq := range e.slaveReqs {
		slaveReq.ResCh <- &Response{Data: resp.EncodeResp([]string{"REPLCONF", "GETACK", "*"})}
	}

	if timeoutMs != 0 {
		e.timeoutReq(req, time.Duration(timeoutMs)*time.Millisecond)
	}
	e.ackMap[req.ConnId] = &WaitReq{
		Req:          req,
		NumReplicas:  numReplicas,
		OffsetTarget: e.replicationInfo.Offset,
		AckedCount:   0,
	}

	return nil
}

func (e *engine) handleInfo(command []string) []byte {
	role := "master"
	if e.replicationInfo.MasterAddress != "" {
		role = "slave"
	}
	info := fmt.Sprintf("role:%s\r\n", role)
	info += fmt.Sprintf("master_replid:%s\r\n", e.replicationInfo.ReplicationId)
	info += fmt.Sprintf("master_repl_offset:%d\r\n", e.replicationInfo.Offset)

	return resp.EncodeResp(info)
}

func (e *engine) handleSetCommand(command []string) []byte {
	if len(command) < 3 {
		return resp.EncodeInvalidArgCount("SET")
	}

	e.storage.Set(command[1], command[2])
	if len(command) > 3 && (command[3] == "EX" || command[3] == "PX") {
		if len(command) < 5 {
			return resp.EncodeErrorMessage("wrong number of arguments for 'SET' command with expiry")
		}
		var duration int
		fmt.Sscanf(command[4], "%d", &duration)
		if command[3] == "EX" {
			e.storage.Expire(command[1], time.Duration(duration)*time.Second)
		} else {
			e.storage.Expire(command[1], time.Duration(duration)*time.Millisecond)
		}
	}
	return resp.EncodeSimpleString("OK")

}

func (e *engine) handleRPushCommand(command []string) []byte {
	if len(command) < 3 {
		return resp.EncodeInvalidArgCount(command[0])
	}

	list, err := e.storage.GetOrMakeList(command[1])

	if err != nil {
		return resp.EncodeErrorMessage(err.Error())
	}
	for i := 2; i < len(command); i++ {
		list = append(list, command[i])
	}
	e.storage.Set(command[1], list)
	return resp.EncodeResp(len(list))
}

func (e *engine) handleLPushCommand(command []string) []byte {
	if len(command) < 3 {
		return resp.EncodeInvalidArgCount(command[0])
	}
	list, err := e.storage.GetOrMakeList(command[1])
	if err != nil {
		return resp.EncodeErrorMessage(err.Error())
	}
	for i := 2; i < len(command); i++ {
		list = append([]any{command[i]}, list...)
	}
	e.storage.Set(command[1], list)
	return resp.EncodeResp(len(list))
}

func (e *engine) handleLRangeCommand(command []string) []byte {
	if len(command) < 4 {
		return resp.EncodeInvalidArgCount("LRANGE")
	}
	existingValue, ok := e.storage.Get(command[1])
	if !ok {
		return resp.EncodeResp([]any{})
	}
	list, ok := existingValue.([]any)
	if !ok {
		return resp.EncodeErrorMessage("value is not a list")
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
		return resp.EncodeResp([]any{})
	}

	return resp.EncodeResp(list[start : end+1])

}

func (e *engine) handleLLen(command []string) []byte {
	if len(command) < 2 {
		return resp.EncodeInvalidArgCount(command[0])
	}
	list, err := e.storage.GetOrMakeList(command[1])

	if err != nil {
		return resp.EncodeError(err)
	}
	return resp.EncodeResp(len(list))

}

func (e *engine) handleLPopCommand(command []string) []byte {
	if len(command) < 2 {
		return resp.EncodeInvalidArgCount(command[0])
	}
	existingValue, ok := e.storage.Get(command[1])
	if !ok {
		return resp.EncodeNull()
	}
	list, ok := existingValue.([]any)
	if !ok {
		return resp.EncodeError(storage.ErrWrongType)
	}
	if len(list) == 0 {
		return resp.EncodeNull()
	}

	popCount := 1
	if len(command) >= 3 {
		_, err := fmt.Sscanf(command[2], "%d", &popCount)
		if err != nil {
			return resp.EncodeErrorMessage("count must be an integer")
		}
		if popCount < 1 {
			return resp.EncodeErrorMessage("count must be positive")
		}
		if popCount > len(list) {
			popCount = len(list)
		}
	}

	poppedValues := list[:popCount]
	list = list[popCount:]
	e.storage.Set(command[1], list)

	if popCount == 1 {
		return resp.EncodeResp(poppedValues[0])
	}
	return resp.EncodeResp(poppedValues)
}

func (e *engine) handleBLPop(req *Request) []byte {
	command := req.Command
	if len(command) < 3 {
		return resp.EncodeInvalidArgCount(command[0])
	}

	var timeoutSec float32
	_, err := fmt.Sscanf(command[2], "%f", &timeoutSec)
	if err != nil {
		return resp.EncodeErrorMessage("timeout must be an integer")
	}
	if timeoutSec < 0 {
		return resp.EncodeErrorMessage("timeout must be non-negative")
	}

	timeoutDuration := time.Duration(timeoutSec*1000) * time.Millisecond

	e.timeoutReq(req, timeoutDuration)

	timeoutTime := req.Timestamp.Add(timeoutDuration)

	if timeoutSec != 0 && timeoutTime.Before(time.Now()) {
		return resp.EncodeNullArray()
	}

	list, err := e.storage.GetOrMakeList(command[1])

	if err != nil {
		return resp.EncodeError(err)
	}

	if len(list) > 0 {
		poppedValue := list[0]
		list = list[1:]
		e.storage.Set(command[1], list)
		return resp.EncodeResp([]any{command[1], poppedValue})
	}

	e.blockedReqs = append(e.blockedReqs, req)

	return nil
}

func (e *engine) handleType(command []string) []byte {
	if len(command) < 2 {
		return resp.EncodeInvalidArgCount(command[0])
	}

	value, exists := e.storage.Get(command[1])
	if !exists {
		return resp.EncodeSimpleString("none")
	}

	switch value.(type) {
	case string:
		return resp.EncodeSimpleString("string")
	case []any:
		return resp.EncodeSimpleString("list")
	case *storage.Stream:
		return resp.EncodeSimpleString("stream")
	default:
		return resp.EncodeSimpleString("unknown")
	}
}

func (e *engine) handleXAdd(command []string) []byte {
	if len(command) < 2 {
		return resp.EncodeInvalidArgCount(command[0])
	}

	stream, err := e.storage.GetOrMakeStream(command[1])
	if err != nil {
		return resp.EncodeError(err)
	}

	entry := storage.StreamEntry{
		ID:     storage.EntryID{},
		Fields: make(map[string]string),
	}

	validatedID, err := stream.GenerateOrValidateEntryID(command[2])
	if err != nil {
		return resp.EncodeErrorMessage(err.Error())
	}
	entry.ID = *validatedID

	for i := 3; i < len(command); i += 2 {
		if i+1 >= len(command) {
			return resp.EncodeErrorMessage("XADD requires field-value pairs")
		}
		entry.Fields[command[i]] = command[i+1]
	}

	stream.Entries = append(stream.Entries, entry)
	e.storage.Set(command[1], stream)

	return resp.EncodeResp(entry.ID)
}

func (e *engine) handleXRange(command []string) []byte {
	if len(command) < 4 {
		return resp.EncodeInvalidArgCount(command[0])
	}

	stream, err := e.storage.GetOrMakeStream(command[1])
	if err != nil {
		return resp.EncodeError(err)
	}

	startID, err := storage.ParseRangeID(command[2], true)
	if err != nil {
		return resp.EncodeError(err)
	}
	endID, err := storage.ParseRangeID(command[3], false)
	if err != nil {
		return resp.EncodeError(err)
	}

	entries := stream.GetRange(startID, &endID)

	return resp.EncodeResp(entries)
}

func (e *engine) handleXRead(req *Request) []byte {
	command := req.Command

	keys := make([]string, 0)
	ids := make([]storage.EntryID, 0)

	i := 1

	isBlocking := false
	if strings.ToUpper(command[i]) == "BLOCK" {
		if len(command) < 6 {
			return resp.EncodeInvalidArgCount(command[0])
		}
		isBlocking = true
		var timeoutMs int
		_, err := fmt.Sscanf(command[i+1], "%d", &timeoutMs)
		if err != nil {
			return resp.EncodeErrorMessage("BLOCK time must be an integer")
		}
		if timeoutMs < 0 {
			return resp.EncodeErrorMessage("BLOCK time must be non-negative")
		}
		timeoutDuration := time.Duration(timeoutMs) * time.Millisecond
		timeoutTime := req.Timestamp.Add(timeoutDuration)

		e.timeoutReq(req, timeoutDuration)

		if timeoutMs != 0 && timeoutTime.Before(time.Now()) {
			return resp.EncodeNullArray()
		}

		i += 2
	}

	if strings.ToUpper(command[i]) != "STREAMS" {
		return resp.EncodeInvalidArgCount(command[0])
	}

	i++

	keyCount := (len(command) - i) / 2

	for ; i < len(command)-keyCount; i++ {
		keys = append(keys, command[i])
	}
	for ; i < len(command); i++ {
		idStr := command[i]
		entryID := storage.EntryID{}
		if idStr == "$" {
			id, err := e.storage.GetStreamTopID(command[i-keyCount])
			if err != nil {
				return resp.EncodeError(err)
			}
			command[i] = fmt.Sprintf("%d-%d", id.T, id.S)
			ids = append(ids, *id)
			continue
		}
		_, err := fmt.Sscanf(idStr, "%d-%d", &entryID.T, &entryID.S)
		if err != nil {
			return resp.EncodeError(err)
		}
		ids = append(ids, entryID)
	}

	result := make([]any, 0)

	hasData := false
	for idx, streamKey := range keys {
		stream, err := e.storage.GetStream(streamKey)
		if err != nil {
			if err == storage.ErrKeyNotFound {
				continue
			}
			return resp.EncodeError(err)
		}
		entries := stream.GetAfterID(ids[idx])
		if len(entries) > 0 {
			hasData = true
		}
		result = append(result, []any{streamKey, entries})
	}

	if hasData {
		return resp.EncodeResp(result)
	}
	if isBlocking {
		e.blockedReqs = append(e.blockedReqs, req)
		return nil
	}
	return resp.EncodeNullArray()

}

func (e *engine) handleIncr(command []string) []byte {
	if len(command) < 2 {
		return resp.EncodeInvalidArgCount(command[0])
	}

	value, exists := e.storage.Get(command[1])
	if !exists {
		e.storage.Set(command[1], "1")
		return resp.EncodeResp(1)
	}

	strValue, ok := value.(string)
	if !ok {
		return resp.EncodeErrorMessage("value is not an integer or out of range")
	}

	var intValue int
	_, err := fmt.Sscanf(strValue, "%d", &intValue)
	if err != nil {
		return resp.EncodeErrorMessage("value is not an integer or out of range")
	}

	intValue += 1
	e.storage.Set(command[1], fmt.Sprintf("%d", intValue))
	return resp.EncodeResp(intValue)
}

func (e *engine) handleMulti(req *Request) []byte {
	_, exists := e.commandQueues[req.ConnId]
	if !exists {
		e.commandQueues[req.ConnId] = make([]*Request, 0)
		return resp.EncodeSimpleString("OK")
	}
	return resp.EncodeErrorMessage("MULTI calls cannot be nested")

}

func (e *engine) queueIfMulti(req *Request) bool {
	if req.Command[0] == "EXEC" || req.Command[0] == "MULTI" || req.Command[0] == "DISCARD" || e.isExecutingMulti {
		return false
	}
	queue, exists := e.commandQueues[req.ConnId]
	if exists {
		e.commandQueues[req.ConnId] = append(queue, req)
	}
	return exists
}

func (e *engine) handleExec(req *Request) []byte {
	_, exists := e.commandQueues[req.ConnId]
	if !exists {
		return resp.EncodeErrorMessage("EXEC without MULTI")
	}

	responses := make([][]byte, 0)

	e.isExecutingMulti = true
	defer func() { e.isExecutingMulti = false }()

	for _, queuedReq := range e.commandQueues[req.ConnId] {
		resp := e.Handle(queuedReq)
		responses = append(responses, resp.Data)
	}

	delete(e.commandQueues, req.ConnId)

	return resp.EncodeArray(responses)
}

func (e *engine) handleDiscard(connId string) []byte {
	_, exists := e.commandQueues[connId]
	if !exists {
		return resp.EncodeErrorMessage("DISCARD without MULTI")
	}

	delete(e.commandQueues, connId)
	return resp.EncodeSimpleString("OK")
}

func (e *engine) handleConfig(command []string) []byte {
	if len(command) < 3 {
		return resp.EncodeInvalidArgCount("CONFIG")
	}

	subcommand := strings.ToUpper(command[1])
	switch subcommand {
	case "GET":
		pattern := command[2]
		result := make([]any, 0)
		if pattern == "*" || pattern == "dir" {
			result = append(result, "dir", e.config.Dir)
		}
		if pattern == "*" || pattern == "dbfilename" {
			result = append(result, "dbfilename", e.config.DBFilename)
		}
		return resp.EncodeResp(result)
	default:
		return resp.EncodeErrorMessage("unknown CONFIG subcommand: " + subcommand)
	}
}

func (e *engine) handleKeys(command []string) []byte {
	if len(command) < 2 {
		return resp.EncodeInvalidArgCount("KEYS")
	}
	pattern := command[1]
	keys, err := e.storage.GetMatchingKeys(pattern)
	if err != nil {
		return resp.EncodeError(err)
	}
	return resp.EncodeResp(keys)
}

func (e *engine) isSubscribed(req *Request) bool {
	subCount, exists := e.subCount[req.ConnId]
	return exists && subCount > 0
}

func (e *engine) verifySubscribed(req *Request) bool {
	if !e.isSubscribed(req) {
		return true
	}

	command := Command(req.Command[0])
	allowedCommands := map[Command]bool{
		CmdSubscribe:   true,
		CmdUnsubscribe: true,
		CmdPing:        true,
		CmdQuit:        true,
	}

	if _, ok := allowedCommands[command]; ok {
		return true
	}
	return false
}

func (e *engine) handleSubscribe(req *Request) []byte {
	command := req.Command
	if len(command) < 2 {
		return resp.EncodeInvalidArgCount(command[0])
	}

	channels := command[1:]
	subCount, exists := e.subCount[req.ConnId]
	if !exists {
		subCount = 0
	}
	for _, channel := range channels {
		subscribers, exists := e.channels[channel]
		if !exists {
			subscribers = make([]*Request, 0)
		}

		isSubscribed := false
		for _, subscriber := range subscribers {
			if subscriber == req {
				isSubscribed = true
			}
		}

		if !isSubscribed {
			subscribers = append(subscribers, req)
			subCount++
		}
		e.channels[channel] = subscribers

		response := Response{}
		response.Data = resp.EncodeResp([]any{"subscribe", channel, subCount})
		req.ResCh <- &response
	}
	e.subCount[req.ConnId] = subCount
	return nil
}

func (e *engine) handlePub(command []string) []byte {
	if len(command) < 3 {
		return resp.EncodeInvalidArgCount(command[0])
	}
	
	channel := command[1]
	message := command[2]
	subscribers, exists := e.channels[channel]
	if !exists {
		return resp.EncodeResp(0)
	}
	
	for _, subscriber := range subscribers {
		response := Response{}
		response.Data = resp.EncodeResp([]any{"message", channel, message})
		subscriber.ResCh <- &response
	}
	return resp.EncodeResp(len(subscribers))
}

func (e *engine) handleUnsubscribe(req *Request) []byte {
	command := req.Command
	if len(command) < 2 {
		return resp.EncodeInvalidArgCount(command[0])
	}
	
	channels := command[1:]
	subCount, exists := e.subCount[req.ConnId]
	if !exists {
		subCount = 0
	}
	for _, channel := range channels {
		subscribers, exists := e.channels[channel]
		if !exists {
			subscribers = make([]*Request, 0)
		}
		
		newSubscribers := make([]*Request, 0)
		for _, subscriber := range subscribers {
			if subscriber.ConnId == req.ConnId {
				subCount--
				close(subscriber.ResCh)
			} else {
				newSubscribers = append(newSubscribers, subscriber)
			}
		}
		e.channels[channel] = newSubscribers
		
		response := Response{}
		response.Data = resp.EncodeResp([]any{"unsubscribe", channel, subCount})
		req.ResCh <- &response
	}
	e.subCount[req.ConnId] = subCount
	close(req.ResCh)
	return nil
}
