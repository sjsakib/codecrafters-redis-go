package resp

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/storage"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

const intFmtStr = "%d\r\n"

type CmdWithLen struct {
	Command []string
	Length  int
}

func ParseCommands(input []byte) ([]CmdWithLen, error) {
	commands := make([]CmdWithLen, 0)
	reader := bytes.NewReader(input)
	lastOffset := int64(0)
	for {
		command, err := Parse(reader)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}
		offset := int64(reader.Size()) - int64(reader.Len())
		command[0] = strings.ToUpper(command[0])
		commands = append(commands, CmdWithLen{Command: command, Length: int(offset - lastOffset)})
		lastOffset = offset
	}
	return commands, nil
}

func ParseCommand(input []byte) ([]string, error) {
	command, err := Parse(bytes.NewReader(input))
	if err != nil {
		return nil, err
	}
	command[0] = strings.ToUpper(command[0])
	return command, nil
}

func Parse(reader *bytes.Reader) ([]string, error) {

	switch b, err := reader.ReadByte(); b {
	case '*':
		var numArgs int
		fmt.Fscanf(reader, intFmtStr, &numArgs)

		commands := make([]string, numArgs)
		for i := 0; i < numArgs; i++ {
			var length int
			fmt.Fscanf(reader, intFmtStr, &length)
			command, err := Parse(reader)

			if err != nil {
				return nil, err
			}
			if len(command) == 0 {
				return nil, fmt.Errorf("invalid command format")
			}
			commands[i] = command[0]
		}

		return commands, nil
	case '$':
		var length int
		fmt.Fscanf(reader, intFmtStr, &length)
		var command string
		fmt.Fscanf(reader, "%s\r\n", &command)
		if length != len(command) {
			// forward the reader to skip the remaining bytes
			remaining := length - len(command)
			if remaining > 0 {
				reader.Seek(int64(remaining), io.SeekCurrent)
			}
		}
		return []string{command}, nil
	case '+':
		line, err := utils.ReadLine(reader)
		if err != nil {
			return nil, err
		}
		return []string{line}, nil
	case '-':
		var command string
		fmt.Fscanf(reader, "%s\r\n", &command)
		return []string{command}, nil
	default:
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("unexpected byte: %c", b)
	}

}

func EncodeResp(val any) []byte {
	switch v := val.(type) {
	case string:
		return EncodeBulkString(v)
	case int:
		return fmt.Appendf(nil, ":%d\r\n", v)
	case int64:
		return fmt.Appendf(nil, ":%d\r\n", v)
	case float64:
		return EncodeBulkString(fmt.Sprintf("%g", val))
	case []byte:
		return v
	case storage.StreamEntry:
		var buffer bytes.Buffer
		fmt.Fprintf(&buffer, "*2\r\n")
		buffer.Write(EncodeResp(v.ID))
		fmt.Fprintf(&buffer, "*%d\r\n", len(v.Fields)*2)
		for field, value := range v.Fields {
			buffer.Write(EncodeResp(field))
			buffer.Write(EncodeResp(value))
		}
		return buffer.Bytes()
	case storage.EntryID:
		return EncodeResp(fmt.Sprintf("%d-%d", v.T, v.S))
	case nil:
		return EncodeResp(nil)
	default:
		rv := reflect.ValueOf(val)
		if rv.Kind() == reflect.Slice {
			return EncodeArrayReflect(rv)
		}
		return EncodeErrorMessage(fmt.Sprintf("failed to encode response: unknown type %T", val))
	}
}

func EncodeArray[T any](items []T) []byte {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "*%d\r\n", len(items))
	for _, item := range items {
		buffer.Write(EncodeResp(item))
	}
	return buffer.Bytes()
}

func EncodeArrayReflect(rv reflect.Value) []byte {
	var buffer bytes.Buffer
	length := rv.Len()
	fmt.Fprintf(&buffer, "*%d\r\n", length)
	for i := range length {
		item := rv.Index(i).Interface()
		buffer.Write(EncodeResp(item))
	}
	return buffer.Bytes()
}

func EncodeBulkString(s string) []byte {
	return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(s), s)
}

func EncodeError(err error) []byte {
	switch err {
	case storage.ErrWrongType:
		return []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	case storage.ErrKeyNotFound:
		return []byte("$-1\r\n")
	default:
		return fmt.Appendf(nil, "-ERR %s\r\n", err.Error())
	}
}

func EncodeOK() []byte {
	return []byte("+OK\r\n")
}

func EncodeNull() []byte {
	return []byte("$-1\r\n")
}
func EncodeNullArray() []byte {
	return []byte("*-1\r\n")
}

func EncodeErrorMessage(message string) []byte {
	return fmt.Appendf(nil, "-ERR %s\r\n", message)
}

func EncodeSimpleString(s string) []byte {
	return fmt.Appendf(nil, "+%s\r\n", s)
}

func EncodeInvalidArgCount(command string) []byte {
	return fmt.Appendf(nil, "-ERR wrong number of arguments for '%s' command\r\n", command)
}
