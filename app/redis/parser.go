package redis

import (
	"bytes"
	"fmt"
	"strings"
)

const intFmtStr = "%d\r\n"

func parseCommand(input []byte) ([]string, error) {
	command, err := parse(bytes.NewReader(input))
	if err != nil {
		return nil, err
	}
	command[0] = strings.ToUpper(command[0])
	return command, nil
}

func parse(reader *bytes.Reader) ([]string, error) {

	switch b, err := reader.ReadByte(); b {
	case '*':
		var numArgs int
		fmt.Fscanf(reader, intFmtStr, &numArgs)

		commands := make([]string, numArgs)
		for i := 0; i < numArgs; i++ {
			var length int
			fmt.Fscanf(reader, intFmtStr, &length)
			command, err := parse(reader)

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
		return []string{command}, nil
	default:
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("unexpected byte: %c", b)
	}

}

func encodeResp(val any) []byte {
	switch v := val.(type) {
	case string:
		return encodeBulkString(v)
	case int:
		return fmt.Appendf(nil, ":%d\r\n", v)
	case []any:
		var buffer bytes.Buffer
		fmt.Fprintf(&buffer, "*%d\r\n", len(v))
		for _, item := range v {
			buffer.Write(encodeResp(item))
		}
		return buffer.Bytes()
	default:
		return encodeErrorMessage("failed to encode response: unknown type")
	}
}

func encodeBulkString(s string) []byte {
	return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(s), s)
}

func encodeError(err error) []byte {
	switch err {
	case ErrWrongType:
		return []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	case ErrKeyNotFound:
		return []byte("$-1\r\n")
	default:
		return fmt.Appendf(nil, "-ERR %s\r\n", err.Error())
	}
}

func encodeNull() []byte {
	return []byte("$-1\r\n")
}
func encodeNullArray() []byte {
	return []byte("*-1\r\n")
}

func encodeErrorMessage(message string) []byte {
	return fmt.Appendf(nil, "-ERR %s\r\n", message)
}

func encodeSimpleString(s string) []byte {
	return fmt.Appendf(nil, "+%s\r\n", s)
}

func encodeInvalidArgCount(command string) []byte {
	return fmt.Appendf(nil, "-ERR wrong number of arguments for '%s' command\r\n", command)
}
