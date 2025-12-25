package redis

import (
	"bytes"
	"fmt"
)

const intFmtStr = "%d\r\n"

func parseCommand(reader *bytes.Reader) ([]string, error) {

	switch b, err := reader.ReadByte(); b {
	case '*':
		var numArgs int
		fmt.Fscanf(reader, intFmtStr, &numArgs)

		commands := make([]string, numArgs)
		for i := 0; i < numArgs; i++ {
			var length int
			fmt.Fscanf(reader, intFmtStr, &length)
			command, err := parseCommand(reader)

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

func encodeResp(val any) string {
	switch v := val.(type) {
	case string:
		return encodeBulkString(v)
	case int:
		return fmt.Sprintf(":%d\r\n", v)
	case []any:
		var buffer bytes.Buffer
		fmt.Fprintf(&buffer, "*%d\r\n", len(v))
		for _, item := range v {
			buffer.WriteString(encodeResp(item))
		}
		return buffer.String()
	default:
		return "-ERR unknown type\r\n"
	}
}

func encodeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}
