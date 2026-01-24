package redis

import (
	"fmt"
	"crypto/rand"
)

func randomID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func isWriteCommand(command string) bool {
	writeCommands := []Command {
		CmdSet,
		CmdDel,
		CmdIncr,
		CmdLPush,
		CmdRPush,
		CmdXAdd,
	}

	for _, cmd := range writeCommands {
		if command == string(cmd) {
			return true
		}
	}
	return false
}