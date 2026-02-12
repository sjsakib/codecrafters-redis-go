package engine

func isWriteCommand(command string) bool {
	writeCommands := []Command{
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
