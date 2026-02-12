package engine

type Command string

const (
	CmdPing      Command = "PING"
	CmdSet       Command = "SET"
	CmdGet       Command = "GET"
	CmdRPush     Command = "RPUSH"
	CmdLPush     Command = "LPUSH"
	CmdLRange    Command = "LRANGE"
	CmdLLen      Command = "LLEN"
	CmdLPop      Command = "LPOP"
	CmdBLPop     Command = "BLPOP"
	CmdType      Command = "TYPE"
	CmdXAdd      Command = "XADD"
	CmdXRange    Command = "XRANGE"
	CmdXRead     Command = "XREAD"
	CmdIncr      Command = "INCR"
	CmdMulti     Command = "MULTI"
	CmdExec      Command = "EXEC"
	CmdDiscard   Command = "DISCARD"
	CmdInfo      Command = "INFO"
	CmdDel       Command = "DEL"
	CmdPsync     Command = "PSYNC"
	CmdReplConf  Command = "REPLCONF"
	CmdEcho      Command = "ECHO"
	CmdWait      Command = "WAIT"
	CmdConfig    Command = "CONFIG"
	CmdKeys      Command = "KEYS"
	CmdSubscribe Command = "SUBSCRIBE"
)
