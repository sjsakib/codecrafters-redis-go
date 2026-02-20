package engine

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

func (e *engine) handleAcl(command []string) []byte {
	if len(command) < 2 {
		return resp.EncodeErrorMessage("wrong number of arguments for 'ACL' command")
	}
	subcommand := command[1]
	switch subcommand {
	case "WHOAMI":
		return resp.EncodeResp("default")
	default:
		return resp.EncodeErrorMessage(fmt.Sprintf("unknown ACL subcommand '%s'", subcommand))
	}
}
