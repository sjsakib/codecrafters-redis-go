package engine

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type User struct {
	Username  string
	Passwords []string
	Flags     map[string]bool
}

func (e *engine) handleAcl(command []string) []byte {
	if len(command) < 2 {
		return resp.EncodeErrorMessage("wrong number of arguments for 'ACL' command")
	}
	subcommand := command[1]
	switch subcommand {
	case "WHOAMI":
		return resp.EncodeResp("default")
	case "GETUSER":
		if len(command) != 3 {
			return resp.EncodeErrorMessage("wrong number of arguments for 'ACL GETUSER' command")
		}
		username := command[2]
		user, exists := e.users[username]
		if !exists {
			return resp.EncodeNull()
		}

		flags := make([]any, 0)
		for flag := range user.Flags {
			flags = append(flags, flag)
		}

		return resp.EncodeResp([]any{
			"flags", flags,
			"passwords", user.Passwords,
		})
	case "SETUSER":
		return e.handleSetUser(command)
	default:
		return resp.EncodeErrorMessage(fmt.Sprintf("unknown ACL subcommand '%s'", subcommand))
	}
}

func (e *engine) handleSetUser(command []string) []byte {
	if len(command) < 3 {
		return resp.EncodeErrorMessage("wrong number of arguments for 'ACL SETUSER' command")
	}
	username := command[2]
	user, exists := e.users[username]
	if !exists {
		user = &User{
			Username: username,
			Flags:    make(map[string]bool),
		}
		e.users[username] = user
	}

	for i := 3; i < len(command); i++ {
		arg := command[i]
		if strings.HasPrefix(arg, ">") {
			password := hashPassword(arg[1:])
			user.Passwords = append(user.Passwords, password)
			delete(user.Flags, "nopass")
		}
	}
	return resp.EncodeOK()
}

func hashPassword(password string) string {
	sha256Hash := sha256.Sum256([]byte(password))
	return fmt.Sprintf("%x", sha256Hash)
}
