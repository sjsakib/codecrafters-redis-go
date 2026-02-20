package engine

import (
	"crypto/sha256"
	"fmt"
	"slices"
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

func (e *engine) handleAuth(request *Request) []byte {
	command := request.Command
	if len(command) != 3 {
		return resp.EncodeErrorMessage("wrong number of arguments for 'AUTH' command")
	}
	username := command[1]
	password := hashPassword(command[2])
	user, exists := e.users[username]
	if !exists {
		return resp.EncodeError(resp.ErrWrongPass)
	}
	if slices.Contains(user.Passwords, password) {
		e.connUserMap[request.ConnId] = user
		return resp.EncodeOK()
	}
	return resp.EncodeError(resp.ErrWrongPass)
}

func (e *engine) checkAuth(request *Request) bool {
	command := request.Command
	if len(command) > 0 && command[0] == string(CmdAuth) {
		return true
	}
	user := e.connUserMap[request.ConnId]
	if user != nil {
		return true
	}
	user = e.users["default"]
	if user.Flags["nopass"] {
		e.connUserMap[request.ConnId] = user
		return true
	}
	return false
}

func hashPassword(password string) string {
	sha256Hash := sha256.Sum256([]byte(password))
	return fmt.Sprintf("%x", sha256Hash)
}
