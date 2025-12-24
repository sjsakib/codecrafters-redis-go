package main

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/redis"
)

func main() { 
	server := redis.NewServer()

	if err := server.Start(); err != nil {
		fmt.Println("Failed to start server:", err)
		os.Exit(1)
	}
}
