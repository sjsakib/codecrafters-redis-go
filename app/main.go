package main

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/redis"
)

func main() {
	storage := redis.NewInMemoryStorage()
	engine := redis.NewEngine(storage)
	server := redis.NewServer(engine)

	if err := server.Start(":6379"); err != nil {
		fmt.Println("Failed to start server:", err)
		os.Exit(1)
	}

}
