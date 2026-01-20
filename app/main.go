package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/redis"
)

func main() {
	storage := redis.NewInMemoryStorage()
	engine := redis.NewEngine(storage)
	server := redis.NewServer(engine)

	port := flag.Int("port", 6379, "Port to run the Redis server on")
	flag.Parse()


	if err := server.Start(fmt.Sprintf(":%d", *port)); err != nil {
		fmt.Println("Failed to start server:", err)
		os.Exit(1)
	}

}
