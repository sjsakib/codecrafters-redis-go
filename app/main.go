package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/redis"
)

func main() {
	port := flag.Int("port", 6379, "Port to run the Redis server on")

	replicaof := flag.String("replicaof", "", "Address of master")

	flag.Parse()

	storage := redis.NewInMemoryStorage()
	engine := redis.NewEngine(storage, *replicaof)
	server := redis.NewServer(engine)

	if err := server.Start(fmt.Sprintf(":%d", *port)); err != nil {
		fmt.Println("Failed to start server:", err)
		os.Exit(1)
	}

}
