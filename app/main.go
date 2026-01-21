package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/redis"
)

func main() {
	port := flag.Int("port", 6379, "Port to run the Redis server on")

	replicaof := flag.String("replicaof", "", "Address of master")

	flag.Parse()

	masterAddress := ""
	if *replicaof != "" {
		parts := strings.Split(*replicaof, " ")
		if len(parts) != 2 {
			fmt.Println("Invalid replicaof format. Use: <host> <port>")
			os.Exit(1)
		}
		masterAddress = fmt.Sprintf("%s:%s", parts[0], parts[1])
	}

	storage := redis.NewInMemoryStorage()
	engine := redis.NewEngine(storage, masterAddress)
	server := redis.NewServer(engine)

	err := engine.PingMasterIfSlave()
	if err != nil {
		fmt.Println("Failed to ping master:", err)
		os.Exit(1)
	}

	if err := server.Start(fmt.Sprintf(":%d", *port)); err != nil {
		fmt.Println("Failed to start server:", err)
		os.Exit(1)
	}

}
