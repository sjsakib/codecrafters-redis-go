package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/engine"
	"github.com/codecrafters-io/redis-starter-go/app/server"
	"github.com/codecrafters-io/redis-starter-go/app/storage"
)

func main() {
	port := flag.Int("port", 6379, "Port to run the Redis server on")

	replicaof := flag.String("replicaof", "", "Address of master")

	dbfilename := flag.String("dbfilename", "dump.rdb", "Filename for RDB persistence")
	dir := flag.String("dir", "./data", "Directory for RDB persistence")

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

	s := storage.NewInMemoryStorage()
	e := engine.NewEngine(s, masterAddress)
	e.SetConfig(engine.Config{
		DBFilename: *dbfilename,
		Dir:        *dir,
	})

	server := server.NewServer(e)

	if err := server.Start(fmt.Sprintf(":%d", *port)); err != nil {
		fmt.Println("Failed to start server:", err)
		os.Exit(1)
	}

}
