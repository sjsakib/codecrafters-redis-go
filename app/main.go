package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	// defer conn.Close()

	_, err = conn.Write([]byte("+PONG\r\n"))
	if err != nil {
		fmt.Println("Error writing to connection: ", err.Error())
		os.Exit(1)
	}

	// buf := make([]byte, 1024)
	// n, err := conn.Read(buf)

	// if err != nil {
	// 	fmt.Println("Error reading from connection: ", err.Error())
	// 	os.Exit(1)
	// }

	// fmt.Println("Received data: ", string(buf[:n]) )

	// if string(buf[:n]) == "PING\r\n" {
	// 	_, err := conn.Write([]byte("+PONG\r\n"))

	// 	if err != nil {
	// 		fmt.Println("Error writing to connection: ", err.Error())
	// 		os.Exit(1)
	// 	}

	// } else {
	// 	conn.Write([]byte("-ERR unknown command\r\n"))
	// }

}
