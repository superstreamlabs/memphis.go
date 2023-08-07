package main

import (
	"fmt"
	"os"

	"github.com/memphisdev/memphis.go"
)

func main() {
	conn, err := memphis.Connect("localhost", "root", memphis.ConnectionToken("<broker-token>"))
	if err != nil {
		fmt.Printf("Connection failed: %v", err)
		os.Exit(1)
	}
	defer conn.Close()
	p, err := conn.CreateProducer("<station-name>", "<producer-name>")

	if err != nil {
		fmt.Printf("Create Producer failed: %v", err)
		os.Exit(1)
	}

	hdrs := memphis.Headers{}
	hdrs.New()
	err = hdrs.Add("key", "value")

	if err != nil {
		fmt.Printf("Header failed: %v", err)
		os.Exit(1)
	}

	err = p.Produce([]byte("You have a message!"), memphis.MsgHeaders(hdrs))

	if err != nil {
		fmt.Printf("Produce failed: %v", err)
		os.Exit(1)
	}
}
