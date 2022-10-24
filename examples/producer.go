package main

import (
	"fmt"
	"os"

	"github.com/memphisdev/memphis.go"
)

func main() {
	conn, err := memphis.Connect("<memphis-host>", "<application type username>", "<broker-token>")
	if err != nil {
		os.Exit(1)
	}
	defer conn.Close()
	p, err := conn.CreateProducer("<station-name>", "<producer-name>")

	hdrs := memphis.Headers{}
	err = hdrs.Add("<key>", "<value>")

	if err != nil {
		fmt.Errorf("Header failed: %v", err)
		os.Exit(1)
	}

	err = p.Produce([]byte("You have a message!"), memphis.MsgHeaders(hdrs))

	if err != nil {
		fmt.Errorf("Produce failed: %v", err)
		os.Exit(1)
	}
}
