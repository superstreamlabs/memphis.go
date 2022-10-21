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
	err = p.Produce([]byte("You have a message!"), map[string][]string{"<key>": {"<value>"}, "<key1>": {"<value1>"}})

	if err != nil {
		fmt.Errorf("Produce failed: %v", err)
		os.Exit(1)
	}
}
