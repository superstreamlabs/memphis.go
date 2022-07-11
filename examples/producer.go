package main

import (
	"fmt"
	"os"

	"github.com/memphisdev/memphis.go"
)

func main() {
	c, err := memphis.Connect("<memphis-host>", "<application type username>", "<broker-token>")
	if err != nil {
		os.Exit(1)
	}
	defer c.Close()

	p, err := c.CreateProducer("<station-name>", "<producer-name>")
	err = p.Produce([]byte("You have a message!"))

	if err != nil {
		fmt.Errorf("Produce failed: %v", err)
		os.Exit(1)
	}
}
