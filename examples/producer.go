package main

import (
	"fmt"
	"os"

	"github.com/memphisdev/memphis.go"
)

func main() {
	c, err := memphis.Connect("localhost", "root", "memphis")
	if err != nil {
		os.Exit(1)
	}
	defer c.Close()

	p, err := c.CreateProducer("station_1", "producer_name_a")
	err = p.Produce([]byte("You have a message!"))

	if err != nil {
		fmt.Errorf("Produce failed: %v", err)
		os.Exit(1)
	}
}
