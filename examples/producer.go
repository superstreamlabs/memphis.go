package main

import (
	"fmt"
	"os"

	"github.com/memphisdev/memphis.go/memphis"
)

func main() {
	c, err := memphis.Connect("localhost", memphis.Username("root"), memphis.ConnectionToken("memphis"))
	if err != nil {
		os.Exit(1)
	}
	defer c.Close()

	//Implicit Producer creation (default factory and station)
	p1, err := c.CreateProducer("producer_name_a")
	p1.Producer([]byte("You have a message!"))

	//Explicit factory and station creations
	f, err := c.CreateFactory("factory_name_1")
	if err != nil {
		fmt.Errorf("Factory creation failed: %v", err)
		os.Exit(1)
	}

	// Comment the next line if you want the message to persist
	defer f.Remove()

	s, err := f.CreateStation("station_name")
	if err != nil {
		fmt.Errorf("Station creation failed: %v", err)
		os.Exit(1)
	}

	p2, err := s.CreateProducer("producer_name_a")
	if err != nil {
		fmt.Errorf("Producer creation failed: %v", err)
		os.Exit(1)
	}

	err = p2.Produce([]byte("Hey There!"), memphis.AckWaitSec(15))

	if err != nil {
		os.Exit(1)
	}
}
