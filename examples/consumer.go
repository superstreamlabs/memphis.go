package main

import (
	"fmt"
	"os"

	"github.com/memphisdev/memphis.go"
)

func main() {
	conn, err := memphis.Connect("localhost", "root", "memphis")
	if err != nil {
		os.Exit(1)
	}
	defer conn.Close()

	consumer, err := conn.CreateConsumer("station_1", "consumer_name")

	if err != nil {
		fmt.Printf("Consumer creation failed: %v", err)
		os.Exit(1)
	}

	for {
		res, _ := <-consumer.Puller
		fmt.Println(string(res))
	}

	err = consumer.Destroy()
	if err != nil {
		fmt.Printf("Consumer removal failed: %v", err)
		os.Exit(1)
	}
}
