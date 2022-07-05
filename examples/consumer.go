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
		fmt.Printf("Consumer creation failed: %v\n", err)
		os.Exit(1)
	}

	msgs, err := consumer.Fetch()
	if err != nil {
		fmt.Printf("Fetch failed: %v\n", err)
		os.Exit(1)
	}

	for _, msg := range msgs {
		fmt.Println(string(msg.Data()))
		msg.Ack()
	}

	err = consumer.Destroy()
	if err != nil {
		fmt.Printf("Consumer removal failed: %v\n", err)
		os.Exit(1)
	}
}
