package main

import (
	"fmt"
	"os"
	"time"

	"github.com/memphisdev/memphis.go"
)

func main() {
	conn, err := memphis.Connect("<memphis-host>", "<application type username>", "<broker-token>")
	if err != nil {
		os.Exit(1)
	}
	defer conn.Close()

	consumer, err := conn.CreateConsumer("<station-name>", "<consumer-name>")

	if err != nil {
		fmt.Printf("Consumer creation failed: %v\n", err)
		os.Exit(1)
	}

	handler := func(msgs []*memphis.Msg, err error) {
		if err != nil {
			fmt.Printf("Fetch failed: %v\n", err)
			return
		}

		for _, msg := range msgs {
			fmt.Println(string(msg.Data()))
			msg.Ack()
		}
	}

	consumer.Consume(15*time.Second, handler)

	time.Sleep(30 * time.Second)
}
