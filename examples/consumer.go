package main

import (
	"fmt"
	"os"

	"github.com/memphisdev/memphis.go/memphis"
)

func main() {
	conn, err := memphis.Connect("localhost", memphis.Username("root"), memphis.ConnectionToken("memphis"))
	if err != nil {
		os.Exit(1)
	}
	defer conn.Close()

	consumer, err := conn.CreateConsumer("consumer_name", "station_name")

	if err != nil {
		fmt.Errorf("Consumer creation failed: %v", err)
		os.Exit(1)
	}

	for {
		res, _ := <-consumer.Puller
		fmt.Println(string(res))
	}

	err = consumer.Remove()
	if err != nil {
		fmt.Errorf("Consumer removal failed: %v", err)
		os.Exit(1)
	}
}
